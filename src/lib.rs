// Copyright (C) 2025 Piers Finlayson <piers@piers.rocks>
//
// MIT License

//! Implements DFU operations for USB devices.
//! 
//! Based on the [`nusb`](https://docs.rs/nusb/latest/nusb/) native Rust stack
//! for USB communication, this crate provides a simple interface for
//! discovering DFU-capable devices and performing DFU operations.
//! 
//! It is designed for use in host applications that need to update firmware
//! on embedded devices via USB DFU.
//! 
//! It is intended to work on Windows, Linux, and macOS.
//! 
//! On Windows, WinUSB must be installed for the target device.  This can be
//! done via [Zadig](https://zadig.akeo.ie/), implementing a WCID descriptor
//! in your device, or using [`wdi-rs`](https://docs.rs/wdi-rs/latest/wdi_rs/)
//! to install the WinUSB driver programmatically (requires elevation).
//! 
//! # Features
//! 
//! - Upload (read) data from DFU devices
//! - Download (write) data to DFU devices
//! - Erase flash memory (page-wise and mass erase)
//! - Device discovery with filtering by DFU type (e.g. internal flash)
//! - Error handling with detailed DFU and USB errors
//! - Uses blocking calls via `rusb` - wrap in `tokio::task::spawn_blocking`
//!   for async runtimes
//! - Customizable USB timeout
//! 
//! # Usage
//! 
//! ```no_run
//! use dfu_rs::{Device, DfuType};
//! 
//! if let Some(device) = Device::search(None).unwrap().first() {
//!    let mut buffer = vec![0u32; 4096]; // 16KB
//!   device.upload(0x08000000, &mut buffer).unwrap();
//!   println!("Uploaded data: {:X?}", &buffer[..16]); // Print first 16 words
//! }
//! ```
//! 
//! See [`Device`] for more examples.

use async_io::Timer;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use nusb::{Device as NusbDevice, DeviceInfo as NusbDeviceInfo, Error as NusbError, Interface};
use nusb::transfer::{ControlIn, ControlOut, ControlType, Recipient, TransferError};
use std::time::Duration;

// Timeout
pub const DEFAULT_USB_TIMEOUT: Duration = Duration::from_secs(30);

// USB class/subclass codes for DFU
const USB_CLASS_APPLICATION_SPECIFIC: u8 = 0xFE;
const USB_SUBCLASS_DFU: u8 = 0x01;

// DFU block size
const DFU_BLOCK_SIZE: usize = 2048;

// STM32 DFU commands (vendor-specific)
const STM32_DFU_CMD_SET_ADDRESS: u8 = 0x21;
const STM32_DFU_CMD_ERASE: u8 = 0x41;
#[allow(dead_code)]
const STM32_DFU_CMD_READ_UNPROTECT: u8 = 0x92;

// Language ID for string descriptors
const LANGUAGE_ID: u16 = 0x0409; // English (United States)

// DFU request types
#[derive(Debug, Clone, PartialEq)]
#[repr(u8)]
#[allow(dead_code)]
enum Request {
    Detach = 0,
    Download = 1,
    Upload = 2,
    GetStatus = 3,
    ClearStatus = 4,
    GetState = 5,
    Abort = 6,
}

impl Into<u8> for Request {
    fn into(self) -> u8 {
        self as u8
    }
}

impl Request {
    // Returns expected length of the request's data phase where known
    const fn fixed_length(&self) -> usize {
        match self {
            Request::Detach => 0,
            Request::Download => 0,
            Request::Upload => 0,
            Request::GetStatus => 6,
            Request::ClearStatus => 0,
            Request::GetState => 1,
            Request::Abort => 0,
        }
    }
}

/// DFU Status codes
#[derive(Debug, Clone, PartialEq)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    ErrTarget = 1,
    ErrFile = 2,
    ErrWrite = 3,
    ErrErase = 4,
    ErrCheckErased = 5,
    ErrProg = 6,
    ErrVerify = 7,
    ErrAddress = 8,
    ErrNotDone = 9,
    ErrFirmware = 10,
    ErrVendor = 11,
    ErrUsbReset = 12,
    ErrPowerOnReset = 13,
    ErrUnknown = 14,
    ErrStalledPkt = 15,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Ok => write!(f, "OK"),
            Status::ErrTarget => write!(f, "Error: Target"),
            Status::ErrFile => write!(f, "Error: File"),
            Status::ErrWrite => write!(f, "Error: Write"),
            Status::ErrErase => write!(f, "Error: Erase"),
            Status::ErrCheckErased => write!(f, "Error: Check Erased"),
            Status::ErrProg => write!(f, "Error: Program"),
            Status::ErrVerify => write!(f, "Error: Verify"),
            Status::ErrAddress => write!(f, "Error: Address"),
            Status::ErrNotDone => write!(f, "Error: Not Done"),
            Status::ErrFirmware => write!(f, "Error: Firmware"),
            Status::ErrVendor => write!(f, "Error: Vendor"),
            Status::ErrUsbReset => write!(f, "Error: USB Reset"),
            Status::ErrPowerOnReset => write!(f, "Error: Power On Reset"),
            Status::ErrUnknown => write!(f, "Error: Unknown"),
            Status::ErrStalledPkt => write!(f, "Error: Stalled Packet"),
        }
    }
}

/// DFU Device State codes
#[derive(Debug, Clone, PartialEq)]
#[repr(u8)]
pub enum State {
    AppIdle = 0,
    AppDetach = 1,
    DfuIdle = 2,
    DfuDnloadSync = 3,
    DfuDnloadBusy = 4,
    DfuDnloadIdle = 5,
    DfuManifestSync = 6,
    DfuManifest = 7,
    DfuManifestWaitReset = 8,
    DfuUploadIdle = 9,
    DfuError = 10,
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::AppIdle => write!(f, "App Idle"),
            State::AppDetach => write!(f, "App Detach"),
            State::DfuIdle => write!(f, "DFU Idle"),
            State::DfuDnloadSync => write!(f, "DFU Download Sync"),
            State::DfuDnloadBusy => write!(f, "DFU Download Busy"),
            State::DfuDnloadIdle => write!(f, "DFU Download Idle"),
            State::DfuManifestSync => write!(f, "DFU Manifest Sync"),
            State::DfuManifest => write!(f, "DFU Manifest"),
            State::DfuManifestWaitReset => write!(f, "DFU Manifest Wait Reset"),
            State::DfuUploadIdle => write!(f, "DFU Upload Idle"),
            State::DfuError => write!(f, "DFU Error"),
        }
    }
}

// Status returned by the DFU device
struct DeviceStatus {
    status: Status,
    state: State,
    poll_time: u32,
    string: u8,
}

#[allow(dead_code)]
impl DeviceStatus {
    fn status(&self) -> Status {
        self.status.clone()
    }

    fn state(&self) -> State {
        self.state.clone()
    }

    fn poll_time(&self) -> u32 {
        self.poll_time
    }

    fn string_index(&self) -> u8 {
        self.string
    }

    fn is_error(&self) -> bool {
        self.is_status_error() || self.is_state_error()
    }

    fn is_status_error(&self) -> bool {
        self.status != Status::Ok
    }

    fn is_state_error(&self) -> bool {
        self.state == State::DfuError
    }

    fn is_state_dfu_idle(&self) -> bool {
        self.state == State::DfuIdle
    }

    fn is_download_busy(&self) -> bool {
        self.state == State::DfuDnloadBusy
    }

    fn is_download_manifest(&self) -> bool {
        self.state == State::DfuManifest || self.state == State::DfuManifestSync
    }

    fn from_packet(data: &[u8]) -> Result<Self, Error> {
        if data.len() < Request::GetStatus.fixed_length() {
            warn!("Invalid DFU device status packet length: {}", data.len());
            return Err(Error::DfuInvalidDeviceStatus);
        }

        let status = match data[0] {
            0 => Status::Ok,
            1 => Status::ErrTarget,
            2 => Status::ErrFile,
            3 => Status::ErrWrite,
            4 => Status::ErrErase,
            5 => Status::ErrCheckErased,
            6 => Status::ErrProg,
            7 => Status::ErrVerify,
            8 => Status::ErrAddress,
            9 => Status::ErrNotDone,
            10 => Status::ErrFirmware,
            11 => Status::ErrVendor,
            12 => Status::ErrUsbReset,
            13 => Status::ErrPowerOnReset,
            14 => Status::ErrUnknown,
            15 => Status::ErrStalledPkt,
            _ => {
                warn!("Unknown DFU status code: {}", data[0]);
                return Err(Error::DfuInvalidDeviceStatus)
            }
        };

        let state = match data[4] {
            0 => State::AppIdle,
            1 => State::AppDetach,
            2 => State::DfuIdle,
            3 => State::DfuDnloadSync,
            4 => State::DfuDnloadBusy,
            5 => State::DfuDnloadIdle,
            6 => State::DfuManifestSync,
            7 => State::DfuManifest,
            8 => State::DfuManifestWaitReset,
            9 => State::DfuUploadIdle,
            10 => State::DfuError,
            _ => {
                warn!("Unknown DFU state code: {}", data[4]);
                return Err(Error::DfuInvalidDeviceStatus);
            }
        };

        let poll_time = u32::from_le_bytes([data[1], data[2], data[3], 0]);
        let string = data[5];

        trace!("Device Status: status={}, state={}, poll_time={}ms, string={}", status, state, poll_time, string);

        Ok(DeviceStatus {
            status,
            state,
            poll_time,
            string,
        })
    }
}

/// DFU Type enumeration - each USB DFU device can represent different memory
/// regions, represented by this object
#[derive(Debug, Clone, PartialEq)]
pub enum DfuType {
    InternalFlash,
    OptionBytes,
    SystemMemory,
    Unknown(String),
}

impl std::fmt::Display for DfuType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DfuType::InternalFlash => write!(f, "Internal Flash"),
            DfuType::OptionBytes => write!(f, "Option Bytes"),
            DfuType::SystemMemory => write!(f, "System Memory"),
            DfuType::Unknown(desc) => write!(f, "Unknown ({})", desc),
        }
    }
}

/// USB device information
#[derive(Debug, Clone, PartialEq)]
pub struct DeviceInfo {
    /// USB device vendor ID
    pub vid: u16,
    /// USB device product ID
    pub pid: u16,
    /// USB bus ID
    pub bus: String,
    /// USB device address
    pub address: u8,
    /// DFU information
    pub dfu: DfuInfo,
}

impl std::fmt::Display for DeviceInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:0>4X}:{:0>4X}",
            self.vid, self.pid
        )
    }
}

impl DeviceInfo {
    pub fn from_nusb(info: &NusbDeviceInfo, dfu: DfuInfo) -> Self {
        let vid = info.vendor_id();
        let pid = info.product_id();
        let bus = info.bus_id().to_string();
        let address = info.device_address();

        DeviceInfo {
            vid,
            pid,
            bus,
            address,
            dfu: dfu,
        }
    }

    pub fn dfu_type(&self) -> &DfuType {
        &self.dfu.dfu_type
    }

    pub fn is_dfu_type(&self, dfu_type: &DfuType) -> bool {
        self.dfu.dfu_type == *dfu_type
    }

    pub fn interface(&self) -> u8 {
        self.dfu.interface
    }

    pub fn vid(&self) -> u16 {
        self.vid
    }   

    pub fn pid(&self) -> u16 {
        self.pid
    }
}

/// Information about a DFU interface
#[derive(Debug, Clone, PartialEq)]
pub struct DfuInfo {
    /// USB interface number
    pub interface: u8,
    /// USB alternate setting
    pub alt: u8,
    /// Device description string
    pub desc: String,
    /// DFU Type, decoded via USB description string
    pub dfu_type: DfuType,
}

impl std::fmt::Display for DfuInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Interface {}, Alt {}, Type: {}, Desc: {}",
            self.interface, self.alt, self.dfu_type, self.desc
        )
    }
}

/// Error type
/// 
/// Many of the errors are wrappers around `rusb::Error`, and are often
/// somewhat esoteric.  [`Error::usb_stack_error()`] can be used to retrieve
/// the underlying USB stack error for further analysis, or test whether the
/// failure was a USB stack one.
#[derive(Debug, Clone)]
pub enum Error {
    /// DFU Device not found
    DeviceNotFound,
    /// DFU status error returned by device
    DfuStatus{
        status: Status,
        state: State,
    },
    /// Invalid DFU device status response
    DfuInvalidDeviceStatus,
    /// DFU set address pointer failed
    DfuSetAddressFailed(Status, State),
    UsbContext(NusbError),
    UsbDeviceEnumeration(NusbError),
    UsbDeviceOpen(NusbError),
    UsbKernelDriverDetach(NusbError),
    UsbClaimInterface(NusbError),
    UsbSetAltSetting(NusbError),
    UsbControlTransfer(TransferError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DeviceNotFound => write!(f, "DFU Device Not Found"),
            Error::DfuStatus{ status, state } => write!(f, "DFU Status Error: status code {}, state {}", status, state),
            Error::DfuInvalidDeviceStatus => write!(f, "DFU Invalid Device Status"),
            Error::DfuSetAddressFailed(status, state) => write!(f, "DFU Set Address Failed: status code {}, state {}", status, state),
            Error::UsbContext(e) => write!(f, "USB Context Error: {}", e),
            Error::UsbDeviceEnumeration(e) => write!(f, "USB Device Enumeration Error: {}", e),
            Error::UsbDeviceOpen(e) => write!(f, "Device Open Error: {}", e),
            Error::UsbKernelDriverDetach(e) => write!(f, "Kernel Driver Detach Error: {}", e),
            Error::UsbClaimInterface(e) => write!(f, "Claim Interface Error: {}", e),
            Error::UsbSetAltSetting(e) => write!(f, "Set Alternate Setting Error: {}", e),
            Error::UsbControlTransfer(e) => write!(f, "Control Transfer Error: {}", e),
        }
    }
}

#[derive(Debug, Clone)]
pub enum UsbStackError {
    Nusb(NusbError),
    Transfer(TransferError),
}

impl Error {
    /// Returns the underlying USB stack error if applicable.
    pub fn usb_stack_error(&self) -> Option<UsbStackError> {
        match self {
            Error::UsbContext(e) => Some(UsbStackError::Nusb(e.clone())),
            Error::UsbDeviceEnumeration(e) => Some(UsbStackError::Nusb(e.clone())),
            Error::UsbDeviceOpen(e) => Some(UsbStackError::Nusb(e.clone())),
            Error::UsbKernelDriverDetach(e) => Some(UsbStackError::Nusb(e.clone())),
            Error::UsbClaimInterface(e) => Some(UsbStackError::Nusb(e.clone())),
            Error::UsbSetAltSetting(e) => Some(UsbStackError::Nusb(e.clone())),
            Error::UsbControlTransfer(e) => Some(UsbStackError::Transfer(e.clone())),
            _ => None,
        }
    }
}

// Handle object to abstract away Windows + Unix like OS differences
#[derive(Debug)]
#[allow(dead_code)]
struct Handle {
    device: NusbDevice,
    interface: Interface,
}

/// DFU Device representation
/// 
/// Used to search for DFU-capable devices, hold their information, and perform
/// DFU operations.
/// 
/// Create a DFU device by calling `Device::search()`, which returns a list
/// of found DFU devices.
/// 
/// Example:
/// ```no_run
/// use dfu_rs::{Device, DfuType};
/// 
/// // Search for all DFU devices
/// let devices = Device::search(None).unwrap();
/// for device in devices {
///    println!("Found DFU Device: {}", device);
/// }
/// 
/// // Search for only Internal Flash DFU devices
/// let flash_devices = Device::search(Some(DfuType::InternalFlash)).unwrap();
/// for device in flash_devices {
///    println!("Found Flash DFU Device: {}", device);
/// }
/// 
/// // Retrieve the first 16KB from the first found DFU device
/// if let Some(device) = Device::search(None).unwrap().first() {
///    let mut buffer = vec![0u32; 4096]; // 16KB
///   device.upload(0x08000000, &mut buffer).unwrap();
///   println!("Uploaded data: {:X?}", &buffer[..16]); // Print first 16 words
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Device {
    info: DeviceInfo,
    nusb_info: NusbDeviceInfo,
    timeout: Duration,
}

impl PartialEq for Device {
    fn eq(&self, other: &Self) -> bool {
        self.info == other.info
    }
}

impl std::fmt::Display for Device {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({:04X}:{:04X})",
            self.nusb_info.product_string().unwrap_or("Unknown Device"),
            self.nusb_info.vendor_id(),
            self.nusb_info.product_id(),
        )
    }
}

impl Device {
    /// Creates a new DFU Device instance from the provided DeviceInfo.
    /// 
    /// Users should primarily use [`Device::search()`] to find and create DFU
    /// devices.  However, this constructor is provided for cases where
    /// [`Device::search()`] doesn't return the desired device, or is
    /// considered inefficient or otherwise insufficient.
    /// 
    /// Arguments:
    /// - `nusb_info`: The DeviceInfo object from nusb
    /// - `dfu_info`: Informaton about the DFU interface of the device
    /// 
    /// Returns:
    /// - `Device`: The created DFU Device instance.
    pub fn from_nusb(nusb_info: NusbDeviceInfo, dfu_info: DfuInfo) -> Self {
        let info = DeviceInfo::from_nusb(&nusb_info, dfu_info);

        Device {
            info,
            nusb_info,
            timeout: DEFAULT_USB_TIMEOUT,
        }
    }

    pub async fn from_device_info(info: DeviceInfo) -> Result<Self, Error> {
        // Get USB devices from nusb
        let devices = nusb::list_devices()
            .await
            .map_err(|e| Error::UsbDeviceEnumeration(e))?;

        // Find the matching device
        let nusb_info = devices.into_iter()
            .find(|d| d.vendor_id() == info.vid && d.product_id() == info.pid &&
                      d.bus_id().to_string() == info.bus && d.device_address() == info.address)
            .ok_or_else(|| Error::DeviceNotFound)?;

        Ok(Device {
            info,
            nusb_info,
            timeout: DEFAULT_USB_TIMEOUT,
        })
    }

    /// Sets the USB timeout duration for operations.
    pub fn set_timeout(&mut self, timeout: Duration) {
        debug!("Setting USB timeout to {:?}", timeout);
        self.timeout = timeout;
    }

    /// Returns the device information.
    pub fn info(&self) -> &DeviceInfo {
        &self.info
    }

    /// Upload (retrieve) data from the device via DFU.
    /// 
    /// Arguments:
    /// - `address`: The starting address to read from.
    /// - `length`: The length of data to read (in bytes).
    /// 
    /// Returns:
    /// - `Ok(())`: The upload was successful, and `buf` is filled
    /// - `Err(Error)`: An error occurred during the upload process.
    pub async fn upload(&self, address: u32, length: usize) -> Result<Vec<u8>, Error> {
        trace!("Starting DFU upload from address 0x{:08X} for {} bytes", address, length);
        let mut bytes = vec![0u8; length];
        
        // Open and setup device
        let handle = self.open().await?;
        trace!("DFU device opened successfully");
        
        // Set address pointer
        self.set_address(&handle, address).await?;
        trace!("DFU address set successfully");
        
        // Abort to return to dfuIDLE before upload
        self.abort(&handle, false).await?;
        trace!("DFU aborted to enter dfuIDLE state");
        
        // Calculate blocks needed
        let total_blocks = (length + DFU_BLOCK_SIZE - 1) / DFU_BLOCK_SIZE;
        
        // Read each block
        for block in 0..total_blocks {
            let block_data = self.read_block(&handle, block).await?;
            let offset = block * DFU_BLOCK_SIZE;
            let end = (offset + block_data.len()).min(length);
            bytes[offset..end].copy_from_slice(&block_data);
        }
        trace!("DFU upload completed successfully");
        
        // Final abort
        self.abort(&handle, true).await?;
        trace!("DFU session aborted successfully");
        
        Ok(bytes)
    }

    /// Erases flash memory by page/sector
    /// 
    /// Arguments:
    /// - `address`: Starting address to erase from
    /// - `length`: Number of bytes to erase (rounded up to page boundary)
    /// - `page_size`: Size of each page/sector in bytes (device-specific)
    /// 
    /// STM32F4 devices have variable sector sizes. Common values:
    /// - STM32F0/F1: 1024-2048 bytes
    /// - STM32F4: 16384 bytes for sectors 0-3, then 65536, then 131072
    /// 
    /// For STM32F4 with mixed sector sizes, use the smallest sector size
    /// and this will erase more than requested (safe but potentially slower).
    /// Alternatively, call erase_page() directly for each specific sector.
    pub async fn erase(&self, address: u32, length: usize, page_size: usize) -> Result<(), Error> {
        trace!("Starting DFU erase at address 0x{:08X} for {} bytes with page size {}", 
               address, length, page_size);
        
        let handle = self.open().await?;
        trace!("DFU device opened successfully");
        
        // Calculate number of pages to erase
        let total_pages = (length + page_size - 1) / page_size;
        
        // Erase each page
        for page in 0..total_pages {
            let page_address = address + (page * page_size) as u32;
            trace!("Erasing page {} at address 0x{:08X}", page, page_address);
            self.erase_page(&handle, page_address).await?;
        }
        
        trace!("DFU erase completed successfully");
        Ok(())
    }

    /// Erases the entire flash memory
    /// 
    /// More efficient than page-by-page erase when erasing the complete flash.
    /// Uses the STM32 mass erase command (0x41 with 0xFF parameter).
    pub async fn mass_erase(&self) -> Result<(), Error> {
        trace!("Starting DFU mass erase");
        
        let handle = self.open().await?;
        trace!("DFU device opened successfully");
        
        // Mass erase command: Just hte command byte
        let cmd = vec![STM32_DFU_CMD_ERASE];
        self.control_out(&handle, Request::Download, 0, &cmd).await?;

        // Check we get a busy response
        self.get_status_check_download_busy(&handle).await?;

        // Now try again
        self.get_status(&handle).await?;

        trace!("DFU mass erase completed successfully");
        Ok(())
    }

    /// Downloads (writes) data to flash memory
    /// 
    /// Arguments:
    /// - `address`: Starting address to write to
    /// - `data`: Slice of u32 words to write
    /// 
    /// Note: Flash must be erased before writing. Use erase() or mass_erase() first.
    /// Data is written in 2KB blocks using the DFU download protocol.
    pub async fn download(&self, address: u32, data: &[u8]) -> Result<(), Error> {
        trace!("Starting DFU download to address 0x{:08X} for {} bytes", address, data.len());
        
        let handle = self.open().await?;
        trace!("DFU device opened successfully");
        
        // Set address pointer
        self.set_address(&handle, address).await?;
        trace!("DFU address set successfully");
        
        // Calculate blocks needed
        let total_blocks = (data.len() + DFU_BLOCK_SIZE - 1) / DFU_BLOCK_SIZE;

        // Write each block
        for block in 0..total_blocks {
            let start = block * DFU_BLOCK_SIZE;
            let end = (start + DFU_BLOCK_SIZE).min(data.len());

            trace!("Writing block {} of {}", block + 1, total_blocks);
            self.write_block(&handle, block, &data[start..end]).await?;
        }
        trace!("DFU download completed successfully");
        
        // Send zero-length download to complete the transfer
        self.control_out(&handle, Request::Download, 0, &[]).await?;
        
        // Final status check to trigger manifest
        self.get_status(&handle).await?;
        
        trace!("DFU download session completed successfully");
        Ok(())
    }
}

// Private helper methods
impl Device {
    fn is_dfu_type(&self, dfu_type: &DfuType) -> bool {
        self.info.is_dfu_type(dfu_type)
    }

    fn interface(&self) -> u8 {
        self.info.interface()
    }

    async fn open(&self) -> Result<Handle, Error> {
        trace!("Opening DFU device: {:?}", self.info);
        let nusb_device = self.nusb_info.open()
            .await
            .map_err(|e| Error::UsbDeviceOpen(e))?;

        // Check there is an active configuration
        let _ = nusb_device.active_configuration()
            .map_err(|e| Error::UsbDeviceOpen(e.into()))?;
        
        // Claim interface
        let interface = self.info.interface();
        let interface = nusb_device.detach_and_claim_interface(interface).await
            .map_err(|e| Error::UsbClaimInterface(e))?;

        // Create the "handle"
        let handle = Handle {
            device: nusb_device,
            interface,
        };

        // Initialize the DFU state machine
        let needs_clear = match self.get_status(&handle).await {
            Ok(status) => status.is_state_error(),
            Err(_) => true,
        };

        if needs_clear {
            self.clear_status(&handle).await?;
            self.get_status(&handle).await?;
        }
        
        // Ensure device is in dfuIDLE state for subsequent operations
        self.abort(&handle, false).await?;

        Ok(handle)
    }


    #[cfg(not(target_os = "windows"))]
    async fn control_out(&self, handle: &Handle, request: Request, value: u16, data: &[u8]) -> Result<(), Error> {
        trace!("Sending DFU control out: {:?}", request);
        handle.device.control_out(ControlOut {
                control_type: ControlType::Class,
                recipient: Recipient::Interface,
                request: request.into(),
                value,
                index: self.interface() as u16,
                data,
            },
            self.timeout,
        ).await.map_err(|e| Error::UsbControlTransfer(e))
    }

    #[cfg(target_os = "windows")]
    async fn control_out(&self, handle: &Handle, request: Request, value: u16, data: &[u8]) -> Result<(), Error> {
        trace!("Sending DFU control out: {:?}", request);
        handle.interface.control_out(ControlOut {
                control_type: ControlType::Class,
                recipient: Recipient::Interface,
                request: request.into(),
                value,
                index: self.interface() as u16,
                data,
            },
            self.timeout,
        ).await.map_err(|e| Error::UsbControlTransfer(e))
    }

    #[cfg(not(target_os = "windows"))]
    async fn control_in(&self, handle: &Handle, request: Request, value: u16, length: u16) -> Result<Vec<u8>, Error> {
        trace!("Sending DFU control in: {:?}", request);
        handle.device.control_in(ControlIn {
                control_type: ControlType::Class,
                recipient: Recipient::Interface,
                request: request.into(),
                value,
                index: self.interface() as u16,
                length
            },
            self.timeout,
        ).await.map_err(|e| Error::UsbControlTransfer(e))
    }

    #[cfg(target_os = "windows")]
    async fn control_in(&self, handle: &Handle, request: Request, value: u16, length: u16) -> Result<Vec<u8>, Error> {
        trace!("Sending DFU control in: {:?}", request);
        handle.interface.control_in(ControlIn {
                control_type: ControlType::Class,
                recipient: Recipient::Interface,
                request: request.into(),
                value,
                index: self.interface() as u16,
                length
            },
            self.timeout,
        ).await.map_err(|e| Error::UsbControlTransfer(e))
    }
    
    async fn clear_status(&self, handle: &Handle) -> Result<(), Error> {
        trace!("Clearing DFU status");
        self.control_out(handle, Request::ClearStatus, 0, &[]).await
    }
    
    async fn set_address(&self, handle: &Handle, address: u32) -> Result<(), Error> {
        trace!("Setting DFU address to 0x{:08X}", address);
        trace!("DFU info {:?}", self.info);

        // Command: 0x21 followed by address in little-endian
        let mut cmd = vec![STM32_DFU_CMD_SET_ADDRESS];
        cmd.extend_from_slice(&address.to_le_bytes());

        self.control_out(handle, Request::Download, 0, &cmd).await?;

        // First get status after write address should return download busy
        self.get_status_check_download_busy(handle).await?;
        self.get_status(handle).await?;

        Ok(())
    }

    async fn erase_page(&self, handle: &Handle, address: u32) -> Result<(), Error> {
        trace!("Erasing page at address 0x{:08X}", address);
        
        // Command: 0x41 followed by address in little-endian
        let mut cmd = vec![STM32_DFU_CMD_ERASE];
        cmd.extend_from_slice(&address.to_le_bytes());
        
        self.control_out(handle, Request::Download, 0, &cmd).await?;
        
        // Wait for erase to complete - can take significant time
        self.get_status_check_download_busy(handle).await?;
        self.get_status(handle).await?;

        Ok(())
    }

    async fn abort(&self, handle: &Handle, get_status: bool) -> Result<(), Error> {
        trace!("Sending DFU abort");
        self.control_out(handle, Request::Abort, 0, &[]).await?;

        if get_status {
            self.get_status(handle).await?;
        }
        
        Ok(())
    }
    
    async fn read_block(&self, handle: &Handle, block: usize) -> Result<Vec<u8>, Error> {
        trace!("Reading DFU block {}", block);

        let data = self.control_in(
            handle,
            Request::Upload,
            (2 + block) as u16,
            DFU_BLOCK_SIZE as u16,
        ).await?;

        self.get_status(handle).await?;
        self.get_status(handle).await?;

        Ok(data)
    }
    
    async fn write_block(&self, handle: &Handle, block: usize, data: &[u8]) -> Result<(), Error> {
        trace!("Writing DFU block {} ({} bytes)", block, data.len());
        
        // Prepare 2KB block, padding with 0xFF if needed
        let mut block_data = vec![0xFF; DFU_BLOCK_SIZE];
        block_data[..data.len()].copy_from_slice(data);

        self.control_out(
            handle,
            Request::Download,
        (2 + block) as u16,
            &block_data,
        ).await?;
        
        // Wait for write to complete
        self.get_status_check_download_busy(handle).await?;
        self.get_status(handle).await?;

        Ok(())
    }

    // Returns Err(Error) if device is not in download busy state
    async fn get_status_check_download_busy(&self, handle: &Handle) -> Result<(), Error> {
        let status = self.get_status_error_flag(handle, true).await?;
        if !status.is_download_busy() {
            return Err(Error::DfuStatus{ status: status.status(), state: status.state() })
        }
        Ok(())
    }

    // Returns Err(Error) if any error status/state is reported
    async fn get_status(&self, handle: &Handle) -> Result<DeviceStatus, Error> {
        self.get_status_error_flag(&handle, false).await
    }

    async fn get_status_error_flag(&self, handle: &Handle, error_ok: bool) -> Result<DeviceStatus, Error> {
        trace!("Getting DFU status");
        let data = self.control_in(
            handle,
            Request::GetStatus,
            0,
            Request::GetStatus.fixed_length() as u16,
        ).await?;

        let status = DeviceStatus::from_packet(&data)?;
        
        // Wait for poll time
        trace!("Waiting for DFU poll time: {} ms", status.poll_time());
        Timer::after(Duration::from_millis(status.poll_time() as u64)).await;

        if !error_ok && status.is_error() {
            return Err(Error::DfuStatus{ status: status.status(), state: status.state() });
        }
        
        Ok(status)
    }
}

// Parses the DFU type from the USB interface description string
fn parse_dfu_type(desc: &str) -> DfuType {
    // Look for @Region Name / pattern
    if let Some(at_pos) = desc.find('@') {
        if let Some(slash_pos) = desc[at_pos..].find('/') {
            let region = desc[at_pos + 1..at_pos + slash_pos].trim();
            
            return match region {
                s if s.contains("Internal Flash") => DfuType::InternalFlash,
                s if s.contains("Option Bytes") => DfuType::OptionBytes,
                s if s.contains("System Memory") || s.contains("Bootloader") => DfuType::SystemMemory,
                _ => DfuType::Unknown(region.to_string()),
            };
        }
    }
    
    DfuType::Unknown(desc.to_string())
}

// Checks a single USB device for DFU interfaces, returning any found
async fn check_device_for_dfu(timeout: Duration, device_info: &NusbDeviceInfo) -> Option<Vec<Device>> {
    // First of all check if this device has any interfaces with a DFU class/subclass
    let mut dfu_device = false;
    for interface in device_info.interfaces() {
        trace!("Checking {device_info:?} interface {interface:?} for DFU class/subclass");
        let class = interface.class();
        let subclass = interface.subclass();
        if class == USB_CLASS_APPLICATION_SPECIFIC && subclass == USB_SUBCLASS_DFU {
            trace!("Found DFU interface");
            dfu_device = true;
            break;
        }
    }

    if !dfu_device {
        return None
    }

    // Open the device
    let vid = device_info.vendor_id();
    let pid = device_info.product_id();
    let device = match device_info.open().await {
        Ok(dev) => dev,
        Err(e) => {
            warn!("Failed to open USB device {vid:04X}:{pid:04X} for DFU interface check: {e}");
            return None
        }
    };

    // Get the active configuration
    let config = match device.active_configuration() {
        Ok(cfg) => cfg,
        Err(e) => {
            warn!("Failed to get active configuration for USB device {vid:04X}:{pid:04X}: {e}");
            return None
        }
    };

    // Iterate through all interfaces and alt settings of this config
    let mut results = Vec::new();
    for interface in config.interface_alt_settings() {
        let string_index = interface.string_index();
        if let Some(index) = string_index {
            // Read the interface string;
            let desc_str = match device.get_string_descriptor(index, LANGUAGE_ID, timeout).await {
                Ok(s) => s,
                Err(e) => {
                    warn!("Failed to read interface string for USB device {vid:04X}:{pid:04X}: {e}");
                    "Unknown".to_string();
                    return None;
                }
            };

            let dfu_type = parse_dfu_type(&desc_str);
            let dfu_info = DfuInfo {
                interface: interface.interface_number(),
                alt: interface.alternate_setting(),
                desc: desc_str,
                dfu_type,
            };

            let device = Device::from_nusb(device_info.clone(), dfu_info);
            trace!("Found DFU-capable device: {device}");
            results.push(device);
        }
    }
    
    Some(results)
}

/// Enumerates the USB bus and searches for DFU-capable devices.
/// 
/// Arguments:
/// - `filter`: Optional filter to only return devices of a specific DFU type.
///   (such as flash)
/// 
/// Returns:
/// - `Ok(Vec<DeviceInfo>)`: A vector of found DFU devices.
/// - `Err(Error)`: An error occurred during USB enumeration.
pub async fn search_for_dfu(timeout: Duration, filter: Option<DfuType>) -> Result<Vec<Device>, Error> {
    // Get USB devices from nusb
    let devices = nusb::list_devices()
        .await
        .map_err(|e| Error::UsbDeviceEnumeration(e))?;

    // Build the DFU devices list, checking each device for DFU capability
    let mut dfu_devices = Vec::new();
    for device in devices {
        if let Some(info) = check_device_for_dfu(timeout, &device).await {
            dfu_devices.extend(info);
        }
    }

    // If there's an optional filter, apply it now
    let filtered_dfu_devices = if let Some(filter) = &filter {
        dfu_devices.iter()
            .filter(|device| {
                trace!("Checking device {} for DFU type {:?}", device, filter);
                let is_match = device.is_dfu_type(filter);
                if is_match {
                    trace!("Device {} matches DFU type {:?}", device, filter);
                }
                is_match
            })
            .cloned()
            .collect()
    } else {
        dfu_devices
    };

    Ok(filtered_dfu_devices)
}
