// Copyright (C) 2025 Piers Finlayson <piers@piers.rocks>
//
// MIT License

//! Implements DFU operations for USB devices.
//! 
//! Based on [`rusb`](https://docs.rs/rusb/latest/rusb/) for USB communication,
//! this crate provides a simple interface for discovering DFU-capable devices
//! and performing DFU operations.
//! 
//! It is designed for use in host applications that need to update firmware
//! on embedded devices via USB DFU.
//! 
//! It is intended to work on Windows, Linux, and macOS.
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

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use rusb::{Context, Device as RusbDevice, Error as RusbError, UsbContext};
use std::time::Duration;

// Timeout
const DEFAULT_USB_TIMEOUT: Duration = Duration::from_secs(30);

// USB class/subclass codes for DFU
const USB_CLASS_APPLICATION_SPECIFIC: u8 = 0xFE;
const USB_SUBCLASS_DFU: u8 = 0x01;

// DFU block size
const DFU_BLOCK_SIZE: usize = 2048;

// USB request types (bmRequestType)
const USB_CLASS_INTERFACE_OUT: u8 = 0x21; // Class, Interface, Host-to-Device
const USB_CLASS_INTERFACE_IN: u8 = 0xA1;  // Class, Interface, Device-to-Host

// STM32 DFU commands (vendor-specific)
const STM32_DFU_CMD_SET_ADDRESS: u8 = 0x21;
const STM32_DFU_CMD_ERASE: u8 = 0x41;
#[allow(dead_code)]
const STM32_DFU_CMD_READ_UNPROTECT: u8 = 0x92;

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

/// USB device information
#[derive(Debug, Clone, PartialEq)]
pub struct DeviceInfo {
    /// USB device vendor ID
    pub vid: u16,
    /// USB device product ID
    pub pid: u16,
    /// USB interface number
    pub interface: u8,
    /// USB bus number
    pub bus: u8,
    /// USB device address
    pub address: u8,
    /// USB alternate setting
    pub alt: u8,
    /// Device description string
    pub desc: String,
    /// DFU Type, decoded via USB description string
    pub dfu_type: DfuType,
}

impl std::fmt::Display for DeviceInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:0>4X}:{:0>4X}",
            self.vid, self.pid,
        )
    }
}

/// Error type
/// 
/// Many of the errors are wrappers around `rusb::Error`, and are often
/// somewhat esoteric.  [`Error::usb_stack_error()`] can be used to retrieve
/// the underlying USB stack error for further analysis, or test whether the
/// failure was a USB stack one.
#[derive(Debug, Clone, PartialEq)]
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
    UsbContext(RusbError),
    UsbDeviceEnumeration(RusbError),
    UsbDeviceOpen(RusbError),
    UsbKernelDriverDetach(RusbError),
    UsbClaimInterface(RusbError),
    UsbSetAltSetting(RusbError),
    UsbControlTransfer(RusbError),
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

impl Error {
    /// Returns the underlying USB stack error if applicable.
    pub fn usb_stack_error(&self) -> Option<&RusbError> {
        match self {
            Error::UsbContext(e) => Some(e),
            Error::UsbDeviceEnumeration(e) => Some(e),
            Error::UsbDeviceOpen(e) => Some(e),
            Error::UsbKernelDriverDetach(e) => Some(e),
            Error::UsbClaimInterface(e) => Some(e),
            Error::UsbSetAltSetting(e) => Some(e),
            Error::UsbControlTransfer(e) => Some(e),
            _ => None,
        }
    }
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
#[derive(Debug, Clone, PartialEq)]
pub struct Device {
    info: DeviceInfo,
    timeout: Duration,
}

impl std::fmt::Display for Device {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({:04X}:{:04X})",
            self.info.desc,
            self.info.vid,
            self.info.pid,
        )
    }
}

impl Device {
    /// Enumerates the USB bus and searches for DFU-capable devices.
    /// 
    /// Arguments:
    /// - `filter`: Optional filter to only return devices of a specific DFU type.
    ///   (such as flash)
    /// 
    /// Returns:
    /// - `Ok(Vec<DeviceInfo>)`: A vector of found DFU devices.
    /// - `Err(Error)`: An error occurred during USB enumeration.
    pub fn search(filter: Option<DfuType>) -> Result<Vec<Self>, Error> {
        let context = Context::new()
            .map_err(|e| Error::UsbContext(e))?;
        
        let devices = context.devices()
            .map_err(|e| Error::UsbDeviceEnumeration(e))?;

        let mut dfu_devices = Vec::new();
        
        for device in devices.iter() {
            dfu_devices.extend(check_device_for_dfu(&device));
        }

        let dfu_devices = if let Some(filter) = filter {
            check_device_for_dfu_type(filter, dfu_devices)
        } else {
            dfu_devices
        };

        let dfu_devices: Vec<Self> = dfu_devices.into_iter()
            .map(|info| Device::new(info))
            .collect();
        
        Ok(dfu_devices)
    }

    /// Creates a new DFU Device instance from the provided DeviceInfo.
    /// 
    /// Users should primarily use [`Device::search()`] to find and create DFU
    /// devices.  However, this constructor is provided for cases where
    /// [`Device::search()`] doesn't return the desired device, or is
    /// considered inefficient or otherwise insufficient.
    /// 
    /// Arguments:
    /// - `info`: The DeviceInfo struct representing the DFU device.
    /// 
    /// Returns:
    /// - `Device`: The created DFU Device instance.
    pub fn new(info: DeviceInfo) -> Self {
        Device { 
            info,
            timeout: DEFAULT_USB_TIMEOUT,
        }
    }

    /// Sets the USB timeout duration for operations.
    pub fn set_timeout(&mut self, timeout: Duration) {
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
    /// - `buf`: The buffer to fill with read data (in words).
    /// 
    /// Returns:
    /// - `Ok(())`: The upload was successful, and `buf` is filled
    /// - `Err(Error)`: An error occurred during the upload process.
    pub fn upload(&self, address: u32, buf: &mut [u32]) -> Result<(), Error> {
        trace!("Starting DFU upload from address 0x{:08X} for {} words", address, buf.len());
        let byte_count = buf.len() * 4;
        let mut bytes = vec![0u8; byte_count];
        
        // Open and setup device
        let (_context, handle) = self.open_device()?;
        trace!("DFU device opened successfully");
        
        // Set address pointer
        self.set_address(&handle, address)?;
        trace!("DFU address set successfully");
        
        // Abort to return to dfuIDLE before upload
        self.abort(&handle, false)?;
        trace!("DFU aborted to enter dfuIDLE state");
        
        // Calculate blocks needed
        let total_blocks = (byte_count + DFU_BLOCK_SIZE - 1) / DFU_BLOCK_SIZE;
        
        // Read each block
        for block in 0..total_blocks {
            let block_data = self.read_block(&handle, block)?;
            let offset = block * DFU_BLOCK_SIZE;
            let end = (offset + block_data.len()).min(byte_count);
            bytes[offset..end].copy_from_slice(&block_data[..end - offset]);
        }
        trace!("DFU upload completed successfully");
        
        // Final abort
        self.abort(&handle, true)?;
        trace!("DFU session aborted successfully");
        
        // Convert bytes to words and write to caller's buffer
        for (ii, chunk) in bytes.chunks_exact(4).enumerate() {
            buf[ii] = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        }
        
        Ok(())
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
    pub fn erase(&self, address: u32, length: usize, page_size: usize) -> Result<(), Error> {
        trace!("Starting DFU erase at address 0x{:08X} for {} bytes with page size {}", 
               address, length, page_size);
        
        let (_context, handle) = self.open_device()?;
        trace!("DFU device opened successfully");
        
        // Calculate number of pages to erase
        let total_pages = (length + page_size - 1) / page_size;
        
        // Erase each page
        for page in 0..total_pages {
            let page_address = address + (page * page_size) as u32;
            trace!("Erasing page {} at address 0x{:08X}", page, page_address);
            self.erase_page(&handle, page_address)?;
        }
        
        trace!("DFU erase completed successfully");
        Ok(())
    }

    /// Erases the entire flash memory
    /// 
    /// More efficient than page-by-page erase when erasing the complete flash.
    /// Uses the STM32 mass erase command (0x41 with 0xFF parameter).
    pub fn mass_erase(&self) -> Result<(), Error> {
        trace!("Starting DFU mass erase");
        
        let (_context, handle) = self.open_device()?;
        trace!("DFU device opened successfully");
        
        // Mass erase command: Just hte command byte
        let cmd = vec![STM32_DFU_CMD_ERASE];
        
        handle.write_control(
            USB_CLASS_INTERFACE_OUT,
            Request::Download.into(),
            0,
            self.info.interface as u16,
            &cmd,
            self.timeout
        ).map_err(|e| Error::UsbControlTransfer(e))?;
        
        // Wait for erase to complete
        self.get_status_check_download_busy(&handle)?;
        self.get_status(&handle)?;
        
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
    pub fn download(&self, address: u32, data: &[u32]) -> Result<(), Error> {
        trace!("Starting DFU download to address 0x{:08X} for {} words", address, data.len());
        
        // Convert words to bytes
        let mut bytes = Vec::with_capacity(data.len() * 4);
        for word in data {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        
        let (_context, handle) = self.open_device()?;
        trace!("DFU device opened successfully");
        
        // Set address pointer
        self.set_address(&handle, address)?;
        trace!("DFU address set successfully");
        
        // Calculate blocks needed
        let total_blocks = (bytes.len() + DFU_BLOCK_SIZE - 1) / DFU_BLOCK_SIZE;
        
        // Write each block
        for block in 0..total_blocks {
            let start = block * DFU_BLOCK_SIZE;
            let end = (start + DFU_BLOCK_SIZE).min(bytes.len());
            
            trace!("Writing block {} of {}", block + 1, total_blocks);
            self.write_block(&handle, block, &bytes[start..end])?;
        }
        trace!("DFU download completed successfully");
        
        // Send zero-length download to complete the transfer
        handle.write_control(
            USB_CLASS_INTERFACE_OUT,
            Request::Download.into(),
            0,
            self.info.interface as u16,
            &[],
            self.timeout
        ).map_err(|e| Error::UsbControlTransfer(e))?;
        
        // Final status check to trigger manifest
        self.get_status(&handle)?;
        
        trace!("DFU download session completed successfully");
        Ok(())
    }
}

// Private helper methods
impl Device {
    fn clear_status(&self, handle: &rusb::DeviceHandle<Context>) -> Result<(), Error> {
        trace!("Clearing DFU status");
        handle.write_control(
            USB_CLASS_INTERFACE_OUT,
            Request::ClearStatus.into(),
            0,
            self.info.interface as u16,
            &[],
            self.timeout
        ).map_err(|e| Error::UsbControlTransfer(e))?;
        
        Ok(())
    }
    
    fn open_device(&self) -> Result<(Context, rusb::DeviceHandle<Context>), Error> {
        trace!("Opening DFU device: {:?}", self.info);
        let context = Context::new()
            .map_err(|e| Error::UsbContext(e))?;
        
        let devices = context.devices()
            .map_err(|e| Error::UsbDeviceEnumeration(e))?;
        
        let device = devices.iter()
            .find(|d| d.bus_number() == self.info.bus && d.address() == self.info.address)
            .ok_or_else(|| Error::DeviceNotFound)?;
        
        #[allow(unused_mut)] // Required mutable on Windows
        let mut handle = device.open()
            .map_err(|e| Error::UsbDeviceOpen(e))?;

        // Select configuration 1 (required on Windows)
        handle.set_active_configuration(1)
            .map_err(|e| Error::UsbDeviceOpen(e))?;
        
        // Detach kernel driver if needed
        if let Ok(true) = handle.kernel_driver_active(self.info.interface) {
            handle.detach_kernel_driver(self.info.interface)
                .map_err(|e| Error::UsbKernelDriverDetach(e))?;
        }
        
        handle.claim_interface(self.info.interface)
            .map_err(|e| Error::UsbClaimInterface(e))?;
        
        handle.set_alternate_setting(self.info.interface, self.info.alt)
            .map_err(|e| Error::UsbSetAltSetting(e))?;

        // Initialize the DFU state machine
        let needs_clear = match self.get_status(&handle) {
            Ok(status) => status.is_state_error(),
            Err(_) => true,  // getStatus failed, needs initialization
        };

        if needs_clear {
            self.clear_status(&handle)?;
            self.get_status(&handle)?;
        }
        
        // Ensure device is in dfuIDLE state for subsequent operations
        self.abort(&handle, false)?;

        Ok((context, handle))
    }
    
    fn set_address(&self, handle: &rusb::DeviceHandle<Context>, address: u32) -> Result<(), Error> {
        trace!("Setting DFU address to 0x{:08X}", address);
        trace!("DFU info {:?}", self.info);

        // Command: 0x21 followed by address in little-endian
        let mut cmd = vec![STM32_DFU_CMD_SET_ADDRESS];
        cmd.extend_from_slice(&address.to_le_bytes());
        
        handle.write_control(
            USB_CLASS_INTERFACE_OUT,
            Request::Download.into(),
            0,    // wValue: 0 for command mode
            self.info.interface as u16,
            &cmd,
            self.timeout
        )
            .inspect_err(|e| warn!("Control transfer error during set_address: {e}"))
            .map_err(|e| Error::UsbControlTransfer(e))?;
        
        // First get status after write address should return download busy
        self.get_status_check_download_busy(handle)?;
        self.get_status(handle)?;
        
        Ok(())
    }

    fn erase_page(&self, handle: &rusb::DeviceHandle<Context>, address: u32) -> Result<(), Error> {
        trace!("Erasing page at address 0x{:08X}", address);
        
        // Command: 0x41 followed by address in little-endian
        let mut cmd = vec![STM32_DFU_CMD_ERASE];
        cmd.extend_from_slice(&address.to_le_bytes());
        
        handle.write_control(
            USB_CLASS_INTERFACE_OUT,
            Request::Download.into(),
            0,    // wValue: 0 for command mode
            self.info.interface as u16,
            &cmd,
            self.timeout
        ).map_err(|e| Error::UsbControlTransfer(e))?;
        
        // Wait for erase to complete - can take significant time
        self.get_status_check_download_busy(handle)?;
        self.get_status(handle)?;
        
        Ok(())
    }

    fn abort(&self, handle: &rusb::DeviceHandle<Context>, get_status: bool) -> Result<(), Error> {
        trace!("Sending DFU abort");
        handle.write_control(
            USB_CLASS_INTERFACE_OUT,
            Request::Abort.into(),
            0,
            self.info.interface as u16,
            &[],
            self.timeout
        ).map_err(|e| Error::UsbControlTransfer(e))?;
        
        if get_status {
            self.get_status(handle)?;
        }
        
        Ok(())
    }
    
    fn read_block(&self, handle: &rusb::DeviceHandle<Context>, block: usize) -> Result<Vec<u8>, Error> {
        trace!("Reading DFU block {}", block);
        let mut buf = vec![0u8; DFU_BLOCK_SIZE];
        
        let bytes_read = handle.read_control(
            USB_CLASS_INTERFACE_IN,
            Request::Upload.into(),
            (2 + block) as u16, // wValue: block number + 2
            self.info.interface as u16,
            &mut buf,
            self.timeout
        ).map_err(|e| Error::UsbControlTransfer(e))?;
        
        buf.truncate(bytes_read);
        
        self.get_status(handle)?;
        self.get_status(handle)?;
        
        Ok(buf)
    }
    
    fn write_block(&self, handle: &rusb::DeviceHandle<Context>, block: usize, data: &[u8]) -> Result<(), Error> {
        trace!("Writing DFU block {} ({} bytes)", block, data.len());
        
        // Prepare 2KB block, padding with 0xFF if needed
        let mut block_data = vec![0xFF; DFU_BLOCK_SIZE];
        block_data[..data.len()].copy_from_slice(data);
        
        handle.write_control(
            USB_CLASS_INTERFACE_OUT,
            Request::Download.into(),
            (2 + block) as u16, // wValue: block number + 2
            self.info.interface as u16,
            &block_data,
            self.timeout
        ).map_err(|e| Error::UsbControlTransfer(e))?;
        
        // Wait for write to complete
        self.get_status_check_download_busy(handle)?;
        self.get_status(handle)?;

        Ok(())
    }

    // Returns Err(Error) if device is not in download busy state
    fn get_status_check_download_busy(&self, handle: &rusb::DeviceHandle<Context>) -> Result<(), Error> {
        let status = self.get_status_error_flag(handle, true)?;
        if !status.is_download_busy() {
            return Err(Error::DfuStatus{ status: status.status(), state: status.state() })
        }
        Ok(())
    }

    // Returns Err(Error) if any error status/state is reported
    fn get_status(&self, handle: &rusb::DeviceHandle<Context>) -> Result<DeviceStatus, Error> {
        self.get_status_error_flag(handle, false)
    }

    fn get_status_error_flag(&self, handle: &rusb::DeviceHandle<Context>, error_ok: bool) -> Result<DeviceStatus, Error> {
        trace!("Getting DFU status");
        let mut data = [0u8; Request::GetStatus.fixed_length()];
        handle.read_control(
            USB_CLASS_INTERFACE_IN,
            Request::GetStatus.into(),
            0,
            self.info.interface as u16,
            &mut data,
            self.timeout
        ).map_err(|e| Error::UsbControlTransfer(e))?;

        let status = DeviceStatus::from_packet(&data)?;
        
        // Wait for poll time
        std::thread::sleep(std::time::Duration::from_millis(status.poll_time() as u64));
        
        if !error_ok && status.is_error() {
            return Err(Error::DfuStatus{ status: status.status(), state: status.state() });
        }
        
        Ok(status)
    }
}

fn check_device_for_dfu_type(filter: DfuType, device_info: Vec<DeviceInfo>) -> Vec<DeviceInfo> {
    device_info.into_iter()
        .filter(|info| info.dfu_type == filter)
        .collect()
}

fn check_device_for_dfu(device: &RusbDevice<Context>) -> Vec<DeviceInfo> {
    let mut results = Vec::new();
    
    let desc = match device.device_descriptor() {
        Ok(d) => d,
        Err(_) => return results,
    };
    
    let vid = desc.vendor_id();
    let pid = desc.product_id();
    let bus = device.bus_number();
    let address = device.address();
    
    for config_idx in 0..desc.num_configurations() {
        let config = match device.config_descriptor(config_idx) {
            Ok(c) => c,
            Err(_) => continue,
        };
        
        for interface in config.interfaces() {
            for iface_desc in interface.descriptors() {
                if iface_desc.class_code() == USB_CLASS_APPLICATION_SPECIFIC && iface_desc.sub_class_code() == USB_SUBCLASS_DFU {
                    let alt = iface_desc.setting_number();
                    let desc_string = get_interface_string(device, &iface_desc);
                    let dfu_type = parse_dfu_type(&desc_string);
                    
                    results.push(DeviceInfo {
                        vid,
                        pid,
                        interface: interface.number(),
                        bus,
                        address,
                        alt,
                        desc: desc_string,
                        dfu_type,
                    });
                }
            }
        }
    }
    
    results
}

fn get_interface_string(
    device: &RusbDevice<Context>,
    iface_desc: &rusb::InterfaceDescriptor,
) -> String {
    device.open()
        .and_then(|handle| {
            handle.read_string_descriptor_ascii(iface_desc.description_string_index().unwrap_or(0))
        })
        .unwrap_or_else(|_| "Unknown".to_string())
}

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