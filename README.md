# dfu-rs

Implements DFU operations for USB devices.

Based on [`rusb`](https://docs.rs/rusb/latest/rusb/) for USB communication, this crate provides a simple interface for discovering DFU-capable devices and performing DFU operations.

It is designed for use in host applications that need to update firmware on embedded devices via USB DFU.

It is intended to work on Windows, Linux, and macOS.

# Features

- Upload (read) data from DFU devices
- Download (write) data to DFU devices
- Erase flash memory (page-wise and mass erase)
- Device discovery with filtering by DFU type (e.g. internal flash)
- Error handling with detailed DFU and USB errors
- Uses blocking calls via `rusb` - wrap in `tokio::task::spawn_blocking` for async runtimes
- Customizable USB timeout

# Usage

```rust
use dfu_rs::{Device, DfuType};

if let Some(device) = Device::search(None).unwrap().first() {
   let mut buffer = vec![0u32; 4096]; // 16KB
  device.upload(0x08000000, &mut buffer).unwrap();
  println!("Uploaded data: {:X?}", &buffer[..16]); // Print first 16 words
}
```

See [the docs](https://docs.rs/dfu-rs) for more examples.