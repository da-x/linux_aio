// Copyright 2016 Takeru Ohta <phjgt308@gmail.com>
// See the LICENSE file at the top-level directory of this distribution.

//! An example implementation of a simple file copy command

extern crate linux_aio;
extern crate libc;

use std::fs::File;
use std::io::Result as IoResult;
use std::io::Error as IoError;
use std::os::unix::io::RawFd;
use std::os::unix::io::FromRawFd;
use std::os::unix::io::AsRawFd;
use std::collections::HashMap;
use std::ffi::CString;
use linux_aio::Context;
use linux_aio::ControlBlock;
use linux_aio::AlignedBuf;

fn open_direct_file(path: &str) -> IoResult<File> {
    let result = unsafe {
        libc::open(CString::new(path).unwrap().as_ptr(),
                   libc::O_RDONLY | libc::O_DIRECT)
    };
    if result == -1 {
        Err(IoError::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(result as RawFd) })
    }
}

fn create_direct_file(path: &str) -> IoResult<File> {
    let result = unsafe {
        libc::open(CString::new(path).unwrap().as_ptr(),
                   libc::O_CREAT | libc::O_WRONLY | libc::O_TRUNC | libc::O_DIRECT,
                   0o666)
    };
    if result == -1 {
        Err(IoError::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(result as RawFd) })
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        println!("Usage: {} FROM_FILE TO_FILE", args[0]);
        std::process::exit(1);
    }

    let from_file = &args[1];
    let to_file = &args[2];

    let rf = open_direct_file(from_file).expect("Failed to open the input file");
    let wf = create_direct_file(to_file).expect("Failed to create the output file");

    let read_file_len = rf.metadata().unwrap().len() as usize;
    let block_size = 10 * 1024 * 1024;
    let block_count = (read_file_len + block_size - 1) / block_size;
    let mut offsets = HashMap::new();

    let mut context = Context::setup(block_count).expect("Can't setup an AIO Context");
    for i in 0..block_count {
        let offset = i * block_size;
        let buf = AlignedBuf::new(std::cmp::min(read_file_len - offset, block_size));
        let event_id = context.submit(ControlBlock::pread(rf.as_raw_fd(), buf, offset))
                              .expect("Failed to submit a read request");
        offsets.insert(event_id, offset);
    }

    let mut events = vec![];
    while !offsets.is_empty() {
        context.get_events(1, Some(4), None, &mut events).unwrap();

        for e in events.drain(..) {
            if let Some(offset) = offsets.remove(&e.id) {
                // read event

                let buf = e.cb.into_buf();
                context.submit(ControlBlock::pwrite(wf.as_raw_fd(), buf, offset))
                       .expect("Failed to submit a write request");
            }
        }
    }

    while !context.is_empty() {
        context.get_events(1, Some(4), None, &mut events).unwrap();
    }
}
