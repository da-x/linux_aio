// Copyright 2016 Takeru Ohta <phjgt308@gmail.com>
// See the LICENSE file at the top-level directory of this distribution.

//! Types and Constants which are ported from "include/linux/aio_api.h".
//!
//! And system call wrapping functions.
#![allow(non_camel_case_types)]

use std;
use libc;
use std::io::Result as IoResult;
use std::io::Error as IoError;

pub type aio_context_t = libc::c_ulong;

#[repr(u16)]
#[derive(Clone)]
#[derive(Debug)]
pub enum IOCB_CMD {
    PREAD = 0,
    PWRITE = 1,
    FSYNC = 2,
    FDSYNC = 3,
    PREADX = 4, // experimental
    POLL = 5, // experimental
    NOOP = 6,
    PREADV = 7,
    PWRITEV = 8,
}

/// Valid flags for the "aio_flags" member of the "iocb".
///
/// IOCB_FLAG_RESFD - Set if the "aio_resfd" member of the "iocb"
///                   is valid.
///
pub const IOCB_FLAG_RESFD: libc::uint32_t = (1 << 0);

/// read() from /dev/aio returns these structures.
#[repr(C)]
#[derive(Clone)]
pub struct io_event {
    pub data: libc::uint64_t, // the data field from the iocb
    pub obj: libc::uint64_t, // what iocb this event came from
    pub res: libc::int64_t, // result code for this event
    pub res2: libc::int64_t, // secondary result
}

impl io_event {
    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct iocb {
    // these are internl to the kernel/libc.
    pub aio_data: libc::uint64_t, // data to be returned in event's data
    pub aio_reserved1: libc::uint64_t,

    // common fields
    pub aio_lio_opcode: IOCB_CMD,
    pub aio_reqprio: libc::int16_t,
    pub aio_fildes: libc::uint32_t,

    pub aio_buf: libc::uint64_t,
    pub aio_nbytes: libc::uint64_t,
    pub aio_offset: libc::int64_t,

    // extra parameters
    pub aio_reserved2: libc::uint64_t,

    // flags for the "iocb"
    pub aio_flags: libc::uint32_t,

    // if the IOCB_FLAG_RESFD flag of "aio_flags" is set, this is an
    // eventfd to signal AIO readiness to
    pub aio_resfd: libc::uint32_t,
} // 64 bytes

impl iocb {
    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

pub fn io_setup(nr_events: libc::c_uint, ctx_idp: &mut aio_context_t) -> IoResult<()> {
    let result = unsafe { libc::syscall(libc::SYS_io_setup, nr_events, ctx_idp) };
    from_aio_result(result).map(|_| ())
}

pub fn io_destroy(ctx_id: aio_context_t) -> IoResult<()> {
    let result = unsafe { libc::syscall(libc::SYS_io_destroy, ctx_id) };
    from_aio_result(result).map(|_| ())
}

pub fn io_getevents(ctx_id: aio_context_t,
                    min_nr: libc::c_long,
                    nr: libc::c_long,
                    events: *mut io_event,
                    timeout: Option<&libc::timespec>)
                    -> IoResult<usize> {
    let result = unsafe {
        libc::syscall(libc::SYS_io_getevents,
                      ctx_id,
                      min_nr,
                      nr,
                      events,
                      timeout.map(|x| x as *const libc::timespec)
                      .unwrap_or(std::ptr::null()))
    };
    from_aio_result(result)
}

pub fn io_submit(ctx_id: aio_context_t,
                 nr: libc::c_long,
                 iocbp: *const *const iocb)
                 -> IoResult<usize> {
    let result = unsafe { libc::syscall(libc::SYS_io_submit, ctx_id, nr, iocbp) };
    from_aio_result(result)
}

pub fn io_cancel(ctx_id: aio_context_t, iocb: *const iocb, result: *mut io_event) -> IoResult<()> {
    let result = unsafe { libc::syscall(libc::SYS_io_cancel, ctx_id, iocb, result) };
    from_aio_result(result).map(|_| ())
}

#[inline]
fn from_aio_result(result: libc::c_long) -> IoResult<usize> {
    if result < 0 {
        Err(IoError::last_os_error())
    } else {
        Ok(result as usize)
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn it_works() {
        let mut ctx_id = 0;
        io_setup(1, &mut ctx_id).unwrap();
        assert!(0 != ctx_id);

        io_destroy(ctx_id).unwrap();
    }
}
