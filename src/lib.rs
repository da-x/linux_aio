// Copyright 2016 Takeru Ohta <phjgt308@gmail.com>
// See the LICENSE file at the top-level directory of this distribution.

extern crate libc;

pub mod c_api;

use std::os::unix::io::RawFd;
use std::io::Error as IoError;
use std::time::Duration;
use std::ops::Deref;
use std::ops::DerefMut;
use slab::Slab;

pub type Result<T> = std::io::Result<T>;

pub struct AlignedBuf {
    ptr: *mut u8,
    size: usize,
    capacity: usize,
}

impl AlignedBuf {
    pub fn new(size: usize) -> Self {
        let align_size = 512; // TODO: replace to ioctl(BLKSSZGET)
        let allocate_size = (size + align_size - 1) / align_size * align_size;

        let mut ptr = std::ptr::null_mut();
        let result = unsafe { libc::posix_memalign(&mut ptr, align_size, allocate_size) };
        if result != 0 {
            panic!(format!("[linux_aio:{}:{}] {}",
                           file!(),
                           line!(),
                           IoError::from_raw_os_error(result)));
        }

        AlignedBuf {
            ptr: ptr as *mut _,
            size,
            capacity: allocate_size,
        }
    }

    /// Allow to use a smaller amount of space for the I/O itself, even if the
    /// buffer was allocated bigger.
    pub fn resize(&mut self, size: usize) -> std::result::Result<(), ()> {
        if self.capacity < size {
            return Err(());
        }

        self.size = size;
        Ok(())
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { libc::free(self.ptr as *mut _) };
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }
}

impl DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
}

pub struct ControlBlock {
    iocb: c_api::iocb,
    buf: AlignedBuf,
}

impl ControlBlock {
    pub fn pread(fd: RawFd, mut buf: AlignedBuf, offset: usize) -> Self {
        let mut iocb = c_api::iocb::zeroed();
        iocb.aio_fildes = fd as libc::uint32_t;
        iocb.aio_lio_opcode = c_api::IOCB_CMD::PREAD;
        iocb.aio_buf = unsafe { std::mem::transmute(buf.as_mut_ptr()) };
        iocb.aio_nbytes = buf.size as libc::uint64_t;
        iocb.aio_offset = offset as libc::int64_t;

        ControlBlock { iocb, buf }
    }

    pub fn pwrite(fd: RawFd, buf: AlignedBuf, offset: usize) -> Self {
        let mut iocb = c_api::iocb::zeroed();
        iocb.aio_fildes = fd as libc::uint32_t;
        iocb.aio_lio_opcode = c_api::IOCB_CMD::PWRITE;
        iocb.aio_buf = unsafe { std::mem::transmute(buf.as_ptr()) };
        iocb.aio_nbytes = buf.size as libc::uint64_t;
        iocb.aio_offset = offset as libc::int64_t;

        ControlBlock { iocb, buf }
    }

    pub fn eventfd(mut self, fd: RawFd) -> Self {
        self.iocb.aio_flags |= c_api::IOCB_FLAG_RESFD;
        self.iocb.aio_resfd = fd as libc::uint32_t;
        self
    }

    #[inline]
    pub fn get_buf(&self) -> &AlignedBuf {
        &self.buf
    }

    #[inline]
    pub fn is_write(&self) -> bool {
        match self.iocb.aio_lio_opcode {
            c_api::IOCB_CMD::PWRITE => true,
            _ => false,
        }
    }

    #[inline]
    pub fn offset(&self) -> u64 {
        self.iocb.aio_offset as u64
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.iocb.aio_nbytes
    }

    pub fn into_buf(self) -> AlignedBuf {
        self.buf
    }
}

pub struct Context {
    context_id: c_api::aio_context_t,
    nr_events: usize,
    pending_events: Slab<ControlBlock>,
    ended_events: Vec<c_api::io_event>,
}

impl Context {
    pub fn setup(nr_events: usize) -> Result<Self> {
        let mut context_id = 0;

        c_api::io_setup(nr_events as libc::c_uint, &mut context_id).map(|_| {
            Context {
                pending_events: Slab::with_capacity(nr_events),
                context_id,
                nr_events,
                ended_events: vec![c_api::io_event::zeroed(); nr_events],
            }
        })
    }

    pub fn is_empty(&self) -> bool {
        self.pending_events.is_empty()
    }

    pub fn submit(&mut self, cb: ControlBlock) -> Result<EventId> {
        let entry = self.pending_events.vacant_entry();
        let key = entry.key();

        let event = entry.insert(cb);
        event.iocb.aio_data = key as libc::uint64_t;

        // NOTE: Assuming that Slab will not change addresses of contained
        // objects, as we are passing the pointer to the iocb!
        let iocbp = &event.iocb as *const _;
        c_api::io_submit(self.context_id, 1, &iocbp).map(|_| key)
    }

    pub fn get_events(&mut self,
                      min_nr: usize,
                      max_nr: Option<usize>,
                      timeout: Option<Duration>,
                      out_events: &mut Vec<Event>) -> Result<()>
    {
        let max_nr = max_nr.unwrap_or(self.nr_events);
        let c_timeout = timeout.map(|t| {
            libc::timespec {
                tv_sec: t.as_secs() as libc::time_t,
                tv_nsec: t.subsec_nanos() as libc::c_long,
            }
        });

        out_events.clear();

        match c_api::io_getevents(self.context_id,
                                  min_nr as libc::c_long,
                                  max_nr as libc::c_long,
                                  self.ended_events.as_mut_ptr(),
                                  c_timeout.as_ref()) {
            Err(reason) => Err(reason),
            Ok(count) => {
                for e in &self.ended_events[0 .. count] {
                    let event_id = e.data as EventId;
                    if !self.pending_events.contains(event_id) {
                        panic!("unqueued event {} returned", event_id);
                    }
                    let cb = self.pending_events.remove(event_id);
                    out_events.push(Event::new(e, cb))
                }

                Ok(())
            }
        }
    }

    pub fn cancel(&mut self, event_id: EventId) -> Result<ControlBlock> {
        let iocbp = {
            let event = self.pending_events.get(event_id)
                .expect(&format!("event Id {} not queued", event_id));

            &event.iocb as *const _
        };

        let mut result = c_api::io_event::zeroed();

        // On Linux, 4.19 this is an O(n) operation over the number of
        // active I/Os, and it returns EINVAL if the I/O is not the list,
        // found by finding the iocbp user space pointer.

        c_api::io_cancel(self.context_id, iocbp, &mut result)?;

        Ok(self.pending_events.remove(event_id))
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        for event in self.pending_events.iter_mut() {
            let iocbp = &event.1.iocb as *const _;
            let mut result = c_api::io_event::zeroed();
            let _ = c_api::io_cancel(self.context_id, iocbp, &mut result);
        }
        c_api::io_destroy(self.context_id).unwrap();
    }
}

pub type EventId = usize;

pub struct Event {
    pub id: EventId,
    pub cb: ControlBlock,
    pub result: Result<usize>,
}

impl Event {
    fn new(e: &c_api::io_event, cb: ControlBlock) -> Self {
        let event_id = e.data as EventId;
        let result = if e.res < 0 {
            Err(IoError::from_raw_os_error(-e.res as i32))
        } else {
            Ok(e.res as usize)
        };

        Event {
            id: event_id,
            cb,
            result: result,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use libc;
    use std::ffi::CString;
    use std::fs::File;
    use std::os::unix::io::AsRawFd;

    #[test]
    fn it_works() {
        let mut ctx = Context::setup(1).unwrap();
        let mut events = vec![];
        ctx.get_events(0, None, None, &mut events).unwrap();
        assert_eq!(0, events.len());
    }

    #[test]
    fn read_test() {
        let mut context = Context::setup(1).unwrap();
        let f = File::open("/dev/zero").unwrap();
        let raw_fd = f.as_raw_fd();
        let buf_size = 100;
        let buf = AlignedBuf::new(buf_size); // TODO: fill elements by 1

        let event_id = context.submit(ControlBlock::pread(raw_fd, buf, 0)).unwrap();
        let mut events = vec![];
        context.get_events(1, Some(1), None, &mut events).unwrap();
        let event = events.into_iter().nth(0).unwrap();
        assert_eq!(event_id, event.id);
        assert_eq!(buf_size, event.result.unwrap());
        assert!(event.cb.buf.iter().all(|&x| x == 0));
    }

    #[test]
    fn cancel_test() {
        let raw_fd = unsafe {
            libc::open(CString::new("/etc/hosts").unwrap().as_ptr(),
                       libc::O_RDONLY | libc::O_DIRECT)
        };
        assert!(raw_fd >= 0);

        let mut context = Context::setup(1).unwrap();
        let buf = AlignedBuf::new(100);

        let event_id = context.submit(ControlBlock::pread(raw_fd, buf, 0)).unwrap();

        // Unless we can prevent the kernel from performing the I/OS somehow before
        // we manage to call 'cancel', this test cannot determine the return value
        // of cancel.
        let _ = context.cancel(event_id);
    }
}
