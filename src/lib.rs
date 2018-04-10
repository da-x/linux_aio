// Copyright 2016 Takeru Ohta <phjgt308@gmail.com>
// See the LICENSE file at the top-level directory of this distribution.

extern crate libc;

pub mod c_api;

use std::os::unix::io::RawFd;
use std::io::Error as IoError;
use std::time::Duration;
use std::collections::HashMap;
use std::ops::Deref;
use std::ops::DerefMut;

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
            size: size,
            capacity: allocate_size,
        }
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
    iocb: Box<c_api::iocb>,
    buf: Option<AlignedBuf>,
}

impl ControlBlock {
    pub fn pread(fd: RawFd, mut buf: AlignedBuf, offset: usize) -> Self {
        let mut iocb = c_api::iocb::zeroed();
        iocb.aio_fildes = fd as libc::uint32_t;
        iocb.aio_lio_opcode = c_api::IOCB_CMD::PREAD;
        iocb.aio_buf = unsafe { std::mem::transmute(buf.as_mut_ptr()) };
        iocb.aio_nbytes = buf.size as libc::uint64_t;
        iocb.aio_offset = offset as libc::int64_t;
        ControlBlock {
            iocb: Box::new(iocb),
            buf: Some(buf),
        }
    }

    pub fn pwrite(fd: RawFd, buf: AlignedBuf, offset: usize) -> Self {
        let mut iocb = c_api::iocb::zeroed();
        iocb.aio_fildes = fd as libc::uint32_t;
        iocb.aio_lio_opcode = c_api::IOCB_CMD::PWRITE;
        iocb.aio_buf = unsafe { std::mem::transmute(buf.as_ptr()) };
        iocb.aio_nbytes = buf.size as libc::uint64_t;
        iocb.aio_offset = offset as libc::int64_t;
        ControlBlock {
            iocb: Box::new(iocb),
            buf: Some(buf),
        }
    }

    pub fn eventfd(mut self, fd: RawFd) -> Self {
        self.iocb.aio_flags |= c_api::IOCB_FLAG_RESFD;
        self.iocb.aio_resfd = fd as libc::uint32_t;
        self
    }
}

pub struct Context {
    context_id: c_api::aio_context_t,
    next_id: EventId,
    submitted: HashMap<EventId, ControlBlock>,
    executed: HashMap<EventId, Event>,
}

impl Context {
    pub fn is_empty(&self) -> bool {
        self.submitted.is_empty() && self.executed.is_empty()
    }

    pub fn setup(nr_events: usize) -> Result<Self> {
        let mut context_id = 0;
        c_api::io_setup(nr_events as libc::c_uint, &mut context_id).map(|_| {
            Context {
                context_id: context_id,
                next_id: 1,
                submitted: HashMap::new(),
                executed: HashMap::new(),
            }
        })
    }

    pub fn submit(&mut self, mut cb: ControlBlock) -> Result<EventId> {
        let event_id = self.next_id;
        cb.iocb.aio_data = event_id as libc::uint64_t;

        let iocbp = cb.iocb.as_ref() as *const _;
        c_api::io_submit(self.context_id, 1, &iocbp).map(|count| {
            assert_eq!(1, count);
            self.next_id = self.next_id.wrapping_add(1);
            self.submitted.insert(event_id, cb);
            event_id
        })
    }

    pub fn get_events(&mut self,
                      min_nr: usize,
                      nr: usize,
                      timeout: Option<Duration>)
                      -> Result<Vec<Event>> {
        let prefetched_ids: Vec<_> = self.executed.keys().cloned().take(nr).collect();
        let mut events = Vec::with_capacity(prefetched_ids.len());
        for id in prefetched_ids {
            let event = self.executed.remove(&id).unwrap();
            events.push(event);
        }
        if events.len() == nr {
            return Ok(events);
        }

        let nr = nr - events.len();
        let min_nr = if min_nr > events.len() {
            min_nr - events.len()
        } else {
            0
        };
        let mut io_events = vec![c_api::io_event::zeroed(); nr];
        let c_timeout = timeout.map(|t| {
            libc::timespec {
                tv_sec: t.as_secs() as libc::time_t,
                tv_nsec: t.subsec_nanos() as libc::c_long,
            }
        });
        match c_api::io_getevents(self.context_id,
                                  min_nr as libc::c_long,
                                  nr as libc::c_long,
                                  io_events.as_mut_ptr(),
                                  c_timeout.as_ref()) {
            Err(reason) => {
                if events.is_empty() {
                    Err(reason)
                } else {
                    Ok(events)
                }
            }
            Ok(count) => {
                for e in &io_events[0..count] {
                    let event_id = e.data as EventId;
                    let cb = self.submitted.remove(&event_id).unwrap();
                    events.push(Event::new(e, cb))
                }
                Ok(events)
            }
        }
    }

    pub fn cancel(&mut self, event_id: EventId) -> Result<Option<Event>> {
        if let Some(event) = self.executed.remove(&event_id) {
            return Ok(Some(event));
        }

        if let Some(cb) = self.submitted.remove(&event_id) {
            // NOTE: io_cancel(2) may not work (always return EINVAL)
            let mut result = c_api::io_event::zeroed();
            match c_api::io_cancel(self.context_id, cb.iocb.as_ref(), &mut result) {
                Ok(_) => Ok(Some(Event::new(&result, cb))),
                Err(reason) => {
                    self.submitted.insert(event_id, cb);

                    self.fetch_events();
                    if let Some(event) = self.executed.remove(&event_id) {
                        return Ok(Some(event));
                    }
                    Err(reason)
                }
            }
        } else {
            Ok(None)
        }
    }

    fn fetch_events(&mut self) {
        let max_events = self.submitted.len();
        for e in self.get_events(0, max_events, None).unwrap_or(vec![]) {
            self.executed.insert(e.id, e);
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        let event_ids = self.submitted.keys().cloned().collect::<Vec<_>>();
        for event_id in event_ids.into_iter() {
            let _ = self.cancel(event_id);
        }
        c_api::io_destroy(self.context_id).unwrap();
    }
}

pub type EventId = usize;

pub struct Event {
    pub id: EventId,
    pub buf: Option<AlignedBuf>,
    pub result: Result<usize>,
}

impl Event {
    fn new(e: &c_api::io_event, mut cb: ControlBlock) -> Self {
        let event_id = e.data as EventId;
        let result = if e.res < 0 {
            Err(IoError::from_raw_os_error(-e.res as i32))
        } else {
            Ok(e.res as usize)
        };
        Event {
            id: event_id,
            buf: cb.buf.take(),
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
        let events = ctx.get_events(0, 0, None).unwrap();
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
        let event = context.get_events(1, 1, None).unwrap().into_iter().nth(0).unwrap();
        assert_eq!(event_id, event.id);
        assert_eq!(buf_size, event.result.unwrap());
        assert!(event.buf.unwrap().iter().all(|&x| x == 0));
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
        context.cancel(event_id).expect("Failed to cancel");
    }
}
