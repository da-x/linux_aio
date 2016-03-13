linux_aio
=========

A thin Rust wrapper on top of the Linux AIO API

NOTICE
------

This is an unstable experimental library

Example
-------

```Rust
//
// Reads 100 bytes from the "/dev/zero" file
//
use std::fs::File;
use std::os::unix::io::AsRawFd;
use linux_aio::*;

fn main() {
  let mut context = Context::setup(1).unwrap();
  let f = File::open("/dev/zero").unwrap();
  let raw_fd = f.as_raw_fd();
  let buf = AlignedBuf::new(100);

  let event_id = context.submit(ControlBlock::pread(raw_fd, buf, 0)).unwrap();
  let event = context.get_events(1, 1, None).unwrap().into_iter().nth(0).unwrap();
  assert_eq!(100, event.result.unwrap());
  assert!(event.buf.unwrap().iter().all(|&x| x == 0));
}
```

For more examples, see "examples/*.rs" files.

References
----------

- https://www.fsl.cs.sunysb.edu/~vass/linux-aio.txt
- http://www.oreilly.co.jp/community/blog/2010/09/buffer-cache-and-aio-part3.html
