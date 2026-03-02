use std::{fs::File, io::Read, str};

use nix::sys::resource::Usage;
#[cfg(unix)]
use nix::sys::resource::{UsageWho, getrusage};

use crate::{Error, Result, checked};

pub fn rss() -> Result<usize> {
	statm(0)
		.and_then(|pages| Ok((pages, super::page_size()?)))
		.and_then(|(pages, page_size)| checked!(pages * page_size))
}

#[cfg(target_os = "linux")]
pub fn statm(idx: usize) -> Result<usize> {
	File::open("/proc/self/statm")
		.map_err(Error::from)
		.and_then(|mut fp| {
			let mut buf = [0; 64];
			let len = fp.read(&mut buf)?;

			str::from_utf8(&buf[0..len])?
				.split_ascii_whitespace()
				.map(str::parse::<usize>)
				.nth(idx)
				.transpose()
				.map_err(Into::into)
		})
		.transpose()
		.expect("missing contents of statm")
}

#[cfg(not(target_os = "linux"))]
pub fn statm(idx: usize) -> Result<usize> {
	Err!("proc_pid_statm(5) only available on linux systems")
}

#[cfg(unix)]
pub fn usage() -> Result<Usage> { getrusage(UsageWho::RUSAGE_SELF).map_err(Into::into) }

#[cfg(not(unix))]
pub fn usage() -> Result<Usage> { Ok(Usage::default()) }

#[cfg(any(
	target_os = "linux",
	target_os = "freebsd",
	target_os = "openbsd"
))]
pub fn thread_usage() -> Result<Usage> { getrusage(UsageWho::RUSAGE_THREAD).map_err(Into::into) }

#[cfg(not(any(
	target_os = "linux",
	target_os = "freebsd",
	target_os = "openbsd"
)))]
pub fn thread_usage() -> Result<Usage> { Ok(Usage::default()) }
