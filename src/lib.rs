#![deny(unused_must_use)]

use std::{collections::HashMap, path::Path, time::Duration};

pub mod error;
pub mod memtable;
pub mod sstable;
pub mod table;
pub mod util;

pub fn fixme<T>(arg: T) -> T {
    arg
}

#[deprecated = "FIXME"]
pub fn fixme_msg<T>(arg: T, _msg: &'static str) -> T {
    arg
}

pub async fn dump_write_log_to_println(path: &Path) {
    crate::memtable::MemTable::dump_write_log_to_println(path).await
}
