use std::{collections::HashMap, time::Duration};

pub mod error;
pub mod sstable;
pub mod table;

pub fn fixme<T>(arg: T) -> T {
    arg
}

#[deprecated = "FIXME"]
pub fn fixme_msg<T>(arg: T, _msg: &'static str) -> T {
    arg
}
