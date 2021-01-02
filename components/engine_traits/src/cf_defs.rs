// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, Ordering};

pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_LOCK: CfName = "lock";
pub const CF_WRITE: CfName = "write";
pub const CF_RAFT: CfName = "raft";
pub const CF_VER_DEFAULT: CfName = "ver_default";
pub const CF_RAW_DEFAULT: CfName = "raw_default";
// Cfs that should be very large generally.
pub const LARGE_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAW_DEFAULT];
pub const ALL_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT, CF_RAW_DEFAULT];
pub const TXN_DATA_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];
pub const RAW_DATA_CFS: &[CfName] = &[CF_RAW_DEFAULT];
pub const ALL_DATA_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAW_DEFAULT];
static SAFE_RAWKV_CF: AtomicBool = AtomicBool::new(false);

pub fn name_to_cf(name: &str) -> Option<CfName> {
    if name.is_empty() {
        return Some(CF_DEFAULT);
    }
    for c in ALL_CFS {
        if name == *c {
            return Some(c);
        }
    }

    None
}

fn name_to_rawkv_cf_unsafe(cf: &str) -> Option<CfName> {
    if cf.is_empty() {
        return Some(CF_DEFAULT);
    }
    for c in TXN_DATA_CFS {
        if cf == *c {
            return Some(c);
        }
    }

    None
}

fn name_to_rawkv_cf_safe(cf: &str) -> Option<CfName> {
    if cf.is_empty() || cf == CF_DEFAULT {
        return Some(CF_RAW_DEFAULT);
    }

    None
}

pub fn name_to_rawkv_cf(cf: &str) -> Option<CfName> {
    let result = if SAFE_RAWKV_CF.load(Ordering::Relaxed) {
        name_to_rawkv_cf_safe(cf)
    } else {
        name_to_rawkv_cf_unsafe(cf)
    };
    return result;
}

pub fn set_safe_rawkv(safe: bool) {
    SAFE_RAWKV_CF.store(safe, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name_to_rawkv_cf() {
        set_safe_rawkv(true);
        assert_eq!(name_to_rawkv_cf(""), CF_RAW_DEFAULT);
        assert_eq!(name_to_rawkv_cf(CF_DEFAULT), CF_RAW_DEFAULT);
        assert_eq!(name_to_rawkv_cf(""), CF_DEFAULT);
        assert_eq!(name_to_rawkv_cf(CF_DEFAULT), CF_DEFAULT);
        assert_eq!(name_to_rawkv_cf(CF_LOCK), CF_LOCK);
        assert_eq!(name_to_rawkv_cf(CF_WRITE), CF_WRITE);
    }
}
