// Copyright(C) 2023 Laurynas Biveinis
#![deny(clippy::pedantic)]

use kirunadb::Db;
use std::path::Path;
use tempfile::TempDir;

pub fn get_temp_dir() -> TempDir {
    TempDir::new().unwrap()
}

pub fn open_db_err(path: &Path) {
    let db = Db::open(path);
    assert!(db.is_err());
}
