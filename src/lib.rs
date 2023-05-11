// Copyright (C) 2022-2023 Laurynas Biveinis
#![deny(clippy::pedantic)]

mod buffer_manager;
mod ffi_cxx;
mod log;
mod transaction_manager;

use crate::log::Log;
use buffer_manager::BufferManager;
use cap_std::fs::Dir;
use cap_std::fs::OpenOptions;
use std::cell::RefCell;
use std::env;
use std::io;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use transaction_manager::Transaction;
use transaction_manager::TransactionManager;

// Do the simplest thing that works. Later generalize to being able to contain
// multiple ARTs, with different key types.
#[derive(Debug)]
pub struct Db {
    _dir_handle: Dir,
    transaction_manager: Rc<RefCell<TransactionManager>>,
}

impl Db {
    const VERSION_FILE_NAME: &str = "VERSION";
    const LOG_FILE_NAME: &str = "LOG";

    // TODO(laurynas): custom error for "DB already exists at this path", etc. (C-GOOD-ERR)
    /// # Errors
    /// Will return `io::Error` if it encounters any.
    pub fn open(path: &Path) -> Result<Db, io::Error> {
        let absolute_path: PathBuf = if path.is_absolute() {
            path.to_path_buf()
        } else {
            let current_dir = env::current_dir()?;
            current_dir.join(path)
        };
        let dir_handle_result = Dir::open_ambient_dir(&absolute_path, cap_std::ambient_authority());
        let dir_handle = match dir_handle_result {
            Ok(dir_handle) => dir_handle,
            Err(error) => match error.kind() {
                ErrorKind::NotFound => {
                    let parent_path_opt = absolute_path.parent();
                    let dir_opt = path.file_name();
                    if let (Some(parent_path), Some(dir)) = (parent_path_opt, dir_opt) {
                        let parent_dir_handle =
                            Dir::open_ambient_dir(parent_path, cap_std::ambient_authority())?;
                        parent_dir_handle.create_dir(dir)?;
                        parent_dir_handle.open_dir(dir)?
                    } else {
                        return Err(error);
                    }
                }
                _ => {
                    return Err(error);
                }
            },
        };

        let is_dir_empty = dir_handle.read_dir(".")?.next().is_none();
        {
            let _version_file = if is_dir_empty {
                dir_handle.open_with(
                    Db::VERSION_FILE_NAME,
                    OpenOptions::new().write(true).create_new(true),
                )
            } else {
                dir_handle.open_with(Db::VERSION_FILE_NAME, OpenOptions::new().read(true))
            }?;
        }
        let log = Log::open(&dir_handle, Db::LOG_FILE_NAME, is_dir_empty)?;
        let buffer_manager = BufferManager::init();
        let transaction_manager = TransactionManager::new(buffer_manager, log);
        Ok(Db {
            _dir_handle: dir_handle,
            transaction_manager: Rc::new(RefCell::new(transaction_manager)),
        })
    }

    pub fn begin_transaction(&mut self) -> Box<Transaction> {
        let new_transaction_id = self.transaction_manager.borrow_mut().assign_next_id();
        Box::new(Transaction::new(
            &self.transaction_manager,
            new_transaction_id,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::Db;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn get_temp_dir() -> TempDir {
        TempDir::new().expect("Creating a temp dir must succeed")
    }

    #[test]
    fn create_db_in_existing_empty_dir() {
        let temp_dir = get_temp_dir();
        let path = temp_dir.path();
        let nonexisting_path = path.join("nonexistingdir");
        let db = Db::open(&nonexisting_path);
        assert!(db.is_ok());
    }

    #[test]
    fn create_db_create_dir() {
        let temp_dir = get_temp_dir();
        let path = temp_dir.path();
        let db = Db::open(path);
        assert!(db.is_ok());
    }

    #[test]
    fn try_open_db_nonexisting_path() {
        let non_existing_path = Path::new("/non/ex/ist/ing/p/ath");
        let db = Db::open(non_existing_path);
        assert!(db.is_err());
    }

    #[test]
    fn open_created_db() {
        let temp_dir = get_temp_dir();
        let path = temp_dir.path();
        {
            let created_db = Db::open(path);
            assert!(created_db.is_ok());
        }
        {
            let opened_db = Db::open(path);
            assert!(opened_db.is_ok());
        }
    }

    #[test]
    fn try_open_db_missing_version() {
        let temp_dir = get_temp_dir();
        let path = temp_dir.path();
        {
            let created_db = Db::open(path);
            assert!(created_db.is_ok());
        }
        let version_path = path.join("VERSION");
        fs::remove_file(version_path).expect("Deleting VERSION must succeed");
        {
            let db = Db::open(path);
            assert!(db.is_err());
        }
    }

    // TODO(laurynas): missing VERSION/LOG tests
}
