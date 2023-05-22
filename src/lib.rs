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
use thiserror::Error;
use transaction_manager::Transaction;
use transaction_manager::TransactionManager;

#[derive(Error, Debug)] // COV_EXCL_LINE
pub enum DbError {
    #[error("I/O Error")]
    Io(#[from] io::Error),
    #[error("Corruption: incorrect log record type {bad_type}")]
    BadLogRecordType { bad_type: u8 },
    #[error("Corruption: logged multiple allocations for the same node ID {node_id}")]
    LoggedMultipleNodeIdAllocations { node_id: u64 },
}

// Do the simplest thing that works. Later generalize to being able to contain
// multiple ARTs, with different key types.
#[derive(Debug)] // COV_EXCL_LINE
pub struct Db {
    _dir_handle: Dir,
    transaction_manager: Rc<RefCell<TransactionManager>>,
}

impl Db {
    const VERSION_FILE_NAME: &str = "VERSION";
    const LOG_FILE_NAME: &str = "LOG";

    // TODO(laurynas): custom error for "DB already exists at this path", etc. (C-GOOD-ERR)
    /// # Errors
    /// Will return `DbError` if it encounters any.
    pub fn open(path: &Path) -> Result<Db, DbError> {
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
                        return Err(DbError::Io(error));
                    }
                }
                _ => {
                    return Err(DbError::Io(error));
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
        let buffer_manager = BufferManager::new(log.max_logged_node_id() + 1);
        let transaction_manager = TransactionManager::new(buffer_manager, log);
        Ok(Db {
            _dir_handle: dir_handle,
            transaction_manager: Rc::new(RefCell::new(transaction_manager)),
        })
    }

    pub fn begin_transaction(&mut self) -> Transaction {
        let new_transaction_id = self.transaction_manager.borrow_mut().assign_next_id();
        Transaction::new(&self.transaction_manager, new_transaction_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::transaction_manager::Transaction;
    use crate::Db;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn get_temp_dir() -> TempDir {
        TempDir::new().expect("Creating a temp dir must succeed")
    }

    fn make_path_read_only(path: &Path) {
        let mut path_permissions = path.metadata().unwrap().permissions();
        path_permissions.set_readonly(true);
        std::fs::set_permissions(path, path_permissions).unwrap();
    }

    fn commit_ok(mut t: Transaction) {
        let commit_result = t.commit();
        assert!(commit_result.is_ok());
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
    fn create_db_relative_path_arg() {
        let temp_dir = get_temp_dir();
        let saved_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();
        let db = Db::open(Path::new("relative_under_temp"));
        assert!(db.is_ok());
        std::env::set_current_dir(saved_cwd).unwrap();
    }

    #[test]
    fn try_open_db_nonexisting_path() {
        let non_existing_path = Path::new("/non/ex/ist/ing/p/ath");
        let db = Db::open(non_existing_path);
        assert!(db.is_err());
    }

    #[test]
    #[cfg_attr(target_os = "windows", ignore)]
    fn try_open_db_inaccessible_path() {
        let temp_dir = get_temp_dir();
        let path = temp_dir.path();
        make_path_read_only(path);
        let db = Db::open(path);
        assert!(db.is_err());
    }

    #[test]
    #[cfg_attr(target_os = "windows", ignore)]
    fn try_create_db_inaccessible_path() {
        let temp_dir = get_temp_dir();
        let path = temp_dir.path();
        make_path_read_only(path);
        let nonexisting_path = path.join("nonexistingdir");
        let db = Db::open(&nonexisting_path);
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

    #[test]
    fn try_open_db_missing_log() {
        let temp_dir = get_temp_dir();
        let path = temp_dir.path();
        {
            let created_db = Db::open(path);
            assert!(created_db.is_ok());
        }
        let version_path = path.join("LOG");
        fs::remove_file(version_path).expect("Deleting LOG must succeed");
        {
            let db = Db::open(path);
            assert!(db.is_err());
        }
    }

    #[test]
    fn begin_transaction() {
        let temp_dir = get_temp_dir();
        let path = temp_dir.path();
        let mut db = Db::open(path).unwrap();
        let _transaction = db.begin_transaction();
    }

    #[test]
    fn transaction_ids_sequential() {
        let temp_dir = get_temp_dir();
        let path = temp_dir.path();
        let mut db = Db::open(path).unwrap();
        let t1 = db.begin_transaction();
        let t1_id = t1.id();
        commit_ok(t1);
        let t2 = db.begin_transaction();
        let t2_id = t2.id();
        commit_ok(t2);
        assert_ne!(t1_id, t2_id);
    }

    #[test]
    fn transaction_new_node() {
        let temp_dir = get_temp_dir();
        let path = temp_dir.path();
        let mut db = Db::open(path).unwrap();
        let mut transaction = db.begin_transaction();
        let _new_node_id = transaction.new_art_descriptor_node();
        commit_ok(transaction);
    }

    #[test]
    fn transaction_two_new_nodes() {
        let temp_dir = get_temp_dir();
        let path = temp_dir.path();
        let mut db = Db::open(path).unwrap();
        let mut t1 = db.begin_transaction();
        let t1_new_node_id = t1.new_art_descriptor_node();
        commit_ok(t1);
        let mut t2 = db.begin_transaction();
        let t2_new_node_id = t2.new_art_descriptor_node();
        commit_ok(t2);
        assert_ne!(t1_new_node_id, t2_new_node_id);
    }

    // TODO(laurynas): missing VERSION/LOG tests
}
