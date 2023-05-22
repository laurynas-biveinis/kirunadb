// Copyright (C) 2022-2023 Laurynas Biveinis
use crate::{transaction_manager::TransactionChange, DbError};
use cap_std::fs::{Dir, File, OpenOptions};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::io::{self, Read, Write};

#[derive(Debug)] // COV_EXCL_LINE
pub struct Log {
    file: File,
    max_logged_node_id: u64,
}

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum ChangeId {
    NewNode = 0,
}

impl ChangeId {
    fn new(transaction_change: &TransactionChange) -> Self {
        match transaction_change {
            TransactionChange::NewNode(_) => Self::NewNode,
        }
    }
}

impl Log {
    // TODO(laurynas): here and below: s/&str/Path or something?
    pub fn open(dir_handle: &Dir, log_file_name: &str, create: bool) -> Result<Log, DbError> {
        let mut file = if create {
            dir_handle.open_with(
                log_file_name,
                OpenOptions::new().read(true).write(true).create_new(true),
            )
        } else {
            dir_handle.open_with(log_file_name, OpenOptions::new().read(true).write(true))
        }?;
        let max_logged_node_id = if create {
            0
        } else {
            let mut max_logged_node_id = 0;
            // TODO(laurynas): MaybeUninit?
            let mut one_byte_buf = [0; 1];
            loop {
                let n = file.read(&mut one_byte_buf)?;
                if n == 0 {
                    break;
                }
                debug_assert!(n == 1);
                let type_byte = u8::from_ne_bytes(one_byte_buf);
                let change_type =
                    ChangeId::try_from(type_byte).map_err(|_foo| DbError::BadLogRecordType {
                        bad_type: type_byte,
                    })?;
                match change_type {
                    ChangeId::NewNode => {
                        let mut eight_byte_buf = [0; 8];
                        file.read_exact(&mut eight_byte_buf)?;
                        let node_id = u64::from_ne_bytes(eight_byte_buf);
                        match node_id.cmp(&max_logged_node_id) {
                            std::cmp::Ordering::Greater => max_logged_node_id = node_id,
                            std::cmp::Ordering::Equal => {
                                return Err(DbError::LoggedMultipleNodeIdAllocations { node_id })
                            }
                            std::cmp::Ordering::Less => {}
                        }
                    }
                }
            }
            max_logged_node_id
        };
        Ok(Log {
            file,
            max_logged_node_id,
        })
    }

    pub fn append(&mut self, changes: &Vec<TransactionChange>) -> Result<(), io::Error> {
        // TODO(laurynas): this is throwaway code anyway. Use serde (C-SERDE)
        for change in changes {
            let change_type_id: u8 = ChangeId::new(change).into();
            match change {
                TransactionChange::NewNode(new_art_descriptor) => {
                    self.file.write_all(&change_type_id.to_ne_bytes())?;
                    let node_id = new_art_descriptor.node_id();
                    self.file.write_all(&node_id.to_ne_bytes())?;
                }
            }
        }
        Ok(())
    }

    pub fn max_logged_node_id(&self) -> u64 {
        self.max_logged_node_id
    }
}
