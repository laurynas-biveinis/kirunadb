// Copyright (C) 2022-2023 Laurynas Biveinis
use crate::{node::Id, transaction_manager::TransactionChange, DbError};
use cap_std::fs::{Dir, File, OpenOptions};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::{
    io::{self, Read, Write},
    path::Path,
};

#[derive(Debug)] // COV_EXCL_LINE
#[must_use]
pub struct Log {
    file: File,
    max_logged_node_id: Id,
}

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
#[must_use]
enum ChangeId {
    NewNode = 0,
}

impl ChangeId {
    #[inline]
    fn new(transaction_change: &TransactionChange) -> Self {
        match transaction_change {
            TransactionChange::NewNode(_) => Self::NewNode,
        }
    }
}

impl Log {
    pub fn open(dir_handle: &Dir, log_file_name: &Path, create: bool) -> Result<Self, DbError> {
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
            // We could use MaybeUninit here, but not worth it.
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
                                return Err(DbError::LoggedMultipleNodeIdAllocations {
                                    node_id: Id::from(node_id),
                                })
                            }
                            std::cmp::Ordering::Less => {}
                        }
                    }
                }
            }
            max_logged_node_id
        };
        Ok(Self {
            file,
            max_logged_node_id: Id::from(max_logged_node_id),
        })
    }

    pub fn append_change(&mut self, change: &TransactionChange) -> Result<(), io::Error> {
        let change_type_id: u8 = ChangeId::new(change).into();
        // TODO(laurynas): this is throwaway code anyway. Use serde (C-SERDE)
        match change {
            TransactionChange::NewNode(new_art_descriptor) => {
                self.file.write_all(&change_type_id.to_ne_bytes())?;
                let node_id = new_art_descriptor.node_id();
                self.file.write_all(&node_id.to_ne_bytes())?;
            }
        }
        Ok(())
    }

    pub fn append_changes(&mut self, changes: &Vec<TransactionChange>) -> Result<(), io::Error> {
        for change in changes {
            self.append_change(change)?;
        }
        Ok(())
    }

    #[inline]
    pub fn max_logged_node_id(&self) -> Id {
        self.max_logged_node_id
    }
}
