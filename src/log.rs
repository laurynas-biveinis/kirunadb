// Copyright (C) 2022-2023 Laurynas Biveinis
use crate::transaction_manager::TransactionChange;
use cap_std::fs::{Dir, File, OpenOptions};
use std::io::{self, Seek, Write};

#[derive(Debug)]
pub struct Log {
    file: File,
}

impl Log {
    // TODO(laurynas): here and below: s/&str/Path or something?
    pub fn open(dir_handle: &Dir, log_file_name: &str, create: bool) -> Result<Log, io::Error> {
        let file = if create {
            dir_handle.open_with(
                log_file_name,
                OpenOptions::new().read(true).write(true).create_new(true),
            )
        } else {
            dir_handle.open_with(log_file_name, OpenOptions::new().read(true).write(true))
        }?;
        Ok(Log { file })
    }

    pub fn append(&mut self, changes: &Vec<TransactionChange>) -> Result<(), io::Error> {
        // TODO(laurynas): this is throwaway code anyway
        assert!(self.file.stream_position()? == 0);
        for change in changes {
            match change {
                TransactionChange::NewArtDescriptor(new_art_descriptor) => {
                    let change_type_id = Self::change_type_id(change);
                    self.file.write_all(&change_type_id.to_ne_bytes())?;
                    assert!(self.file.stream_position()? == 1);
                    let node_id = new_art_descriptor.descriptor_node_id();
                    self.file.write_all(&node_id.to_ne_bytes())?;
                    assert!(self.file.stream_position()? == 9);
                }
            }
        }
        Ok(())
    }

    fn change_type_id(change: &TransactionChange) -> u8 {
        match change {
            TransactionChange::NewArtDescriptor(_) => 0,
        }
    }
}
