// Copyright (C) 2022-2023 Laurynas Biveinis
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::buffer_manager::BufferManager;
use crate::log::Log;

#[derive(Debug)] // COV_EXCL_LINE
pub enum TransactionChange {
    NewNode(TransactionChangeNewNode),
}

#[derive(Debug)] // COV_EXCL_LINE
pub struct TransactionChangeNewNode {
    node_id: u64,
}

impl TransactionChangeNewNode {
    fn new(node_id: u64) -> TransactionChangeNewNode {
        TransactionChangeNewNode { node_id }
    }

    #[inline]
    #[must_use]
    pub fn node_id(&self) -> u64 {
        self.node_id
    }
}

#[derive(Debug)] // COV_EXCL_LINE
pub struct Transaction {
    manager: Rc<RefCell<TransactionManager>>,
    id: u64,
    changes: Vec<TransactionChange>,
}

impl Transaction {
    pub fn new(manager: &Rc<RefCell<TransactionManager>>, id: u64) -> Transaction {
        Transaction {
            manager: manager.clone(),
            id,
            changes: Vec::new(),
        }
    }

    /// # Errors
    /// Will return `io::Error` if it encounters any.
    pub fn commit(&mut self) -> Result<(), io::Error> {
        self.manager.borrow_mut().log_append(&self.changes)
    }

    // TODO(laurynas): Option a better fit? But cxx.rs not there yet.
    pub fn new_art_descriptor_node(&mut self) -> u64 {
        let new_node_trx_change = self.manager.borrow_mut().new_art_descriptor_node();
        let new_node_id = new_node_trx_change.node_id();
        let trx_change = TransactionChange::NewNode(new_node_trx_change);
        self.changes.push(trx_change);
        new_node_id
    }

    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Debug)] // COV_EXCL_LINE
pub struct TransactionManager {
    buffer_manager: BufferManager,
    log: Log,
    next_id: AtomicU64,
}

impl TransactionManager {
    pub fn new(buffer_manager: BufferManager, log: Log) -> TransactionManager {
        TransactionManager {
            buffer_manager,
            log,
            next_id: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn assign_next_id(&mut self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn new_art_descriptor_node(&mut self) -> TransactionChangeNewNode {
        let new_node_id = self.buffer_manager.allocate_new_node_id();
        TransactionChangeNewNode::new(new_node_id)
    }

    fn log_append(&mut self, changes: &Vec<TransactionChange>) -> Result<(), io::Error> {
        self.log.append(changes)
    }
}
