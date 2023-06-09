// Copyright (C) 2022-2023 Laurynas Biveinis
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::buffer_manager::BufferManager;
use crate::log::Log;

#[derive(Debug)] // COV_EXCL_LINE
#[must_use]
pub enum TransactionChange {
    // Allowed only in system transactions:
    NewNode(TransactionChangeNewNode),
    // Allowed only in user transactions:
}

// Allowed only in system transactions
#[derive(Debug)] // COV_EXCL_LINE
#[must_use]
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
#[must_use]
struct SystemTransaction {
    manager: Rc<RefCell<TransactionManager>>,
    // Let's try not having an ID until absolutely necessary.
    // Start with a single-action system transaction as it's the most efficient. If needed, support
    // multiple changes later.
    change: Option<TransactionChange>,
}

impl SystemTransaction {
    fn commit(&mut self) -> Result<(), io::Error> {
        debug_assert!(self.change.is_some());
        if let Some(change) = &self.change {
            // TODO(laurynas): no need to fsync
            self.manager.borrow_mut().log_append_change(change)?;
        }
        Ok(())
    }

    fn commit_new_art_descriptor_node(&mut self) -> Result<u64, io::Error> {
        let new_node_trx_change = self.manager.borrow_mut().new_art_descriptor_node();
        let new_node_id = new_node_trx_change.node_id();
        let trx_change = TransactionChange::NewNode(new_node_trx_change);
        self.change = Some(trx_change);
        self.commit()?;
        Ok(new_node_id)
    }
}

#[derive(Debug)] // COV_EXCL_LINE
#[must_use]
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

    fn start_system_transaction(&self) -> SystemTransaction {
        SystemTransaction {
            manager: self.manager.clone(),
            change: None,
        }
    }

    /// # Errors
    /// Returns `io::Error` if an internal system transaction commit failed with one.
    pub fn new_art_descriptor_node(&self) -> Result<u64, io::Error> {
        let mut system_transaction = self.start_system_transaction();
        system_transaction.commit_new_art_descriptor_node()
    }

    /// # Errors
    /// Will return `io::Error` if it encounters any.
    pub fn commit(&mut self) -> Result<(), io::Error> {
        self.manager.borrow_mut().log_append_changes(&self.changes)
    }

    #[inline]
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Debug)] // COV_EXCL_LINE
#[must_use]
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
    #[must_use]
    pub fn assign_next_id(&mut self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn new_art_descriptor_node(&mut self) -> TransactionChangeNewNode {
        let new_node_id = self.buffer_manager.allocate_new_node_id();
        TransactionChangeNewNode::new(new_node_id)
    }

    fn log_append_change(&mut self, change: &TransactionChange) -> Result<(), io::Error> {
        self.log.append_change(change)
    }

    fn log_append_changes(&mut self, changes: &Vec<TransactionChange>) -> Result<(), io::Error> {
        self.log.append_changes(changes)
    }
}
