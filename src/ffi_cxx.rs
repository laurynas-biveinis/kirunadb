// Copyright (C) 2022-2023 Laurynas Biveinis
use crate::transaction_manager::Transaction;
use crate::Db;
use std::io;
use std::path::Path;

#[cxx::bridge(namespace = "kirunadb")]
#[allow(
    clippy::items_after_statements,
    clippy::let_underscore_drop,
    clippy::trait_duplication_in_bounds,
    clippy::used_underscore_binding
)]
pub mod interface {
    extern "Rust" {
        type Transaction;

        pub fn get_id(self: &Transaction) -> u64;

        pub fn new_art_descriptor_node(self: &mut Transaction) -> u64;

        // Assuming pessimistic locking so that a failure to commit is
        // exceptional
        pub fn commit(self: &mut Transaction) -> Result<()>;

        pub fn drop_transaction(transaction: Box<Transaction>);

        type Db;

        pub fn open(path: &str) -> Result<Box<Db>>;

        pub fn close(db: Box<Db>);

        unsafe fn begin_transaction(self: &mut Db) -> Box<Transaction>;
    }
}

pub fn drop_transaction(transaction: Box<Transaction>) {
    std::mem::drop(transaction);
}

// TODO(laurynas): C++17 std::path?
pub fn open(path: &str) -> Result<Box<Db>, io::Error> {
    let path = Path::new(path);
    let db = Db::open(path)?;
    Ok(Box::new(db))
}

pub fn close(db: Box<Db>) {
    std::mem::drop(db);
}
