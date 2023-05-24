// Copyright (C) 2023 Laurynas Biveinis
#![deny(clippy::pedantic)]
#![allow(clippy::unwrap_used)]

use kirunadb::transaction_manager::Transaction;
use kirunadb::Db;
use kirunadb_test_helpers::get_temp_dir;
use kirunadb_test_helpers::open_db_err;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

fn commit_ok(mut t: Transaction) {
    let commit_result = t.commit();
    assert!(commit_result.is_ok());
}

fn open_log_for_corruption(db_path: &Path) -> File {
    let log_path = db_path.join("LOG");
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(log_path)
        .unwrap()
}

fn expect_u64(file: &mut File, offset: u64, value: u64) {
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut u64_buf = [0; 8];
    file.read_exact(&mut u64_buf).unwrap();
    let existing = u64::from_ne_bytes(u64_buf);
    assert_eq!(existing, value);
}

fn replace_u64(file: &mut File, offset: u64, expected: u64, new: u64) {
    expect_u64(file, offset, expected);
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&new.to_ne_bytes()).unwrap();
}

fn replace_u8(file: &mut File, offset: u64, expected: u8, new: u8) {
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut u8_buf = [0; 1];
    file.read_exact(&mut u8_buf).unwrap();
    let existing = u8::from_ne_bytes(u8_buf);
    assert_eq!(existing, expected);
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&new.to_ne_bytes()).unwrap();
}

#[test]
fn sequential_transaction_ids() {
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
fn interleaved_transaction_ids() {
    let temp_dir = get_temp_dir();
    let path = temp_dir.path();
    let mut db = Db::open(path).unwrap();
    let t1 = db.begin_transaction();
    let t1_id = t1.id();
    let t2 = db.begin_transaction();
    let t2_id = t2.id();
    commit_ok(t1);
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

#[test]
fn node_id_assignment_consistent_on_reopen() {
    let temp_dir = get_temp_dir();
    let path = temp_dir.path();
    let n1_id;
    {
        let mut created_db = Db::open(path).unwrap();
        let mut transaction = created_db.begin_transaction();
        n1_id = transaction.new_art_descriptor_node();
        commit_ok(transaction);
    }
    {
        let mut opened_db = Db::open(path).unwrap();
        let mut transaction = opened_db.begin_transaction();
        let n2_id = transaction.new_art_descriptor_node();
        commit_ok(transaction);
        assert_ne!(n1_id, n2_id);
    }
}

#[test]
fn node_id_assignment_consistent_on_reopen_two_ids() {
    let temp_dir = get_temp_dir();
    let path = temp_dir.path();
    let n1_id;
    let n2_id;
    {
        let mut created_db = Db::open(path).unwrap();
        let mut t1 = created_db.begin_transaction();
        n1_id = t1.new_art_descriptor_node();
        commit_ok(t1);
        let mut t2 = created_db.begin_transaction();
        n2_id = t2.new_art_descriptor_node();
        commit_ok(t2);
    }
    {
        let mut opened_db = Db::open(path).unwrap();
        let mut transaction = opened_db.begin_transaction();
        let n3_id = transaction.new_art_descriptor_node();
        commit_ok(transaction);
        assert_ne!(n1_id, n3_id);
        assert_ne!(n2_id, n3_id);
    }
}

#[test]
fn node_id_assignment_consistent_on_reopen_two_ids_lower_id_committed_later() {
    let temp_dir = get_temp_dir();
    let path = temp_dir.path();
    let n1_id;
    let n2_id;
    {
        let mut created_db = Db::open(path).unwrap();
        let mut t1 = created_db.begin_transaction();
        n1_id = t1.new_art_descriptor_node();
        let mut t2 = created_db.begin_transaction();
        n2_id = t2.new_art_descriptor_node();
        commit_ok(t2);
        commit_ok(t1);
    }
    {
        let mut opened_db = Db::open(path).unwrap();
        let mut transaction = opened_db.begin_transaction();
        let n3_id = transaction.new_art_descriptor_node();
        commit_ok(transaction);
        assert_ne!(n1_id, n3_id);
        assert_ne!(n2_id, n3_id);
    }
}

#[test]
fn node_id_assignment_corruption_repeated_id() {
    let temp_dir = get_temp_dir();
    let path = temp_dir.path();
    let n1_id;
    let n2_id;
    {
        let mut created_db = Db::open(path).unwrap();
        let mut t1 = created_db.begin_transaction();
        n1_id = t1.new_art_descriptor_node();
        commit_ok(t1);
        let mut t2 = created_db.begin_transaction();
        n2_id = t2.new_art_descriptor_node();
        commit_ok(t2);
    }
    {
        let mut log_file = open_log_for_corruption(path);
        expect_u64(&mut log_file, 1, n1_id);
        replace_u64(&mut log_file, 10, n2_id, n1_id);
    }
    open_db_err(path);
}

#[test]
fn log_corruption_unknown_type() {
    let temp_dir = get_temp_dir();
    let path = temp_dir.path();
    {
        let mut created_db = Db::open(path).unwrap();
        let mut transaction = created_db.begin_transaction();
        transaction.new_art_descriptor_node();
        commit_ok(transaction);
    }
    {
        let mut log_file = open_log_for_corruption(path);
        replace_u8(&mut log_file, 0, 0, 0xBD);
    }
    open_db_err(path);
}
