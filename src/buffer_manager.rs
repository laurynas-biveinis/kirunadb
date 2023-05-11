// Copyright (C) 2022-2023 Laurynas Biveinis
use std::sync::atomic::AtomicU64;

#[derive(Debug)]
pub struct BufferManager {
    next_node_id: AtomicU64,
}

impl BufferManager {
    pub const NULL_NODE_ID: u64 = 0;

    // TODO(laurynas): pass size
    pub fn init() -> BufferManager {
        BufferManager {
            next_node_id: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn allocate_new_node_id(&mut self) -> u64 {
        self.next_node_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}
