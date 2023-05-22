// Copyright (C) 2022-2023 Laurynas Biveinis
use std::sync::atomic::AtomicU64;

// COV_EXCL_START
#[derive(Debug)]
// COV_EXCL_STOP
pub struct BufferManager {
    next_node_id: AtomicU64,
}

impl BufferManager {
    pub const NULL_NODE_ID: u64 = 0;

    pub fn new(first_free_node_id: u64) -> BufferManager {
        BufferManager {
            next_node_id: AtomicU64::new(first_free_node_id),
        }
    }

    #[inline]
    pub fn allocate_new_node_id(&mut self) -> u64 {
        self.next_node_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::BufferManager;

    #[test]
    fn node_id_sequence() {
        let mut buffer_manager = BufferManager::new(14);
        assert_eq!(14, buffer_manager.allocate_new_node_id());
        assert_eq!(15, buffer_manager.allocate_new_node_id());
    }
}
