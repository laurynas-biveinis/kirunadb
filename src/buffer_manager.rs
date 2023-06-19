// Copyright (C) 2022-2023 Laurynas Biveinis

use crate::node;

#[derive(Debug)] // COV_EXCL_LINE
pub struct BufferManager {
    next_node_id: node::AtomicId,
}

impl BufferManager {
    #[must_use]
    pub fn new(first_free_node_id: node::Id) -> Self {
        Self {
            next_node_id: node::AtomicId::new(first_free_node_id),
        }
    }

    #[inline]
    pub fn allocate_new_node_id(&mut self) -> node::Id {
        self.next_node_id.get_and_advance()
    }
}

#[cfg(test)]
mod tests {
    use super::BufferManager;

    #[test]
    fn node_id_sequence() {
        let mut buffer_manager = BufferManager::new(crate::node::Id::from(14));
        assert_eq!(14, buffer_manager.allocate_new_node_id().as_u64());
        assert_eq!(15, buffer_manager.allocate_new_node_id().as_u64());
    }
}
