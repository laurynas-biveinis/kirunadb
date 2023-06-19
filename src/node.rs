// Copyright (C) 2023 Laurynas Biveinis
#![deny(clippy::pedantic)]

use std::{
    fmt::{self, Display},
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct Id(u64);

impl Id {
    const NULL: Self = Self(0);

    #[inline]
    pub fn next(self) -> Self {
        debug_assert_ne!(self.0, u64::MAX);
        Self::from(self.0 + 1)
    }

    #[must_use]
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    #[must_use]
    #[inline]
    pub fn to_ne_bytes(self) -> [u8; 8] {
        self.0.to_ne_bytes()
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Id {
    #[inline]
    fn from(val: u64) -> Self {
        Self(val)
    }
}

#[derive(Debug)]
#[must_use]
pub struct AtomicId(AtomicU64);

impl AtomicId {
    #[inline]
    pub fn new(id: Id) -> Self {
        debug_assert_ne!(id, Id::NULL);
        Self(AtomicU64::new(id.as_u64()))
    }

    #[inline]
    pub fn get_and_advance(&mut self) -> Id {
        let result_u64 = self.0.fetch_add(1, Ordering::Relaxed);
        let result = Id(result_u64);
        debug_assert_ne!(result, Id::NULL);
        result
    }
}
