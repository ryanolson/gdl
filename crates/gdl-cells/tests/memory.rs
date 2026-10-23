//! The `memory` backend passes the directory conformance suite, with
//! small bounds and with its defaults.

use gdl_cells::{
    MemoryDirectory, MemoryLimits,
    conformance::{self, Backend},
};

struct Memory(MemoryLimits);

impl Backend for Memory {
    type Directory = MemoryDirectory;

    async fn fresh<const N: usize>(&self) -> [MemoryDirectory; N] {
        let directory = MemoryDirectory::with_limits(self.0);
        std::array::from_fn(|_| directory.clone())
    }

    fn buffer(&self) -> usize {
        self.0.buffer
    }

    fn history(&self) -> usize {
        self.0.history
    }

    fn watchers(&self, directory: &MemoryDirectory, class: &str) -> usize {
        directory.watchers(class)
    }
}

#[tokio::test]
async fn the_memory_directory_conforms_with_small_bounds() {
    conformance::run(&Memory(MemoryLimits {
        buffer: 8,
        history: 5,
    }))
    .await;
}

#[tokio::test]
async fn the_memory_directory_conforms_with_its_defaults() {
    conformance::run(&Memory(MemoryLimits::default())).await;
}
