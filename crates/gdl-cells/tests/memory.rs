//! The `memory` backend passes the directory conformance suite, with
//! small bounds and with its defaults. It keeps every rule but
//! [`Rule::Kept`]: it deletes nothing by time, but its records live only
//! as long as its process.

use gdl_cells::{
    MemoryDirectory, MemoryLimits,
    conformance::{self, Backend, Rule},
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

    // A memory directory lives as long as its process.
    async fn restart(&self, _: MemoryDirectory) -> MemoryDirectory {
        MemoryDirectory::with_limits(self.0)
    }

    fn keeps(&self, rule: Rule) -> bool {
        rule != Rule::Kept
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

/// The smallest buffer the suite takes conforms: the suite makes that
/// many changes before a watcher reads them.
#[tokio::test(start_paused = true)]
async fn the_smallest_buffer_the_suite_takes_conforms() {
    conformance::run(&Memory(MemoryLimits {
        buffer: conformance::MIN_BUFFER,
        history: 5,
    }))
    .await;
}

/// A smaller buffer is refused by name, rather than failing cases that
/// queue more changes than it holds.
#[tokio::test]
#[should_panic(expected = "so it takes no buffer under 3, not 2")]
async fn a_buffer_the_suite_cannot_test_is_refused() {
    conformance::run(&Memory(MemoryLimits {
        buffer: 2,
        history: 5,
    }))
    .await;
}
