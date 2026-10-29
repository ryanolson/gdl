//! The relay refusal also applies when a program builds its extension directly.

mod common;

use common::{settings, spec, timing};
use gdl_hub::{HubDirectory, PoolReadiness};
use gdl_model::Parent;
use gdl_pools::MemoryDirectory;

#[test]
fn a_pool_with_one_parent_can_enable_reports() {
    let directory =
        HubDirectory::new(Some("global-hub:1337".into()), None).expect("a relay directory");
    let readiness = PoolReadiness::new(&spec(), settings(), directory.clone(), timing())
        .expect("a pool with one parent");
    readiness
        .with_reports(&directory)
        .expect("the relay serves a pool with one parent");
}

#[test]
fn a_shared_pool_accepts_a_directory_its_parents_share() {
    let pool = gdl_model::PoolSettings {
        parent: Some(Parent::Class("global".into())),
        ..settings()
    };
    PoolReadiness::new(&spec(), pool, MemoryDirectory::new(), timing())
        .expect("a shared pool with a shared directory");
}

#[test]
fn a_shared_pool_refuses_reports_enabled_in_code() {
    let directory =
        HubDirectory::new(Some("global-hub:1337".into()), None).expect("a relay directory");
    let pool = gdl_model::PoolSettings {
        parent: Some(Parent::Class("global".into())),
        ..settings()
    };
    let readiness = PoolReadiness::new(&spec(), pool, directory.clone(), timing())
        .expect("a shared pool with a directory supplied by its program");
    let Err(error) = readiness.with_reports(&directory) else {
        panic!("with_reports accepted a shared pool under relay");
    };
    assert!(error.to_string().contains("[pool] parents"), "{error}");
}
