//! Dropped ancestors do not make destruction of a surviving context recurse.

#[test]
fn dropping_the_last_context_after_many_discarded_ancestors_does_not_overflow() {
    const CHILD: &str = "GDL_REVIEW_DESCEND_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let worker = std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                let mut cx = gdl::Cx::new();
                for _ in 0..100_000 {
                    cx = cx.child();
                }
                assert!(!cx.is_stopping());
                drop(cx);
            })
            .expect("worker");
        worker.join().expect("context creation and drop");
        return;
    }

    // An overflow aborts the process, so isolate it to keep an assertion failure.
    let result = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "dropping_the_last_context_after_many_discarded_ancestors_does_not_overflow",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .expect("isolated worker process");
    assert!(
        result.status.success(),
        "context destruction failed: {}\n{}",
        result.status,
        String::from_utf8_lossy(&result.stderr),
    );
}
