//! Space carried into each selector call, including callers without join order.

#[test]
fn a_selection_without_join_order_keeps_a_compact_view() {
    use gdl::__runtime::Erased;
    use std::mem::size_of;
    use std::sync::Arc;

    // A table can own the stamps and epoch behind one optional reference.
    // A caller without stamps then carries only that reference as overhead.
    let budget = size_of::<&str>() + size_of::<&[gdl::InstanceId]>() + size_of::<Option<&()>>();
    let carried = size_of::<gdl::Instances<'_>>();
    let selector = gdl::__runtime::Typed(Arc::new(gdl::Affinity::default()));
    let cx = gdl::Cx::new();
    let ids = [gdl::InstanceId::from_u128(1)];
    let choosing = selector.select(
        &cx,
        b"head",
        Default::default(),
        &[],
        gdl::Instances::new("worker", &ids),
    );
    println!(
        "Instances: {carried} bytes; erased affinity future without stamps: {} bytes",
        std::mem::size_of_val(choosing.as_ref().get_ref())
    );
    assert!(
        carried <= budget,
        "each selection carries {carried} bytes; ids, segment, and one optional reference need {budget}"
    );
}
