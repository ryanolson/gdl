use std::collections::BTreeMap;

use crate::hash::Fnv;

use super::PropertyInfo;

/// Hash the declared property kinds after the hops, in kind order, each
/// with its type, encoding and version, and its providers and needs in
/// name order: what a declaration means does not depend on the order it
/// is written in. The marker starts them where nothing else of the
/// manifest can follow the hops. A graph that declares none writes
/// nothing, and keeps the hash it had before properties were declared.
pub(super) fn hash(h: &mut Fnv, properties: &[PropertyInfo]) {
    if properties.is_empty() {
        return;
    }
    let by_kind: BTreeMap<&str, &PropertyInfo> = properties
        .iter()
        .map(|property| (property.kind.as_str(), property))
        .collect();
    h.u64(u64::MAX);
    h.u64(by_kind.len() as u64);
    for (kind, property) in by_kind {
        h.str(kind);
        h.str(&property.ty.name);
        h.str(property.ty.encoding.name());
        h.u64(u64::from(property.ty.encoding.version()));
        let mut providers: Vec<&str> = property.providers.iter().map(String::as_str).collect();
        providers.sort_unstable();
        h.strs(&providers);
        let mut needs: Vec<_> = property.needs.iter().collect();
        needs.sort_unstable();
        h.u64(needs.len() as u64);
        for need in needs {
            h.str(&need.node);
            h.str(&need.class);
        }
    }
}
