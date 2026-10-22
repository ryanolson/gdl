use std::collections::{BTreeMap, BTreeSet};

use super::{Placement, PlacementError, check_class_name};

/// Each segment's class, checking that a placement with classes puts every
/// segment in exactly one. A placement without classes has none to check.
pub(super) fn classify(
    placement: &Placement,
    errors: &mut Vec<PlacementError>,
) -> BTreeMap<String, String> {
    let mut class_of = BTreeMap::new();
    if placement.classes.is_empty() {
        return class_of;
    }
    let mut where_classed: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (class, segments) in placement.classes() {
        // A misnamed class still classifies its segments, so that each of
        // them is not also reported as in no class.
        if let Err(reason) = check_class_name(class) {
            errors.push(PlacementError::InvalidClassName {
                class: class.to_string(),
                reason,
            });
        }
        if segments.is_empty() {
            errors.push(PlacementError::EmptyClass {
                class: class.to_string(),
            });
        }
        let mut listed = BTreeSet::new();
        for segment in segments {
            if !listed.insert(segment.as_str()) {
                errors.push(PlacementError::SegmentRepeated {
                    class: class.to_string(),
                    segment: segment.clone(),
                });
            } else if !placement.segments.contains_key(segment) {
                errors.push(PlacementError::UnknownSegment {
                    class: class.to_string(),
                    segment: segment.clone(),
                });
            } else {
                where_classed.entry(segment).or_default().push(class);
            }
        }
    }
    for (segment, _) in placement.segments() {
        match where_classed.get(segment).map(Vec::as_slice) {
            None | Some([]) => errors.push(PlacementError::Unclassed {
                segment: segment.to_string(),
            }),
            Some([class]) => {
                class_of.insert(segment.to_string(), class.to_string());
            }
            Some(classes) => errors.push(PlacementError::ClassedTwice {
                segment: segment.to_string(),
                classes: classes.iter().map(ToString::to_string).collect(),
            }),
        }
    }
    class_of
}
