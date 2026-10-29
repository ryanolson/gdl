//! The pool policy a chooser picks a class's pools by, by the name the
//! placement gives it (`[pools.<class>] policy`): one a deployment
//! registers ([`gdl::PoolPolicies`]), or gdl's `home_first` or
//! `round_robin`.

use std::sync::Arc;

use gdl::__runtime::ErasedPolicy;
use gdl::{Error, ErrorKind, HomeFirstAt, PoolPolicies, Result};
use gdl_model::{Layout, PoolPolicy};

/// The pool policy the placement names for `class`, with its name, from
/// `policies` or gdl's own, `home_first` as configured; refused, naming
/// the policy and the class, when neither has it.
pub(crate) fn of(
    layout: &Layout,
    class: &str,
    policies: &PoolPolicies,
    home_first: HomeFirstAt,
) -> Result<(Arc<str>, Arc<dyn ErasedPolicy>)> {
    let default;
    let policy = match layout.pool_policy(class) {
        Some(policy) => policy,
        None => {
            default = PoolPolicy::default();
            &default
        }
    };
    let name = policy.policy.as_str();
    match policies.resolve(name, home_first) {
        Some(found) => Ok((Arc::from(name), found)),
        None => {
            let known: Vec<String> = policies.names().map(|name| format!("`{name}`")).collect();
            Err(Error::new(
                ErrorKind::Placement,
                format!(
                    "`[pools.{class}] policy = \"{name}\"`, but nothing registers a pool policy of that name and gdl has none; the policies are {}",
                    known.join(", ")
                ),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use gdl::{Encoded, Pool, PoolRequest};

    use super::*;

    /// Takes the last pool, whatever it is.
    struct Last;

    impl gdl::PoolPolicy for Last {
        type Head = Encoded;

        fn choose(
            &self,
            _: PoolRequest<'_, Encoded>,
            pools: &[Pool],
        ) -> Result<gdl::PoolChoice, gdl::NoPool> {
            pools
                .len()
                .checked_sub(1)
                .map(gdl::PoolChoice::new)
                .ok_or_else(gdl::NoPool::new)
        }
    }

    fn layout_under(policy: Option<&str>) -> Layout {
        let manifest = gdl_model::Manifest {
            name: "pools".into(),
            schema: 1,
            nodes: vec![
                gdl_model::NodeInfo {
                    name: "S".into(),
                    role: gdl_model::Role::Source,
                    binding: None,
                },
                gdl_model::NodeInfo {
                    name: "D".into(),
                    role: gdl_model::Role::Sink,
                    binding: Some("d".into()),
                },
            ],
            hops: vec![gdl_model::HopInfo {
                outer: "S".into(),
                forward: vec![],
                inner: "D".into(),
                backward: vec![],
                forward_types: vec![gdl_model::TypeInfo::new("T", true, Default::default())],
                backward_types: vec![gdl_model::TypeInfo::new("T", true, Default::default())],
            }],
        };
        let mut placement = gdl_model::Placement::new()
            .segment("a", ["S"])
            .segment("d", ["D"])
            .class("global", ["a"])
            .class("model_x", ["d"]);
        if let Some(policy) = policy {
            placement = placement.pool_policy(
                "model_x",
                PoolPolicy {
                    policy: policy.into(),
                },
            );
        }
        gdl_model::layout(&manifest, &placement).expect("valid test fixture")
    }

    /// gdl's own policies need no registration; any other name must be
    /// registered, or the chooser is refused, naming the policy and the
    /// class.
    #[test]
    fn a_placement_names_a_policy_that_is_registered_or_gdls_own() {
        let none = PoolPolicies::new();
        for (policy, name) in [
            (None, "round_robin"),
            (Some("round_robin"), "round_robin"),
            (Some("home_first"), "home_first"),
        ] {
            let (found, _) = of(
                &layout_under(policy),
                "model_x",
                &none,
                HomeFirstAt::DEFAULT,
            )
            .expect("gdl's own");
            assert_eq!(&*found, name);
        }
        let layout = layout_under(Some("last"));
        let Err(error) = of(&layout, "model_x", &none, HomeFirstAt::DEFAULT) else {
            panic!("a policy nothing registers was taken");
        };
        let message = error.to_string();
        assert!(
            message.contains("`last`") || message.contains("\"last\""),
            "{message}"
        );
        assert!(message.contains("model_x"), "{message}");
        assert_eq!(error.kind(), ErrorKind::Placement);
        let mut policies = PoolPolicies::new();
        policies.add("last", Last).expect("registered");
        let (found, policy) =
            of(&layout, "model_x", &policies, HomeFirstAt::DEFAULT).expect("registered");
        assert_eq!(&*found, "last");
        let pools = [
            Pool::new("model_x", "m1", "r", gdl::PoolState::Ready).with_entries(1),
            Pool::new("model_x", "m2", "r", gdl::PoolState::Ready).with_entries(1),
        ];
        let cx = gdl::Cx::new();
        let request = PoolRequest::new(&cx, &(), "r", tokio::time::Instant::now());
        assert_eq!(
            policy
                .choose(request, Default::default(), &pools)
                .expect("no head to read")
                .map(|choice| choice.at()),
            Ok(1)
        );
    }
}
