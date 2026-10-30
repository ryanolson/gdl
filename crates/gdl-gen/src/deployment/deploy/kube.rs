//! [`Kube`] over the `kubectl` CLI, and the command lines `deploy` and
//! `down` give it. Every command line starts with `--context <context>`,
//! so that no command reaches a cluster through whatever context is
//! current. No command line names a kubeconfig, a token, a user, a key, or
//! a certificate, and none reads a Secret or kubectl's configuration: the
//! user's own kube contexts reach each cluster.

use std::io::Write as _;
use std::process::{Command, Stdio};

use crate::Error;

/// Runs `kubectl`. [`Kubectl`] runs the real one with the kube contexts
/// the user already has; tests give a fake that records each command line.
pub trait Kube {
    /// Run `kubectl` with `args`, and `input` on its stdin when given.
    /// An error is only a `kubectl` that could not run: a command that ran
    /// and failed is a [`Reply`] that did not succeed.
    fn run(&mut self, args: &[String], input: Option<&[u8]>) -> Result<Reply, Error>;
}

/// What one `kubectl` command answered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reply {
    /// Whether it exited with status 0.
    pub success: bool,
    /// What it printed on stdout.
    pub stdout: String,
    /// What it printed on stderr.
    pub stderr: String,
}

impl Reply {
    /// A command that succeeded and printed `stdout`.
    pub fn ok(stdout: impl Into<String>) -> Self {
        Self {
            success: true,
            stdout: stdout.into(),
            stderr: String::new(),
        }
    }

    /// A command that failed and printed `stderr`.
    pub fn failed(stderr: impl Into<String>) -> Self {
        Self {
            success: false,
            stdout: String::new(),
            stderr: stderr.into(),
        }
    }

    /// Whether the service that the API server proxied to answered 404
    /// itself. kubectl prints a proxied 404 with no body of its own as
    /// "the server could not find the requested resource". The API
    /// server's own 404 for a Service that does not exist names the
    /// Service instead, and proves nothing about what the Service serves.
    pub(super) fn proxied_not_found(&self) -> bool {
        !self.success
            && self
                .stderr
                .contains("(NotFound): the server could not find the requested resource")
    }

    /// Whether the API server answered that Service `service` does not
    /// exist.
    pub(super) fn no_service(&self, service: &str) -> bool {
        !self.success
            && self
                .stderr
                .contains(&format!("(NotFound): services \"{service}\" not found"))
    }
}

/// Runs the `kubectl` on the `PATH`.
#[derive(Debug, Default)]
pub struct Kubectl;

impl Kube for Kubectl {
    fn run(&mut self, args: &[String], input: Option<&[u8]>) -> Result<Reply, Error> {
        let running = |e: std::io::Error| Error::new(format!("running `kubectl`: {e}"));
        let mut child = Command::new("kubectl")
            .args(args)
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(running)?;
        // Written from a thread of its own: a command that prints while it
        // reads would otherwise block on a full pipe.
        let writer = match (input, child.stdin.take()) {
            (Some(input), Some(mut stdin)) => {
                let input = input.to_vec();
                Some(std::thread::spawn(move || stdin.write_all(&input)))
            }
            _ => None,
        };
        let output = child.wait_with_output().map_err(running)?;
        if let Some(writer) = writer {
            // A command that exits before it reads all its input breaks the
            // pipe; its own status says what went wrong.
            let _ = writer.join();
        }
        Ok(Reply {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

/// The field manager of every object `deploy` applies: server-side apply
/// records which fields it set, so that a later apply removes a field the
/// render no longer writes.
const MANAGER: &str = "--field-manager=gdl";

/// The kinds of namespaced object the render writes, as `kubectl get`
/// takes them: what `deploy` prunes and `down` deletes.
pub(super) const KINDS: &str =
    "deployments.apps,services,configmaps,persistentvolumeclaims,serviceaccounts";

/// The service port on which a hub serves its public routes, and its
/// control routes, as the render names them.
pub(super) const PUBLIC: &str = "public";
pub(super) const CONTROL: &str = "control";

/// One cluster's namespace, reached through its kube context: the command
/// lines that `deploy` and `down` run there.
#[derive(Debug, Clone, Copy)]
pub(super) struct At<'a> {
    pub(super) context: &'a str,
    pub(super) namespace: &'a str,
}

impl At<'_> {
    /// `rest` after the context.
    fn cluster(&self, rest: &[&str]) -> Vec<String> {
        let mut args = vec!["--context".to_string(), self.context.to_string()];
        args.extend(rest.iter().map(|arg| arg.to_string()));
        args
    }

    /// `rest` after the context and the namespace.
    fn namespaced(&self, rest: &[&str]) -> Vec<String> {
        let mut args = self.cluster(&["--namespace", self.namespace]);
        args.extend(rest.iter().map(|arg| arg.to_string()));
        args
    }

    /// Whether the context may create Deployments in the namespace:
    /// `kubectl auth can-i` prints `yes` or `no`.
    pub(super) fn can_i(&self) -> Vec<String> {
        self.namespaced(&["auth", "can-i", "create", "deployments.apps"])
    }

    /// Apply the objects on stdin, server-side, and print them as applied,
    /// as JSON: one object, or a list of them.
    pub(super) fn apply(&self) -> Vec<String> {
        self.cluster(&[
            "apply",
            "--server-side",
            MANAGER,
            "-f",
            "-",
            "--output",
            "json",
        ])
    }

    /// Wait, at most `seconds`, until the namespace is active.
    pub(super) fn namespace_active(&self, seconds: u64) -> Vec<String> {
        let namespace = format!("namespace/{}", self.namespace);
        let timeout = format!("--timeout={seconds}s");
        self.cluster(&[
            "wait",
            "--for=jsonpath={.status.phase}=Active",
            &namespace,
            &timeout,
        ])
    }

    /// The namespace's objects of [`KINDS`] that `selector` selects, as a
    /// JSON list.
    pub(super) fn list(&self, selector: &str) -> Vec<String> {
        self.namespaced(&["get", KINDS, "--selector", selector, "--output", "json"])
    }

    /// The namespace's pods that `selector` selects, as a JSON list.
    pub(super) fn pods(&self, selector: &str) -> Vec<String> {
        self.namespaced(&["get", "pods", "--selector", selector, "--output", "json"])
    }

    /// The events of the namespace's object `name`, as a JSON list.
    pub(super) fn events(&self, name: &str) -> Vec<String> {
        let field = format!("involvedObject.name={name}");
        self.namespaced(&[
            "get",
            "events",
            "--field-selector",
            &field,
            "--output",
            "json",
        ])
    }

    /// The path of `route` of Service `service`'s port `port`, through the
    /// API server's service proxy.
    fn proxy(&self, service: &str, port: &str, route: &str) -> String {
        format!(
            "/api/v1/namespaces/{}/services/http:{service}:{port}/proxy{route}",
            self.namespace
        )
    }

    /// `GET` `route` of `service`'s `port`, through the service proxy.
    pub(super) fn get(&self, service: &str, port: &str, route: &str) -> Vec<String> {
        self.cluster(&["get", "--raw", &self.proxy(service, port, route)])
    }

    /// `POST` stdin to `route` of `service`'s `port`, through the service
    /// proxy.
    pub(super) fn post(&self, service: &str, port: &str, route: &str) -> Vec<String> {
        self.cluster(&[
            "create",
            "--raw",
            &self.proxy(service, port, route),
            "-f",
            "-",
        ])
    }

    /// Delete `objects`, each `<kind>/<name>`, and wait, at most `seconds`,
    /// until they and what they own are gone: a Deployment's pods first.
    pub(super) fn delete(&self, objects: &[String], seconds: u64) -> Vec<String> {
        let timeout = format!("--timeout={seconds}s");
        let mut args = self.namespaced(&[
            "delete",
            "--cascade=foreground",
            "--wait=true",
            "--ignore-not-found",
            &timeout,
        ]);
        args.extend(objects.iter().cloned());
        args
    }

    /// Remove the labels `keys` from the namespace.
    pub(super) fn unlabel(&self, keys: &[&str]) -> Vec<String> {
        let namespace = format!("namespace/{}", self.namespace);
        let mut args = self.cluster(&["label", &namespace]);
        args.extend(keys.iter().map(|key| format!("{key}-")));
        args
    }
}
