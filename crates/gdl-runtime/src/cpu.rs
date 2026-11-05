//! The start check of a segment binary built with CPU features
//! (`[deployment.codegen.<arch>]` in the deployment spec): a CPU that
//! lacks one of them would end the process with SIGILL at the first
//! instruction that needs it, with no word of why. So the generated `main`
//! calls [`check_cpu_features`] first, before any code compiled with the
//! features runs, and the process ends with one line that names the
//! feature instead.
//!
//! The check reads the features the kernel lists in `/proc/cpuinfo`, not
//! `std::arch::is_aarch64_feature_detected!` or `is_x86_feature_detected!`:
//! each of those is `true` without looking at the CPU when the feature is
//! enabled at compile time, so in a `+lse` build the check would be
//! compiled away. It covers each feature of the table below that the
//! binary was compiled with (`cfg!(target_feature = ...)`), so also those
//! a `target_cpu` turns on. A feature the table does not name has no
//! start check. The code here holds no atomic operation of its own: what
//! runs before the check must not need the features it checks.

use std::process::ExitCode;

/// Check that every CPU of this host has each feature of the table that
/// the binary was compiled with. When one is missing, or the kernel's
/// list cannot be read, print one line on stderr that says so, and return
/// the exit code `main` should end with. The binary of segment `segment`
/// calls it first in `main`.
pub fn check_cpu_features(segment: &str) -> Result<(), ExitCode> {
    let compiled: Vec<&Feature> = TABLE.iter().filter(|feature| feature.compiled).collect();
    if compiled.is_empty() {
        return Ok(());
    }
    // Not `is_aarch64_feature_detected!` or `is_x86_feature_detected!`:
    // under the target feature each folds to the constant `true`, and this
    // check would be compiled away.
    let refused = match cpuinfo() {
        Err(error) => Some(format!(
            "segment `{segment}`: this binary checks the CPU features it was built for (`[deployment.codegen.{KEY}]` in the deployment spec) before it starts, and it cannot read {CPUINFO}: {error}"
        )),
        Ok(text) => match missing(&compiled, &text) {
            Ok(None) => None,
            Ok(Some(feature)) => Some(refusal(segment, feature)),
            Err(why) => Some(format!(
                "segment `{segment}`: this binary checks the CPU features it was built for (`[deployment.codegen.{KEY}]` in the deployment spec) before it starts, and {CPUINFO} {why}"
            )),
        },
    };
    match refused {
        None => Ok(()),
        Some(line) => {
            eprintln!("{line}");
            Err(ExitCode::FAILURE)
        }
    }
}

/// The line that ends a binary on a CPU without `feature`.
fn refusal(segment: &str, feature: &str) -> String {
    format!(
        "segment `{segment}`: this binary was built for CPU feature `{feature}`, which this {ARCH} CPU does not have; build its image without it (`[deployment.codegen.{KEY}]` in the deployment spec), or run it on nodes whose CPUs have it"
    )
}

/// The kernel's list of each CPU's features.
const CPUINFO: &str = "/proc/cpuinfo";

/// A feature as rustc names it, whether the binary was compiled with it,
/// and the names `/proc/cpuinfo` gives what it needs: all of them.
struct Feature {
    name: &'static str,
    compiled: bool,
    cpuinfo: &'static [&'static str],
}

/// The table, from `name => [cpuinfo names]` pairs.
macro_rules! table {
    ($($name:tt => [$($cpuinfo:tt),+]),* $(,)?) => {
        &[$(Feature {
            name: $name,
            compiled: cfg!(target_feature = $name),
            cpuinfo: &[$($cpuinfo),+],
        }),*]
    };
}

/// The CPU architecture as Rust names it; the `[deployment.codegen.<arch>]`
/// key, which is Docker's name; the label of each CPU's features in
/// `/proc/cpuinfo`; and the features the check covers. On arm64 the
/// kernel prints the features from the CPU's HWCAP bits.
#[cfg(target_arch = "aarch64")]
const ARCH: &str = "aarch64";
#[cfg(target_arch = "aarch64")]
const KEY: &str = "arm64";
#[cfg(target_arch = "aarch64")]
const LABEL: &str = "Features";
#[cfg(target_arch = "aarch64")]
const TABLE: &[Feature] = table![
    "neon" => ["fp", "asimd"],
    "aes" => ["aes", "pmull"],
    "bf16" => ["bf16"],
    "bti" => ["bti"],
    "crc" => ["crc32"],
    "dit" => ["dit"],
    "dotprod" => ["asimddp"],
    "dpb" => ["dcpop"],
    "dpb2" => ["dcpodp"],
    "f32mm" => ["svef32mm"],
    "f64mm" => ["svef64mm"],
    "fcma" => ["fcma"],
    "fhm" => ["asimdfhm"],
    "flagm" => ["flagm"],
    "fp16" => ["fphp", "asimdhp"],
    "frintts" => ["frint"],
    "i8mm" => ["i8mm"],
    "jsconv" => ["jscvt"],
    "lse" => ["atomics"],
    "lse2" => ["uscat"],
    "mte" => ["mte"],
    "paca" => ["paca"],
    "pacg" => ["pacg"],
    "rand" => ["rng"],
    "rcpc" => ["lrcpc"],
    "rcpc2" => ["ilrcpc"],
    "rdm" => ["asimdrdm"],
    "sb" => ["sb"],
    "sha2" => ["sha1", "sha2"],
    "sha3" => ["sha3", "sha512"],
    "sm4" => ["sm3", "sm4"],
    // Not `ssbs`: a kernel can hide SSBS on cores that have it, and the
    // compiler emits no SSBS instruction of its own, so a check would
    // refuse a `target_cpu` build on the very CPU it names.
    "sve" => ["sve"],
    "sve2" => ["sve2"],
    "sve2-aes" => ["sveaes", "svepmull"],
    "sve2-bitperm" => ["svebitperm"],
    "sve2-sha3" => ["svesha3"],
    "sve2-sm4" => ["svesm4"],
];

#[cfg(target_arch = "x86_64")]
const ARCH: &str = "x86_64";
#[cfg(target_arch = "x86_64")]
const KEY: &str = "amd64";
#[cfg(target_arch = "x86_64")]
const LABEL: &str = "flags";
#[cfg(target_arch = "x86_64")]
const TABLE: &[Feature] = table![
    "sse" => ["sse"],
    "sse2" => ["sse2"],
    "fxsr" => ["fxsr"],
    "adx" => ["adx"],
    "aes" => ["aes"],
    "avx" => ["avx"],
    "avx2" => ["avx2"],
    "avx512bf16" => ["avx512_bf16"],
    "avx512bitalg" => ["avx512_bitalg"],
    "avx512bw" => ["avx512bw"],
    "avx512cd" => ["avx512cd"],
    "avx512dq" => ["avx512dq"],
    "avx512f" => ["avx512f"],
    "avx512fp16" => ["avx512_fp16"],
    "avx512ifma" => ["avx512ifma"],
    "avx512vbmi" => ["avx512vbmi"],
    "avx512vbmi2" => ["avx512_vbmi2"],
    "avx512vl" => ["avx512vl"],
    "avx512vnni" => ["avx512_vnni"],
    "avx512vp2intersect" => ["avx512_vp2intersect"],
    "avx512vpopcntdq" => ["avx512_vpopcntdq"],
    "bmi1" => ["bmi1"],
    "bmi2" => ["bmi2"],
    "cmpxchg16b" => ["cx16"],
    "f16c" => ["f16c"],
    "fma" => ["fma"],
    "gfni" => ["gfni"],
    "lzcnt" => ["abm"],
    "movbe" => ["movbe"],
    "pclmulqdq" => ["pclmulqdq"],
    "popcnt" => ["popcnt"],
    "rdrand" => ["rdrand"],
    "rdseed" => ["rdseed"],
    "rtm" => ["rtm"],
    "sha" => ["sha_ni"],
    "sse3" => ["pni"],
    "sse4.1" => ["sse4_1"],
    "sse4.2" => ["sse4_2"],
    "sse4a" => ["sse4a"],
    "ssse3" => ["ssse3"],
    "tbm" => ["tbm"],
    "vaes" => ["vaes"],
    "vpclmulqdq" => ["vpclmulqdq"],
    "xsave" => ["xsave"],
    "xsavec" => ["xsavec"],
    "xsaveopt" => ["xsaveopt"],
    "xsaves" => ["xsaves"],
];

#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
const ARCH: &str = std::env::consts::ARCH;
#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
const KEY: &str = std::env::consts::ARCH;
#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
const LABEL: &str = "";
#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
const TABLE: &[Feature] = &[];

/// The first of `compiled` that some CPU listed in `text`, the kernel's
/// `/proc/cpuinfo`, lacks; `Err` with why when `text` lists no CPU's
/// features.
fn missing(compiled: &[&Feature], text: &str) -> Result<Option<&'static str>, String> {
    let lists: Vec<Vec<&str>> = text
        .lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(label, _)| label.trim() == LABEL)
        .map(|(_, names)| names.split_whitespace().collect())
        .collect();
    if lists.is_empty() {
        return Err(format!("lists no `{LABEL}` of any CPU"));
    }
    Ok(compiled
        .iter()
        .find(|feature| {
            lists
                .iter()
                .any(|list| feature.cpuinfo.iter().any(|name| !list.contains(name)))
        })
        .map(|feature| feature.name))
}

/// The kernel's list of the CPUs' features. The tests read a file of
/// their own instead, so that a refusal can be seen on any CPU; a
/// deployed binary has no such seam.
#[cfg(not(test))]
fn cpuinfo() -> std::io::Result<String> {
    std::fs::read_to_string(CPUINFO)
}

#[cfg(test)]
fn cpuinfo() -> std::io::Result<String> {
    match std::env::var_os(tests::CPUINFO_FILE) {
        Some(file) => std::fs::read_to_string(file),
        None => std::fs::read_to_string(CPUINFO),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The file the test process reads in place of `/proc/cpuinfo`.
    pub(super) const CPUINFO_FILE: &str = "GDL_TEST_CPUINFO";

    /// A feature every build of the architecture is compiled with, and a
    /// cpuinfo text of two CPUs that lists it, then one without it.
    #[cfg(target_arch = "aarch64")]
    const BASELINE: (&str, &str, &str) = (
        "neon",
        "processor\t: 0\nFeatures\t: fp asimd evtstrm atomics\n\nprocessor\t: 1\nFeatures\t: fp asimd evtstrm atomics\n",
        "processor\t: 0\nFeatures\t: fp asimd evtstrm atomics\n\nprocessor\t: 1\nFeatures\t: fp evtstrm atomics\n",
    );
    #[cfg(target_arch = "x86_64")]
    const BASELINE: (&str, &str, &str) = (
        "sse2",
        "processor\t: 0\nflags\t\t: fpu fxsr sse sse2 pni\n\nprocessor\t: 1\nflags\t\t: fpu fxsr sse sse2 pni\n",
        "processor\t: 0\nflags\t\t: fpu fxsr sse sse2 pni\n\nprocessor\t: 1\nflags\t\t: fpu fxsr sse pni\n",
    );

    fn feature(name: &'static str) -> &'static Feature {
        TABLE
            .iter()
            .find(|feature| feature.name == name)
            .expect("a feature of the table")
    }

    /// A compiled feature counts only when every CPU lists each name it
    /// needs: `lse` is `atomics`, and `sha2` needs `sha1` too. A text
    /// with no CPU's features is not taken for a CPU without any.
    #[cfg(target_arch = "aarch64")]
    #[test]
    fn a_feature_is_present_only_when_every_cpu_lists_it() {
        let lse = feature("lse");
        let sha2 = feature("sha2");
        let both =
            "Features\t: fp asimd atomics sha1 sha2\nFeatures\t: fp asimd atomics sha1 sha2\n";
        assert_eq!(missing(&[lse, sha2], both), Ok(None));
        let one_without =
            "Features\t: fp asimd atomics sha1 sha2\nFeatures\t: fp asimd sha1 sha2\n";
        assert_eq!(missing(&[lse, sha2], one_without), Ok(Some("lse")));
        assert_eq!(
            missing(&[lse, sha2], "Features\t: fp asimd atomics sha2\n"),
            Ok(Some("sha2"))
        );
        // `atomics` as a word, not inside another one.
        assert_eq!(
            missing(&[lse], "Features\t: fp asimd atomicsx\n"),
            Ok(Some("lse"))
        );
        assert!(missing(&[lse], "processor\t: 0\nBogoMIPS\t: 2000\n").is_err());
    }

    /// A binary built with `target_cpu = "neoverse-n1"` has the features
    /// rustc gives that CPU. It starts on a Neoverse N1 and on a
    /// Cortex-X925, by the features their kernels list, which name no
    /// `ssbs`: a kernel can hide SSBS on cores that have it. It is refused
    /// on a Cortex-A57.
    #[cfg(target_arch = "aarch64")]
    #[test]
    fn a_target_cpus_features_are_found_on_that_cpu() {
        // `rustc --print cfg -C target-cpu=neoverse-n1`, rustc 1.93.1.
        let n1 = [
            "aes", "crc", "dotprod", "dpb", "fp16", "lor", "lse", "neon", "pan", "pmuv3", "ras",
            "rcpc", "rdm", "sha2", "spe", "ssbs", "vh",
        ];
        let compiled: Vec<&Feature> = TABLE
            .iter()
            .filter(|feature| n1.contains(&feature.name))
            .collect();
        // QEMU 8.2's `-cpu neoverse-n1`, a Cortex-X925 host, and QEMU's
        // `-cpu cortex-a57`.
        let neoverse_n1 = "Features\t: fp asimd aes pmull sha1 sha2 crc32 atomics fphp asimdhp cpuid asimdrdm lrcpc dcpop asimddp\n";
        let x925 = "Features\t: fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics fphp asimdhp cpuid asimdrdm jscvt fcma lrcpc dcpop sha3 sm3 sm4 asimddp sha512 sve asimdfhm dit uscat ilrcpc flagm sb paca pacg dcpodp sve2 sveaes svepmull svebitperm svesha3 svesm4 flagm2 frint svei8mm svebf16 i8mm bf16 dgh bti ecv afp wfxt\n";
        let a57 = "Features\t: fp asimd aes pmull sha1 sha2 crc32 cpuid\n";
        assert_eq!(missing(&compiled, neoverse_n1), Ok(None));
        assert_eq!(missing(&compiled, x925), Ok(None));
        assert!(matches!(missing(&compiled, a57), Ok(Some(_))));
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn a_feature_is_present_only_when_every_cpu_lists_it() {
        let lzcnt = feature("lzcnt");
        let sse41 = feature("sse4.1");
        let both = "flags\t\t: fpu sse4_1 abm\nflags\t\t: fpu sse4_1 abm\n";
        assert_eq!(missing(&[lzcnt, sse41], both), Ok(None));
        let one_without = "flags\t\t: fpu sse4_1 abm\nflags\t\t: fpu sse4_1\n";
        assert_eq!(missing(&[lzcnt, sse41], one_without), Ok(Some("lzcnt")));
        assert!(missing(&[lzcnt], "processor\t: 0\n").is_err());
    }

    /// What a generated `main` does first, as this test binary's own run
    /// of one test: a failure ends the process as a test that fails.
    #[test]
    #[ignore = "run by the tests below, in a process of its own"]
    fn main_starts() -> ExitCode {
        if let Err(code) = check_cpu_features("engine") {
            return code;
        }
        ExitCode::SUCCESS
    }

    /// Run [`main_starts`] in a process of its own, with `cpuinfo` as the
    /// kernel's list: the text of a file, or `None` for the real one.
    fn start(cpuinfo: Option<&str>) -> std::process::Output {
        let dir = tempfile::Builder::new()
            .prefix("gdl-cpuinfo-")
            .tempdir()
            .expect("a directory");
        let mut command =
            std::process::Command::new(std::env::current_exe().expect("this test binary"));
        command
            .args([
                "--exact",
                "cpu::tests::main_starts",
                "--ignored",
                "--nocapture",
            ])
            .env_remove(CPUINFO_FILE)
            .stdin(std::process::Stdio::null());
        if let Some(text) = cpuinfo {
            let file = dir.path().join("cpuinfo");
            std::fs::write(&file, text).expect("a cpuinfo");
            command.env(CPUINFO_FILE, &file);
        }
        command.output().expect("the test binary runs")
    }

    /// On a host where one CPU lacks a feature the binary was compiled
    /// with, the binary ends with a failure, and its one line names the
    /// feature, the architecture, and the spec's table.
    #[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
    #[test]
    fn a_missing_feature_ends_the_binary_with_one_line() {
        let (baseline, _, without) = BASELINE;
        let output = start(Some(without));
        assert!(!output.status.success(), "{output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        let line = refusal("engine", baseline);
        assert!(stderr.lines().any(|l| l == line), "{line} in {stderr}");
        for part in [
            format!("`{baseline}`"),
            format!("this {ARCH} CPU"),
            format!("`[deployment.codegen.{KEY}]`"),
        ] {
            assert!(line.contains(&part), "{part} in {line}");
        }
    }

    /// A list the binary cannot read, or one that names no CPU's
    /// features, ends it too: it never guesses.
    #[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
    #[test]
    fn an_unreadable_list_ends_the_binary() {
        let output = start(Some("processor\t: 0\n"));
        assert!(!output.status.success(), "{output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&format!("{CPUINFO} lists no `{LABEL}` of any CPU")),
            "{stderr}"
        );
    }

    /// The controls: a host whose every CPU lists the features starts,
    /// and so does this host, by its own `/proc/cpuinfo`.
    #[test]
    fn a_cpu_with_every_compiled_feature_starts() {
        #[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
        let listed = Some(BASELINE.1);
        #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
        let listed = None;
        for cpuinfo in [listed, None] {
            let output = start(cpuinfo);
            assert!(output.status.success(), "{cpuinfo:?}: {output:?}");
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(!stderr.contains("CPU feature"), "{cpuinfo:?}: {stderr}");
        }
    }
}
