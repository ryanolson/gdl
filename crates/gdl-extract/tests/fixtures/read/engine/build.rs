// The engine needs a toolkit this machine does not have: building it fails.
// Reading it must not build it.
fn main() {
    panic!("read-engine needs CUDA");
}
