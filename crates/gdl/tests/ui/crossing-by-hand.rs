// A segment asserts `Crossing` and `Anchored` of each message that leaves
// it, so a type that is not a wire type cannot claim them by hand.
struct NotWire;

impl gdl::__private::Crossing for NotWire {}

impl gdl::__private::Anchored for NotWire {}

fn main() {}
