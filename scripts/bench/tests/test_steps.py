"""Check attribution of work called by an item-path step."""

import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location(
    "items", Path(__file__).resolve().parents[1] / "items.py"
)
items = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(items)


class StepAttribution(unittest.TestCase):
    def test_scheduler_work_stays_in_the_scheduler(self):
        frames = [
            "park @ /cargo/registry/src/tokio-1.52.3/src/runtime/park.rs:90",
            "run @ /cargo/registry/src/tokio-1.52.3/src/runtime/scheduler/multi_thread/worker.rs:600",
        ]
        self.assertEqual(items.classify(frames), "tokio scheduler, park and wake")

    def test_main_thread_parking_stays_in_the_scheduler(self):
        frames = [
            "futex_wait",
            "park @ /cargo/registry/src/tokio-1.52.3/src/runtime/park.rs:111",
            "block_on @ /cargo/registry/src/tokio-1.52.3/src/runtime/park.rs:288",
            "main @ /checkout/crates/gdl-runtime/benches/requests.rs:54",
        ]
        self.assertEqual(items.classify(frames), "tokio scheduler, park and wake")

    def test_socket_work_belongs_to_the_transport(self):
        """Tokio socket writes are transport work, including their syscalls."""
        frames = [
            "__libc_write",
            "poll_write @ /cargo/registry/src/tokio-1.52.3/src/net/tcp/stream.rs:1494",
            "run_coalescing_writer @ /cargo/git/checkouts/velo-hash/cee2834/lib/velo/src/transports/coalesce/mod.rs:579",
            "egress_pump @ /cargo/git/checkouts/velo-hash/cee2834/lib/velo/src/streaming/tcp_transport.rs:390",
        ]
        self.assertEqual(items.classify(frames), "velo transport")

    def test_payload_serialization_belongs_to_the_codec(self):
        """A derived payload serializer is codec work, not blob construction."""
        frames = [
            "serialize @ /checkout/crates/gdl-runtime/benches/remote/setup.rs:33",
            "write_named @ /cargo/registry/src/rmp-serde-1.3.0/src/encode.rs:1220",
            "encode_msgpack @ /checkout/crates/gdl/src/codec.rs:26",
        ]
        self.assertEqual(items.classify(frames), "edge encode (gdl codec)")


if __name__ == "__main__":
    unittest.main()
