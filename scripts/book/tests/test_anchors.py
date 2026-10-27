"""Check which links the anchor check reports."""

import importlib.util
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "anchors", Path(__file__).resolve().parents[1] / "anchors.py"
)
anchors = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(anchors)


def page(ids=(), links=()):
    headings = "".join(f'<h2 id="{i}">{i}</h2>' for i in ids)
    hrefs = "".join(f'<a href="{href}">link</a>' for href in links)
    return f"<!DOCTYPE html><html><body>{headings}{hrefs}</body></html>"


class Anchors(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="gdl-anchors-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def test_a_link_to_a_missing_heading_is_reported(self):
        self.write("concepts/placement.html", page(ids=["segments"]))
        self.write(
            "guides/start.html",
            page(links=["../concepts/placement.html#this-section-does-not-exist"]),
        )
        self.assertEqual(
            anchors.broken(self.root),
            ["guides/start.html: ../concepts/placement.html#this-section-does-not-exist"],
        )

    def test_a_link_to_a_heading_on_its_own_page_is_checked(self):
        self.write("a.html", page(ids=["here"], links=["#here", "#gone"]))
        self.assertEqual(anchors.broken(self.root), ["a.html: #gone"])

    def test_links_that_resolve_pass(self):
        self.write("a.html", page(ids=["here", "café"]))
        self.write(
            "sub/b.html",
            page(
                links=[
                    "../a.html#here",
                    "../sub/../a.html#here",
                    "../a.html#caf%C3%A9",
                    "../a.html",
                    "#",
                    "https://example.com/a.html#gone",
                    "mailto:someone@example.com",
                ]
            ),
        )
        self.assertEqual(anchors.broken(self.root), [])

    def test_a_named_anchor_is_a_target(self):
        self.write("a.html", '<a name="old"></a>' + page(links=["#old"]))
        self.assertEqual(anchors.broken(self.root), [])

    def test_a_link_with_a_heading_to_a_missing_page_is_reported(self):
        self.write("a.html", page(links=["gone.html#here"]))
        self.assertEqual(anchors.broken(self.root), ["a.html: gone.html#here"])

    def test_the_print_page_is_not_read_for_links(self):
        # It joins every chapter, so its links are the chapters' own.
        self.write("print.html", page(links=["#gone"]))
        self.write("guide/print.html", page(links=["#gone"]))
        self.assertEqual(anchors.broken(self.root), ["guide/print.html: #gone"])

    def test_the_command_fails_on_a_broken_link(self):
        self.write("a.html", page(links=["#gone"]))
        self.assertEqual(anchors.main(["anchors.py", str(self.root)]), 1)
        self.write("a.html", page(ids=["gone"], links=["#gone"]))
        self.assertEqual(anchors.main(["anchors.py", str(self.root)]), 0)


    def test_a_directory_link_checks_the_index_page(self):
        """F1: Directory URLs skip fragment validation."""
        self.write("guide/index.html", page(ids=["here"]))
        self.write("a.html", page(links=["guide/#gone"]))
        self.assertEqual(anchors.broken(self.root), ["a.html: guide/#gone"])

    def test_a_directory_link_with_an_existing_heading_passes(self):
        self.write("guide/index.html", page(ids=["here"]))
        self.write("a.html", page(links=["guide/#here"]))
        self.assertEqual(anchors.broken(self.root), [])


    def test_a_root_relative_link_uses_the_book_root(self):
        """F2: Root-relative URL paths become absolute filesystem paths."""
        self.write("a.html", page(ids=["here"]))
        self.write("guide/start.html", page(links=["/a.html#here"]))
        self.assertEqual(anchors.broken(self.root), [])

    def test_a_base_resolves_a_valid_fragment_in_its_target(self):
        """F2: A base href changes the page targeted by a fragment."""
        self.write("b.html", page(ids=["here"]))
        self.write(
            "a.html",
            page(links=["#here"]).replace(
                "<body>", '<head><base href="b.html"></head><body>'
            ),
        )
        self.assertEqual(anchors.broken(self.root), [])

    def test_a_base_cannot_hide_a_missing_fragment_in_its_target(self):
        """F2: An id in the source page cannot satisfy a link through base."""
        self.write("b.html", page(ids=["elsewhere"]))
        self.write(
            "a.html",
            page(ids=["here"], links=["#here"]).replace(
                "<body>", '<head><base href="b.html"></head><body>'
            ),
        )
        self.assertEqual(anchors.broken(self.root), ["a.html: #here"])


    def test_markup_in_a_textarea_is_not_a_link(self):
        """F3: Textarea text is reported as a live anchor element."""
        self.write(
            "a.html",
            '<textarea><a href="b.html#gone">example</a></textarea>',
        )
        self.write("b.html", page(ids=["here"]))
        self.assertEqual(anchors.broken(self.root), [])

    def test_markup_in_a_textarea_cannot_satisfy_a_real_link(self):
        """F3: Textarea text cannot create an id in the document."""
        self.write(
            "a.html",
            '<textarea><h2 id="gone">example</h2></textarea>'
            '<a href="#gone">real link</a>',
        )
        self.assertEqual(anchors.broken(self.root), ["a.html: #gone"])


    def test_top_fragments_do_not_need_an_element_id(self):
        """F4: HTML resolves top fragments without a matching id."""
        self.write("a.html", page(links=["#top", "#TOP", "#t%6Fp"]))
        self.assertEqual(anchors.broken(self.root), [])

    def test_a_literal_fragment_id_is_tried_before_decoding(self):
        """F4: A literal fragment match precedes percent-decoding in HTML."""
        self.write(
            "a.html", page(ids=["encoded%20id"], links=["#encoded%20id"])
        )
        self.assertEqual(anchors.broken(self.root), [])

    def test_authored_404_links_are_checked(self):
        """F5: Skipping 404.html hides links from an author's 404.md."""
        self.write("index.html", page(ids=["home"]))
        self.write("guide.html", page(ids=["here"]))
        self.write(
            "404.html",
            '<head><base href="/"></head><body>'
            '<h1 id="missing"><a class="header" href="#missing">Missing</a></h1>'
            '<a href="guide.html#here">Recovery</a>'
            '<a href="guide.html#gone">Recovery</a></body>',
        )
        self.assertEqual(anchors.broken(self.root), ["404.html: guide.html#gone"])

    def test_recovery_links_on_a_checked_page_are_checked(self):
        self.write("guide.html", page(ids=["here"]))
        self.write(
            "recovery.html",
            '<head><base href="/"></head>'
            + page(links=["guide.html#here", "guide.html#gone"]),
        )
        self.assertEqual(
            anchors.broken(self.root), ["recovery.html: guide.html#gone"]
        )


if __name__ == "__main__":
    unittest.main()
