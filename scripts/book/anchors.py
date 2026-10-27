#!/usr/bin/env python3
"""Fail on a link in the built book whose `#heading` names no id in its page.

mdbook-linkcheck checks that a linked page exists, but not the part after
`#`, so a renamed heading would leave its links stale. This reads the HTML
that mdBook wrote, where every heading's id is final.

    python3 scripts/book/anchors.py docs/book/html
"""

from html.parser import HTMLParser
from pathlib import Path
import sys
from urllib.parse import unquote, urljoin, urlsplit


# The print page joins every chapter, so its links are the chapters' own.
PRINT = "print.html"

# mdBook serves this page at any missing path, with `<base>` at the site
# root, so its own heading links (`<a class="header">`) point at the index
# page. Its other links are checked.
NOT_FOUND = "404.html"

# Elements whose content is text: markup inside them is neither a link nor
# an id. Some Python builds parse them so already, and some do not.
TEXT_ONLY = {"textarea", "title"}


class Page(HTMLParser):
    """The ids, the `<base href>`, and the link targets of one HTML page."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.ids = set()
        self.links = []
        self.headers = set()
        self.base = None
        self.text_only = 0

    def handle_starttag(self, tag, attrs):
        if self.text_only:
            return
        if tag in TEXT_ONLY:
            self.text_only += 1
        for name, value in attrs:
            if value is None:
                continue
            if name == "id" or (tag == "a" and name == "name"):
                self.ids.add(value)
            elif tag == "a" and name == "href":
                self.links.append(value)
                if "header" in (dict(attrs).get("class") or "").split():
                    self.headers.add(value)
            elif tag == "base" and name == "href" and self.base is None:
                self.base = value

    def handle_startendtag(self, tag, attrs):
        if tag not in TEXT_ONLY:
            self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag):
        if tag in TEXT_ONLY and self.text_only:
            self.text_only -= 1


def read(path):
    page = Page()
    page.feed(path.read_text(encoding="utf-8"))
    page.close()
    return page


def found(fragment, ids):
    """Whether a browser finds `fragment` among `ids`: as written, then
    decoded, and `top` names the top of any page."""
    decoded = unquote(fragment)
    return fragment in ids or decoded in ids or decoded.lower() == "top"


def broken(root):
    """Each link under `root` to a heading that its page lacks, as
    `<page>: <href>`, in page order. The book is served from `root`, so a
    path that starts with `/` starts there."""
    root = Path(root).resolve()
    pages = {path: read(path) for path in sorted(root.rglob("*.html"))}
    missing = []
    for path, page in pages.items():
        top = path.parent == root
        if top and path.name == PRINT:
            continue
        skipped = page.headers if top and path.name == NOT_FOUND else set()
        url = "/" + path.relative_to(root).as_posix()
        if page.base is not None:
            url = urljoin(url, page.base)
        for href in page.links:
            if href in skipped:
                continue
            parts = urlsplit(urljoin(url, href))
            if parts.scheme or parts.netloc or not parts.fragment:
                continue
            target = unquote(parts.path)
            if target.endswith("/"):
                target += "index.html"
            target = root / target.lstrip("/")
            if target.suffix != ".html":
                continue
            linked = pages.get(target)
            if linked is None or not found(parts.fragment, linked.ids):
                missing.append(f"{path.relative_to(root).as_posix()}: {href}")
    return missing


def main(argv):
    if len(argv) != 2:
        print("usage: anchors.py <book html directory>", file=sys.stderr)
        return 2
    missing = broken(argv[1])
    for link in missing:
        print(f"link to a missing heading: {link}", file=sys.stderr)
    if missing:
        print(f"links to a missing heading: {len(missing)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
