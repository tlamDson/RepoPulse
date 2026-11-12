"""Static checks on site/ (no browser, no network).

The page is plain HTML + CSS + JS. It may only load files that sit next to it (relative paths,
because Pages serves it under /RepoPulse/), only fetch the JSON that repopulse/export_site.py
writes, carry no hand-typed numbers, label GH Archive as real data and spell out the "simulated"
rule, and give every Q1-Q6 section a chart and a table.
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path

import pytest

from repopulse import export_site

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / "site"
INDEX = SITE / "index.html"
SECTIONS = (*export_site.MART_COLUMNS, "runs")
RESOURCE_ATTRS = {"src", "href", "srcset", "action", "data", "poster", "formaction"}
SVG_NAMESPACE = "http://www.w3.org/2000/svg"
EXTERNAL = re.compile(r"^\s*(?:[a-z][a-z0-9+.-]*:|//)", re.IGNORECASE)


class Page(HTMLParser):
    """Collect tags (with their ancestors' attributes), visible text and inline code."""

    VOID = {"meta", "link", "img", "br", "hr", "input", "source", "area", "base", "col", "wbr"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tags: list[tuple[str, dict, list[dict]]] = []
        self.stack: list[tuple[str, dict]] = []
        self.text: list[str] = []
        self.inline: list[str] = []

    def handle_starttag(self, tag, attrs):
        a = {k: (v or "") for k, v in attrs}
        self.tags.append((tag, a, [x for _, x in self.stack]))
        if tag not in self.VOID:
            self.stack.append((tag, a))

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i][0] == tag:
                del self.stack[i:]
                break

    def handle_data(self, data):
        current = self.stack[-1][0] if self.stack else ""
        if current in ("script", "style"):
            self.inline.append(data)
        else:
            self.text.append(data)


def page() -> Page:
    assert INDEX.is_file(), f"missing {INDEX}"
    p = Page()
    p.feed(INDEX.read_text(encoding="utf-8"))
    return p


def site_files(suffix: str) -> list[Path]:
    assert SITE.is_dir(), f"missing {SITE}"
    return sorted(SITE.rglob(f"*{suffix}"))


def js_text() -> str:
    files = site_files(".js")
    assert files, "site/ has no JavaScript"
    return "\n".join(f.read_text(encoding="utf-8") for f in files)


def css_text() -> str:
    files = site_files(".css")
    assert files, "site/ has no stylesheet"
    return "\n".join(f.read_text(encoding="utf-8") for f in files)


def visible_text() -> str:
    return re.sub(r"\s+", " ", " ".join(page().text))


# ---------------------------------------------------------------- basics


def test_index_has_viewport_meta_lang_and_charset():
    tags = page().tags
    metas = [a for t, a, _ in tags if t == "meta"]
    viewport = [m for m in metas if m.get("name") == "viewport"]
    assert viewport and "width=device-width" in viewport[0].get("content", "")
    assert any(m.get("charset", "").lower() == "utf-8" for m in metas)
    html = [a for t, a, _ in tags if t == "html"]
    assert html and html[0].get("lang")


def test_index_loads_only_local_stylesheet_and_script():
    tags = page().tags
    scripts = [a["src"] for t, a, _ in tags if t == "script" and "src" in a]
    styles = [a["href"] for t, a, _ in tags if t == "link" and a.get("rel") == "stylesheet"]
    assert scripts and styles
    for path in scripts + styles:
        assert (SITE / path).is_file(), path


def test_index_declares_an_inline_icon_so_no_favicon_is_requested():
    # Without a declared icon the browser asks for /favicon.ico, which Pages answers with a 404.
    icons = [a.get("href", "") for t, a, _ in page().tags if t == "link" and a.get("rel") == "icon"]
    assert icons == ["data:,"]


# ---------------------------------------------------------------- nothing external


def test_no_external_or_root_absolute_resources_in_html():
    for tag, attrs, _ in page().tags:
        for name, value in attrs.items():
            if name not in RESOURCE_ATTRS or not value:
                continue
            if tag == "a" and name == "href":
                continue  # plain links (e.g. to the repo) load nothing
            if tag == "link" and attrs.get("rel") == "icon" and value == "data:,":
                continue  # the empty inline icon: no request at all
            assert not EXTERNAL.match(value), (tag, name, value)
            assert not value.startswith("/"), (tag, name, value)  # Pages serves under /RepoPulse/


def test_no_inline_script_or_style_in_html():
    assert "".join(page().inline).strip() == ""


def test_no_url_in_scripts_or_styles():
    # The SVG namespace is an XML name that createElementNS needs; the browser never fetches it.
    js = js_text()
    assert js.count(SVG_NAMESPACE) <= 1
    for text in (js.replace(SVG_NAMESPACE, ""), css_text()):
        assert "http://" not in text
        assert "https://" not in text
        assert "//cdn" not in text
    assert "@import" not in css_text()
    assert not re.search(r"url\(", css_text())


def test_scripts_do_not_contact_other_services():
    js = js_text()
    for forbidden in ("XMLHttpRequest", "sendBeacon", "WebSocket", "EventSource", "import("):
        assert forbidden not in js, forbidden
    for target in re.findall(r"fetch\(\s*([^)]*)\)", js):
        assert not re.search(r"['\"`]\s*(?:https?:)?//", target), target


def test_fetches_exactly_the_files_the_exporter_writes():
    js = js_text()
    fetched = set(re.findall(r"""["'`](data/[A-Za-z0-9_./-]+\.json)["'`]""", js))
    expected = {f"data/{m}.json" for m in export_site.MART_COLUMNS}
    expected |= {"data/runs.json", "data/meta.json"}
    assert fetched == expected
    for path in fetched:
        assert not path.startswith("/")


def test_tableau_csv_links_are_relative_and_cover_every_section():
    hrefs = {a["href"] for t, a, _ in page().tags if t == "a" and "href" in a}
    for name in SECTIONS:
        assert f"tableau/{name}.csv" in hrefs, name


# ---------------------------------------------------------------- safe DOM


def test_scripts_never_inject_html():
    js = js_text()
    for forbidden in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("):
        assert forbidden not in js, forbidden
    assert "textContent" in js
    assert "createElementNS" in js  # charts are SVG built node by node


# ---------------------------------------------------------------- labels


def test_says_gh_archive_is_real_data_with_update_time():
    text = visible_text()
    assert "GH Archive" in text
    assert "real data" in text.lower()
    ids = {a.get("id") for _, a, _ in page().tags}
    assert {"generated-at", "latest-data-date"} <= ids  # filled from data/meta.json


def test_spells_out_the_simulated_rule():
    text = visible_text().lower()
    assert "simulated" in text
    assert "no simulated data" in text


def test_calls_it_incremental_batch_not_streaming():
    text = visible_text().lower()
    assert "incremental batch" in text
    assert "real-time" not in text.replace("not real-time", "")
    assert "streaming" not in text.replace("not streaming", "")


def test_no_hand_typed_numbers_in_html():
    allowed = re.compile(r"\bQ[1-6]\b|\bp(?:50|90)\b|\bW\+(?:4|12)\b|\bTop\s?N\b")
    text = allowed.sub("", visible_text())
    assert not re.search(r"\d", text), re.findall(r"\S*\d\S*", text)


def test_no_numbers_in_html_attributes_except_layout_ids():
    for tag, attrs, _ in page().tags:
        for name, value in attrs.items():
            if name in ("content", "charset", "lang"):
                continue
            stripped = re.sub(r"\bq[1-6]\b|\bQ[1-6]\b", "", value)
            assert not re.search(r"\d", stripped), (tag, name, value)


# ---------------------------------------------------------------- sections


@pytest.mark.parametrize("name", SECTIONS)
def test_every_section_has_a_chart_a_table_and_a_status_slot(name):
    tags = page().tags
    sections = [a for t, a, _ in tags if t == "section" and a.get("data-mart") == name]
    assert len(sections) == 1, name
    inside = [a for _, a, anc in tags if any(x.get("data-mart") == name for x in anc)]
    roles = {a.get("data-role") for a in inside}
    assert {"chart", "table", "status"} <= roles, (name, roles)


@pytest.mark.parametrize("q", ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6"])
def test_sections_are_labelled_q1_to_q6(q):
    assert q in visible_text()


# ---------------------------------------------------------------- narrow screens + dark mode


def test_css_handles_narrow_screens_and_dark_mode():
    css = css_text()
    assert "prefers-color-scheme: dark" in css
    assert re.search(r"overflow-x:\s*auto", css)  # wide tables scroll inside their own box
    assert re.search(r"max-width:\s*100%", css)
    assert "@media (max-width" in css


def test_charts_scale_with_viewbox():
    assert "viewBox" in js_text()
