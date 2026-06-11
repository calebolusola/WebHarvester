import gzip
import hashlib
import re

from lxml import html as lxml_html

# tags that never carry the content we want for an LLM corpus
JUNK_TAGS = ("script", "style", "noscript", "svg", "nav", "header", "footer",
             "aside", "form", "button", "iframe", "head")

_WS = re.compile(r"[ \t\r\f\v]+")
_NL = re.compile(r"\n{3,}")


def clean_text(s: str) -> str:
    if not s:
        return ""
    s = _WS.sub(" ", s)
    s = "\n".join(line.strip() for line in s.splitlines())
    s = _NL.sub("\n\n", s)
    return s.strip()


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()


def parse(html: bytes):
    return lxml_html.fromstring(html)


def strip_junk(tree):
    for tag in JUNK_TAGS:
        for el in tree.iter(tag):
            el.drop_tree()
    return tree


def brs_to_newlines(tree):
    # text_content() drops <br>, collapsing "line1<br>line2" into "line1line2".
    # turn each <br> into a newline so post text keeps its line structure.
    for br in tree.iter("br"):
        br.tail = "\n" + (br.tail or "")


def detect_lang(tree) -> str | None:
    for el in tree.iter("html"):
        lang = el.get("lang")
        if lang:
            return lang.split("-")[0].lower()
    return None


def page_title(tree) -> str:
    for h in ("h1", "h2"):
        nodes = tree.findall(".//" + h)
        if nodes and (nodes[0].text_content() or "").strip():
            return clean_text(nodes[0].text_content())[:300]
    t = tree.findall(".//title")
    if t:
        return clean_text(t[0].text_content())[:300]
    return ""


def _main_node(tree):
    # prefer obvious main-content containers, else the densest block
    for xp in ('.//main', './/article', './/*[@id="content"]',
               './/*[@id="main"]', './/*[@role="main"]'):
        nodes = tree.xpath(xp)
        if nodes:
            return nodes[0]
    # fallback: the div/section with the most text
    best, best_len = tree, 0
    for el in tree.iter("div", "section"):
        n = len(el.text_content() or "")
        if n > best_len:
            best, best_len = el, n
    return best


def generic_extract(tree) -> dict:
    # readability-ish: title + ordered headings/paragraphs/list items, no styling
    node = _main_node(tree)
    blocks = []
    for el in node.iter("h1", "h2", "h3", "h4", "p", "li", "blockquote", "pre"):
        txt = clean_text(el.text_content())
        if len(txt) < 2:
            continue
        kind = el.tag
        blocks.append({"type": kind, "text": txt})
    text = clean_text("\n\n".join(b["text"] for b in blocks))
    return {
        "extractor": "generic",
        "title": page_title(tree),
        "text": text,
        "structure": {"blocks": blocks},
    }


def build_record(url: str, html: bytes, http_status: int, store_html: bool,
                 extract_fn) -> dict:
    # the single entry point run inside the process pool. never raises:
    # a parse failure still yields a (thin) record so we don't lose the page
    try:
        tree = parse(html)
        lang = detect_lang(tree)
        strip_junk(tree)
        brs_to_newlines(tree)   # keep line breaks before text_content() drops them
        data = extract_fn(tree)
    except Exception as e:
        data = {"extractor": "error", "title": "", "text": "",
                "structure": {"error": f"{type(e).__name__}: {e}"}}
        lang = None

    text = data.get("text") or ""
    return {
        "url": url,
        "http_status": http_status,
        "lang": lang,
        "extractor": data.get("extractor"),
        "title": data.get("title") or "",
        "text": text,
        "text_len": len(text),
        "structure": data.get("structure"),
        "content_hash": content_hash(text) if text else None,
        "html": gzip.compress(html) if store_html else None,
    }
