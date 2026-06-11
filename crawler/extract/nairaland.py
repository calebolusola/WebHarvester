from .base import clean_text, page_title

# nairaland is old-school table markup. a post is an author/timestamp header
# cell followed by a body cell holding <div class="narrow">. authors that appear
# *inside* a narrow are quoted users (reply targets), not the poster.


def _is_in_narrow(el) -> bool:
    p = el.getparent()
    while p is not None:
        cls = p.get("class") or ""
        if "narrow" in cls:
            return True
        p = p.getparent()
    return False


def _quotes_in(narrow) -> list[dict]:
    # nairaland markup: <blockquote cite="/post/ID"><a href="/post/ID">Author</a>:
    # quoted text...</blockquote>. the quoted author is the first <a> INSIDE the
    # blockquote, and the cite attr points at the exact post being replied to.
    quotes = []
    for bq in narrow.iter("blockquote"):
        author = None
        a = bq.find(".//a")
        if a is not None:
            author = clean_text(a.text_content()) or None
        if author is None:
            # older format: author in the <a> just before the blockquote
            prev = bq.getprevious()
            if prev is not None:
                pa = prev.findall(".//a")
                if pa:
                    author = clean_text(pa[0].text_content()) or None
                elif prev.tag == "a":
                    author = clean_text(prev.text_content()) or None
        text = clean_text(bq.text_content())
        # drop the leading "Author:" the attribution anchor leaves in the text
        if author and text.startswith(author):
            text = text[len(author):].lstrip(": \n").strip()
        quotes.append({
            "quoted_author": author,
            "quoted_post": bq.get("cite") or None,
            "text": text[:1000],
        })
    return quotes


def _body_text(narrow) -> str:
    # drop the quoted blockquotes so the post's own words stand alone.
    # quotes are kept separately in structure (with their author + post ref).
    for bq in list(narrow.iter("blockquote")):
        bq.drop_tree()
    return clean_text(narrow.text_content())


def extract(tree) -> dict:
    title = page_title(tree)

    # walk the doc once, in order, collecting the three things we care about
    authors, stamps, bodies = [], [], []
    for el in tree.iter("a", "span", "div"):
        cls = el.get("class") or ""
        if el.tag == "a" and "user" in cls and not _is_in_narrow(el):
            authors.append(el)
        elif el.tag == "span" and ("s" in cls.split() or "m" in cls.split()) and not _is_in_narrow(el):
            stamps.append(el)
        elif el.tag == "div" and "narrow" in cls.split():
            bodies.append(el)

    posts = []
    for i, narrow in enumerate(bodies):
        author = clean_text(authors[i].text_content()) if i < len(authors) else None
        # match the timestamp that sits between this author and the next
        ts = clean_text(stamps[i].text_content()) if i < len(stamps) else None
        quotes = _quotes_in(narrow)      # before _body_text, which drops the blockquotes
        posts.append({
            "index": i,
            "author": author or None,
            "timestamp": ts or None,
            "body_text": _body_text(narrow),
            "quotes": quotes,
        })

    if not posts:
        # markup didn't look like we expected, let the generic extractor try
        from .base import generic_extract
        return generic_extract(tree)

    # flat text keeps the conversation readable: "author: body" per post
    flat = "\n\n".join(
        f"{p['author'] or 'anon'}:\n{p['body_text']}" for p in posts if p["body_text"]
    )
    return {
        "extractor": "nairaland",
        "title": title,
        "text": clean_text(flat),
        "structure": {"title": title, "posts": posts, "post_count": len(posts)},
    }
