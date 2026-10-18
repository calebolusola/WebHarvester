"""build a public release of the nairaland corpus: parquet shards + dataset card.

what it does, in order:
  - reads thread pages straight out of postgres (keyset paging, no giant export)
  - drops everything that isn't a thread page (profiles, login pages, user post lists)
  - dedups by post content, so /post/ID permalinks that land on an already-seen
    thread page don't count twice
  - swaps usernames for stable pseudonyms (hmac with a private salt), including
    @mentions, and scrubs emails / phone numbers / long account-style numbers
  - tags each post with a language (glotlid, which knows pidgin/yoruba/hausa/igbo)
  - writes zstd parquet shards, stats.json, and a README.md dataset card

usage:
  .venv/bin/pip install -r requirements-release.txt
  .venv/bin/python scripts/build_release.py --db "$CRAWLER_DB" \
      --out ~/crawlerdata/release --langid ~/crawlerdata/models/glotlid.bin

  add --compat-python /path/to/older/python (pyarrow <= 19) so the shards load on
  older pyarrow too. pyarrow 26 writes stats that 19 can't read. to fix shards
  that are already built: --compat-only --compat-python /opt/anaconda3/bin/python

  card tweaks (license, hf user) without rebuilding the shards:
  .venv/bin/python scripts/build_release.py --card-only --license cc-by-nc-4.0 --hf-user <you>

  --limit 5000 for a quick test run. the salt file is created on first run and
  must never be published. keep it though, a later release needs the same salt
  so the same user keeps the same pseudonym.
"""

import argparse
import asyncio
import collections
import datetime as dt
import hashlib
import hmac
import json
import multiprocessing
import os
import re
import secrets
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from urllib.parse import urlsplit

import asyncpg
import pyarrow as pa
import pyarrow.parquet as pq

THREAD_RE = r"^https?://[^/]+/[0-9]+(/|$)"
POST_RE = r"^https?://[^/]+/post/[0-9]+"

POST_T = pa.struct([
    ("index", pa.int32()),
    ("author_id", pa.string()),
    ("posted_at", pa.string()),
    ("edited_at", pa.string()),
    ("lang", pa.string()),
    ("text", pa.string()),
    ("quotes", pa.list_(pa.struct([
        ("author_id", pa.string()),
        ("post_id", pa.int64()),
        ("text", pa.string()),
    ]))),
])

SCHEMA = pa.schema([
    ("id", pa.string()),
    ("url", pa.string()),
    ("thread_id", pa.int64()),
    ("page", pa.int32()),
    ("board", pa.string()),
    ("title", pa.string()),
    ("lang", pa.string()),
    ("n_posts", pa.int32()),
    ("n_chars", pa.int32()),
    ("crawled_at", pa.string()),
    ("text", pa.string()),
    ("posts", pa.list_(POST_T)),
])


# ---- per-page transform (runs in worker processes) ----------------------------

_SALT = b""
_LID = None   # set in the parent before the pool forks, so workers share one copy

EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
# nigerian mobiles (0803..., +234 803...) and anything else that looks like an
# international number
PHONE = re.compile(r"(?<![\w+])(?:(?:\+|00)?234[\s-]?|0)[789][01]\d(?:[\s-]?\d){7}(?!\d)"
                   r"|(?<![\w+])\+\d{1,3}[\s-]?\d(?:[\s-]?\d){7,12}(?!\d)")
# 10-11 digit runs: account numbers (nuban) and bvns get posted a lot in
# "pay into this account" threads
LONGNUM = re.compile(r"(?<!\d)\d{10,11}(?!\d)")
# any @handle, not just names on the page. skips "@N500" style prices
MENTION = re.compile(r"@(?!N[\d,.]+\b)([A-Za-z_]\w{2,})")
# a link to someone's profile ties their pseudonym straight back to a real person.
# the lookbehind keeps "fox.com/..." and "cdninstagram.com" image links out of it
SOCIAL = re.compile(
    r"(?<![\w.-])(?:https?://)?(?:(?:www|m|web|mobile|mbasic)\.)?"
    r"(?:facebook\.com|fb\.com|fb\.me|twitter\.com|x\.com|instagram\.com|instagr\.am|"
    r"tiktok\.com|linkedin\.com|snapchat\.com|t\.me|threads\.net|wa\.me|chat\.whatsapp\.com)"
    r"/[^\s\[\]()<>\"']+", re.I)
# blackberry pins: 8 hex chars, posted everywhere 2009-2014 ("add me on bbm 21598877").
# only scrubbed shortly after a bbm/pin mention, an 8 char hex word alone is too ambiguous
PIN_HINT = re.compile(r"\b(?:bbm|bb|pin)\b", re.I)
HEX8 = re.compile(r"(?<![0-9A-Za-z])(?=[A-Fa-f]*\d)[0-9A-Fa-f]{8}(?![0-9A-Za-z])")
# leftover forum markup. image blocks go entirely (they're just urls), other tags
# go but keep the text between them
IMG_BLOCK = re.compile(r"\[img\].*?\[/img\]", re.I | re.S)
BBTAG = re.compile(
    r"\[/?(?:quote|img|url|b|i|u|s|color|colour|size|font|center|left|right|code|youtube|"
    r"email|sup|sub|tt|pre|hr|li|list|table|tr|td|th|move|glow|shadow|flash|nobbc)"
    r"(?:[ =][^\]\n]{0,200})?\]", re.I)
# hand-typed quote tags people never closed: "[quote author=X post=4987099The..."
# or "[/quote great". strip the tag part, keep what they actually wrote after it
BROKEN_QUOTE = re.compile(r"\[quote\s+author=[^\s\]]+(?:\s+(?:post|link|date)=[\w.#=]*\d)*\]?"
                          r"|\[/quote\]?", re.I)
# a real nairaland username. the extractor sometimes records a link as a quote's
# author (when the quote has no author link), and those must not become "users"
USERNAME = re.compile(r"[\w-]{2,40}")

MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
STAMP = re.compile(r"(\d{1,2}):(\d{2})\s*(am|pm)(?:\s+on\s+([a-z]{3})\s+(\d{1,2})(?:,\s*(\d{4}))?)?", re.I)


def _load_words():
    # macos and most linux boxes ship this. without it every bare name gets swapped,
    # which over-replaces a little but never leaks
    try:
        with open("/usr/share/dict/words") as f:
            return {w.strip().lower() for w in f if len(w.strip()) >= 4}
    except OSError:
        return set()


WORDS = _load_words()


def _init(salt: bytes):
    global _SALT
    _SALT = salt


def pseudo(name):
    if not name:
        return None
    # usernames are case-insensitive on nairaland, so normalize before hashing
    d = hmac.new(_SALT, name.strip().lower().encode(), hashlib.sha256).hexdigest()
    return "u_" + d[:12]


def valid_user(name):
    name = (name or "").strip()
    return name if USERNAME.fullmatch(name) else None


def scrub_pins(text, counts):
    spans = set()
    for m in PIN_HINT.finditer(text):
        for h in HEX8.finditer(text, m.end(), min(len(text), m.end() + 80)):
            spans.add(h.span())
    if not spans:
        return text
    out, last = [], 0
    for a, b in sorted(spans):
        out.append(text[last:a])
        out.append("[PIN]")
        last = b
    out.append(text[last:])
    counts["pin"] += len(spans)
    return "".join(out)


def scrub(text, counts):
    if not text:
        return text
    # markup first: "[quote author=someone post=123]" carries a real username
    text, n = IMG_BLOCK.subn("", text)
    text, m = BBTAG.subn("", text)
    text, k = BROKEN_QUOTE.subn("", text)
    counts["markup"] += n + m + k
    text, n = SOCIAL.subn("[SOCIAL]", text)
    counts["social"] += n
    text, n = EMAIL.subn("[EMAIL]", text)
    counts["email"] += n
    text, n = PHONE.subn("[PHONE]", text)
    counts["phone"] += n
    text = scrub_pins(text, counts)
    text, n = LONGNUM.subn("[NUMBER]", text)
    counts["number"] += n
    return text


def _stamp(m, fetched: dt.datetime):
    # nairaland drops the year for this year's posts and the date for today's.
    # fill them in from when the page was fetched.
    hh, mm, ap, mon, day, year = m.groups()
    h = int(hh) % 12 + (12 if ap.lower() == "pm" else 0)
    if mon is None:
        d = fetched.date()
    else:
        mo = MONTHS.get(mon.lower())
        if mo is None:
            return None
        y = int(year) if year else fetched.year
        try:
            d = dt.date(y, mo, int(day))
        except ValueError:
            return None
        if not year and d > fetched.date():
            d = d.replace(year=y - 1)
    return dt.datetime(d.year, d.month, d.day, h, int(mm)).isoformat(timespec="minutes")


def stamps(raw, fetched):
    if not raw:
        return None, None
    found = list(STAMP.finditer(raw))
    posted = _stamp(found[0], fetched) if found else None
    edited = _stamp(found[1], fetched) if len(found) > 1 and "modified" in raw.lower() else None
    return posted, edited


def canon_url(url):
    p = urlsplit(url)
    host = (p.hostname or "").lower().rstrip(".")
    if host in ("nairaland.com", "wap.nairaland.com"):
        host = "www.nairaland.com"
    return f"https://{host}{p.path}"


def split_title(title):
    # "Topic Words - Board (4) - Nairaland" -> ("Topic Words", "Board")
    if not title:
        return None, None
    parts = title.rsplit(" - ", 2)
    if len(parts) == 3 and parts[2].strip().lower() == "nairaland":
        return parts[0].strip(), re.sub(r"\s*\(\d+\)$", "", parts[1]).strip() or None
    return title.strip(), None


def _int(s, bits=64):
    # garbage in a url or quote cite can be a 30 digit "id". null it, don't crash the writer
    try:
        v = int(s)
    except (TypeError, ValueError):
        return None
    return v if -(2 ** (bits - 1)) <= v < 2 ** (bits - 1) else None


def _lookup(names, hit):
    # re.I folds unicode case more loosely than .lower() (turkish dotless i matches
    # i, for one), so a match isn't always a key. fall back to finding which name it was
    pid = names.get(hit.lower())
    if pid:
        return pid
    for n, pid in names.items():
        if re.fullmatch(re.escape(n), hit, re.I):
            return pid
    return hit


def transform(row):
    url, fetched_at, title, structure = row
    try:
        s = json.loads(structure) if isinstance(structure, str) else structure
    except Exception:
        return ("skip", "bad_json")
    posts_in = (s or {}).get("posts") or []
    if not any((p.get("body_text") or "").strip() for p in posts_in):
        return ("skip", "empty")

    # dedup key: the raw conversation, before any rewriting
    h = hashlib.blake2b(digest_size=16)
    for p in posts_in:
        h.update((p.get("author") or "").encode())
        h.update(b"\x00")
        h.update((p.get("body_text") or "").encode())
        h.update(b"\x01")

    counts = collections.Counter()
    fetched = dt.datetime.fromtimestamp(fetched_at or time.time(), dt.timezone.utc).replace(tzinfo=None)

    # every username on the page, so mentions of them in post bodies get swapped too
    names = {}
    for p in posts_in:
        for n in [p.get("author")] + [q.get("quoted_author") for q in p.get("quotes") or []]:
            n = valid_user(n)
            if n:
                names[n.lower()] = pseudo(n)
    # bare (no @) names too, unless the name is also an ordinary english word. a user
    # called "truth" shouldn't turn every "truth" on the page into a pseudonym
    bare = [re.escape(n) for n in names
            if len(n) >= 4 and (re.search(r"[\d_]", n) or n not in WORDS)]
    bare_re = re.compile(r"(?<!\w)(" + "|".join(bare) + r")(?!\w)", re.I) if bare else None

    def unname(text):
        if not text:
            return text

        def at(m):
            # every @handle gets the same keyed hash, so a nairaland user mentioned
            # on some other page still maps to their one id. plain words ("@all",
            # "@home") are left alone unless someone on the page has that name
            word = m.group(1)
            pid = names.get(word.lower())
            if pid is None:
                if word.lower() in WORDS:
                    return m.group(0)
                pid = pseudo(word)
            counts["mention"] += 1
            return "@" + pid
        text = MENTION.sub(at, text)
        if bare_re:
            text, n = bare_re.subn(lambda m: _lookup(names, m.group(1)), text)
            counts["mention"] += n
        return text

    # scrub before unname: pseudonyms are hex, and a run of 10+ digits in one would
    # get eaten by the account-number pattern
    posts = []
    for p in posts_in:
        posted, edited = stamps(p.get("timestamp"), fetched)
        quotes = []
        for q in p.get("quotes") or []:
            cite = q.get("quoted_post") or ""
            m = re.search(r"(\d+)", cite)
            quotes.append({
                "author_id": pseudo(valid_user(q.get("quoted_author"))),
                "post_id": _int(m.group(1)) if m else None,
                "text": unname(scrub(q.get("text") or "", counts)),
            })
        posts.append({
            "index": _int(p.get("index") or len(posts), 32),
            "author_id": pseudo(valid_user(p.get("author"))),
            "posted_at": posted,
            "edited_at": edited,
            "lang": None,
            "text": unname(scrub(p.get("body_text") or "", counts)),
            "quotes": quotes,
        })

    if _LID:
        _LID.tag(posts)

    curl = canon_url(url)
    path = urlsplit(curl).path.strip("/").split("/")
    thread_id = page = None
    if path and path[0].isdigit():
        thread_id = _int(path[0])
        page = _int(path[2], 32) if len(path) > 2 and path[2].isdigit() else 0
    topic, board = split_title(title)
    text = "\n\n".join(f"{p['author_id'] or 'anon'}:\n{p['text']}" for p in posts if p["text"])

    rec = {
        "id": hashlib.blake2b(curl.encode(), digest_size=8).hexdigest(),
        "url": curl,
        "thread_id": thread_id,
        "page": page,
        "board": board,
        "title": scrub(topic, counts),
        "lang": page_lang(posts),
        "n_posts": len(posts),
        "n_chars": sum(len(p["text"]) for p in posts),
        "crawled_at": fetched.date().isoformat(),
        "text": text,
        "posts": posts,
    }
    return ("ok", h.digest(), rec, dict(counts))


def transform_batch(rows):
    out = []
    for r in rows:
        # one weird page must not kill a 45 minute build. count it and move on
        try:
            out.append(transform(r))
        except Exception as e:
            out.append(("skip", "error:" + type(e).__name__))
    return out


# ---- language id ---------------------------------------------------------------
# the model is ~1.6GB. it's loaded once in the parent and the pool is forked after,
# so every worker reads the same copy-on-write pages instead of loading its own.

# glotlid mixes nigerian pidgin up with other english-based creoles (jamaican,
# cameroonian, hawaiian, krio...). on a nigerian forum it's pidgin.
CREOLES = {k: "pcm_Latn" for k in [
    "jam_Latn", "wes_Latn", "hwc_Latn", "kri_Latn", "pis_Latn", "tpi_Latn", "bzj_Latn",
    "icr_Latn", "srn_Latn", "gul_Latn", "djk_Latn", "trf_Latn", "gcl_Latn", "bah_Latn",
    "svc_Latn", "vic_Latn", "lir_Latn", "bis_Latn", "rop_Latn", "tcs_Latn",
]}
# languages you'd actually expect on nairaland. anything else is a misfire on
# short or odd text and becomes "und"
PLAUSIBLE = {
    "eng_Latn", "pcm_Latn", "yor_Latn", "hau_Latn", "ibo_Latn", "fra_Latn",
    "efi_Latn", "ibb_Latn", "tiv_Latn", "urh_Latn", "bin_Latn", "fuv_Latn",
    "ful_Latn", "ijc_Latn", "kcg_Latn", "arb_Arab", "spa_Latn", "deu_Latn",
}


class LangId:
    def __init__(self, path):
        import fasttext
        fasttext.FastText.eprint = lambda *a, **k: None   # silence the load warning
        self.m = fasttext.load_model(path)

    def tag(self, posts):
        todo = [p for p in posts if len(p["text"]) >= 20]
        if not todo:
            return
        # speed scales with length and 400 chars is plenty to call a language
        texts = [p["text"][:400].replace("\n", " ") for p in todo]
        # call the c++ side directly, the python wrapper's predict() breaks on numpy 2
        labels, probs = self.m.f.multilinePredict(texts, 5, 0.0, "strict")
        for p, labs, prs in zip(todo, labels, probs):
            p["lang"] = "und"
            # first guess that's plausible for a nigerian forum wins
            for lab, pr in zip(labs, prs):
                lab = lab.replace("__label__", "")
                lab = CREOLES.get(lab, lab)
                if lab in PLAUSIBLE:
                    if pr >= 0.3:
                        p["lang"] = lab
                    break


def page_lang(posts):
    by = collections.Counter()
    for p in posts:
        if p["lang"] and p["lang"] != "und":
            by[p["lang"]] += len(p["text"])
    return by.most_common(1)[0][0] if by else None


# ---- output -------------------------------------------------------------------

class ShardWriter:
    def __init__(self, out_dir, shard_mb):
        self.dir = os.path.join(out_dir, "data")
        os.makedirs(self.dir, exist_ok=True)
        for f in os.listdir(self.dir):
            if f.endswith(".parquet"):
                os.unlink(os.path.join(self.dir, f))
        # text sits in both `text` and `posts`, and zstd gets roughly 3x on it,
        # so this many chars lands near shard_mb on disk
        self.max_chars = int(shard_mb * 1.5e6)
        self.paths, self.w, self.chars, self.buf = [], None, 0, []
        self.bad = 0

    def add(self, rec):
        self.buf.append(rec)
        self.chars += rec["n_chars"]
        if len(self.buf) >= 5000:
            self._flush()

    def _flush(self):
        if not self.buf:
            return
        if self.w is None:
            path = os.path.join(self.dir, f"part-{len(self.paths):05d}.parquet")
            self.w = pq.ParquetWriter(path, SCHEMA, compression="zstd", compression_level=9)
            self.paths.append(path)
        try:
            table = pa.Table.from_pylist(self.buf, schema=SCHEMA)
        except (pa.ArrowException, OverflowError, TypeError, ValueError):
            # some row arrow won't take. find it by converting one at a time and drop
            # just that one, a single bad page shouldn't sink the whole build
            ok = []
            for rec in self.buf:
                try:
                    pa.Table.from_pylist([rec], schema=SCHEMA)
                    ok.append(rec)
                except (pa.ArrowException, OverflowError, TypeError, ValueError):
                    self.bad += 1
            table = pa.Table.from_pylist(ok, schema=SCHEMA)
        self.w.write_table(table)
        self.buf = []
        if self.chars >= self.max_chars:
            self.w.close()
            self.w, self.chars = None, 0

    def close(self):
        self._flush()
        if self.w:
            self.w.close()
        # rename to the train-00000-of-00012 pattern the hub expects
        n = len(self.paths)
        final = []
        for i, p in enumerate(self.paths):
            q = os.path.join(self.dir, f"train-{i:05d}-of-{n:05d}.parquet")
            os.replace(p, q)
            final.append(q)
        return final


# ---- compatibility rewrite ------------------------------------------------------
# new pyarrow (26) writes level histograms for nested columns that older readers
# choke on ("Repetition level histogram size mismatch" on pyarrow 19), and there's
# no python switch to turn them off. so the shards get streamed, as arrow ipc
# (stable across versions), into an older pyarrow that rewrites them. same rows,
# same schema, readable everywhere.

_EMIT = """
import sys, pyarrow as pa, pyarrow.parquet as pq
pf = pq.ParquetFile(sys.argv[1])
with pa.ipc.new_stream(sys.stdout.buffer, pf.schema_arrow) as w:
    for i in range(pf.num_row_groups):
        w.write_table(pf.read_row_group(i))
"""

_ABSORB = """
import sys, pyarrow as pa, pyarrow.parquet as pq
r = pa.ipc.open_stream(sys.stdin.buffer)
with pq.ParquetWriter(sys.argv[1], r.schema, compression="zstd", compression_level=9) as w:
    for b in r:
        w.write_table(pa.Table.from_batches([b]), row_group_size=5000)
"""


def compat_rewrite(files, py):
    py = os.path.expanduser(py)
    ver = subprocess.run([py, "-c", "import pyarrow; print(pyarrow.__version__)"],
                         capture_output=True, text=True, check=True).stdout.strip()
    print(f"rewriting {len(files)} shards with pyarrow {ver} for older readers", file=sys.stderr)
    for f in files:
        tmp = f + ".tmp"
        emit = subprocess.Popen([sys.executable, "-c", _EMIT, f], stdout=subprocess.PIPE)
        absorb = subprocess.run([py, "-c", _ABSORB, tmp], stdin=emit.stdout)
        emit.stdout.close()
        if emit.wait() or absorb.returncode:
            raise SystemExit(f"compat rewrite failed on {f}")
        want, got = pq.read_metadata(f).num_rows, pq.read_metadata(tmp).num_rows
        if want != got:
            os.unlink(tmp)
            raise SystemExit(f"compat rewrite lost rows on {f}: {want} -> {got}")
        os.replace(tmp, f)
        print(f"  {os.path.basename(f)}  {got:,} rows", file=sys.stderr)


# ---- main ---------------------------------------------------------------------

def load_salt(path):
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        with open(path, "w") as f:
            f.write(secrets.token_hex(32))
        os.chmod(path, 0o600)
        print(f"created a new pseudonym salt at {path}. back it up, never publish it")
    with open(path) as f:
        return f.read().strip().encode()


async def build(args):
    global _LID
    salt = load_salt(args.salt_file)
    if args.langid:
        _LID = LangId(os.path.expanduser(args.langid))
    out = os.path.expanduser(args.out)
    os.makedirs(out, exist_ok=True)
    writer = ShardWriter(out, args.shard_mb)

    st = {
        "pages": 0, "posts": 0, "chars": 0, "dupes": 0,
        "skipped": collections.Counter(), "pii": collections.Counter(),
        "boards": collections.Counter(), "board_posts": collections.Counter(),
        "years": collections.Counter(), "langs_posts": collections.Counter(),
        "langs_chars": collections.Counter(),
    }
    seen, authors = set(), set()
    t0 = time.time()

    def consume(results):
        for r in results:
            if r[0] == "skip":
                st["skipped"][r[1]] += 1
                continue
            _, key, rec, counts = r
            if key in seen:
                st["dupes"] += 1
                continue
            seen.add(key)
            st["pages"] += 1
            st["posts"] += rec["n_posts"]
            st["chars"] += rec["n_chars"]
            st["pii"].update(counts)
            st["boards"][rec["board"] or "(unknown)"] += 1
            st["board_posts"][rec["board"] or "(unknown)"] += rec["n_posts"]
            for p in rec["posts"]:
                if p["author_id"]:
                    authors.add(p["author_id"])
                if p["posted_at"]:
                    st["years"][p["posted_at"][:4]] += 1
                if p["lang"]:
                    st["langs_posts"][p["lang"]] += 1
                    st["langs_chars"][p["lang"]] += len(p["text"])
            writer.add(rec)

    con = await asyncpg.connect(args.db.replace("+asyncpg", ""), command_timeout=None)
    loop = asyncio.get_running_loop()
    # fork, not spawn: workers inherit _LID's memory instead of reloading the model
    pool = ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("fork"),
                               initializer=_init, initargs=(salt,))
    inflight = collections.deque()
    done_rows = 0
    try:
        # thread urls first, so when a /post/ID permalink is the same page, the
        # canonical thread url is the copy that survives dedup
        for pattern in (THREAD_RE, POST_RE):
            last = ""
            while True:
                if args.limit and st["pages"] >= args.limit:
                    break
                rows = await con.fetch(
                    "SELECT url, fetched_at, title, structure::text FROM corpus "
                    "WHERE extractor = 'nairaland' AND url ~ $1 AND url > $2 "
                    "ORDER BY url LIMIT $3", pattern, last, args.batch)
                if not rows:
                    break
                last = rows[-1]["url"]
                done_rows += len(rows)
                inflight.append(loop.run_in_executor(pool, transform_batch, [tuple(r) for r in rows]))
                while len(inflight) >= args.workers * 2:
                    consume(await inflight.popleft())
                el = time.time() - t0
                print(f"\r  read {done_rows:,}  kept {st['pages']:,}  dupes {st['dupes']:,}  "
                      f"{done_rows / el:,.0f} rows/s", end="", file=sys.stderr, flush=True)
            while inflight:
                consume(await inflight.popleft())
    finally:
        pool.shutdown()
        await con.close()

    files = writer.close()
    if args.compat_python:
        compat_rewrite(files, args.compat_python)
    print(f"\nwrote {len(files)} shards in {(time.time() - t0) / 60:.1f} min", file=sys.stderr)

    stats = {
        "pages": st["pages"], "posts": st["posts"], "chars": st["chars"],
        "authors": len(authors), "duplicates_removed": st["dupes"],
        "skipped": dict(st["skipped"]), "pii_replaced": dict(st["pii"]),
        "boards": dict(st["boards"].most_common()),
        "board_posts": dict(st["board_posts"].most_common()),
        "posts_per_year": dict(sorted(st["years"].items())),
        "lang_posts": dict(st["langs_posts"].most_common()),
        "lang_chars": dict(st["langs_chars"].most_common()),
        "rows_dropped_unwritable": writer.bad,
        "shards": len(files),
        "bytes": sum(os.path.getsize(f) for f in files),
        "built_at": dt.date.today().isoformat(),
    }
    with open(os.path.join(out, "stats.json"), "w") as f:
        json.dump(stats, f, indent=2)
    with open(os.path.join(out, "README.md"), "w") as f:
        f.write(card(stats, args))
    print(json.dumps({k: v for k, v in stats.items() if not isinstance(v, dict)}, indent=2))


# ---- dataset card -------------------------------------------------------------

LANG_NAMES = {
    "eng_Latn": "English", "pcm_Latn": "Nigerian Pidgin", "yor_Latn": "Yoruba",
    "hau_Latn": "Hausa", "ibo_Latn": "Igbo", "fra_Latn": "French", "efi_Latn": "Efik",
    "ibb_Latn": "Ibibio", "tiv_Latn": "Tiv", "urh_Latn": "Urhobo", "bin_Latn": "Edo",
    "fuv_Latn": "Fulfulde", "ful_Latn": "Fulah", "ijc_Latn": "Izon", "kcg_Latn": "Tyap",
    "arb_Arab": "Arabic", "spa_Latn": "Spanish", "deu_Latn": "German", "und": "undetermined",
}
ISO = {"eng_Latn": "en", "pcm_Latn": "pcm", "yor_Latn": "yo", "hau_Latn": "ha", "ibo_Latn": "ig",
       "fra_Latn": "fr", "efi_Latn": "efi", "ibb_Latn": "ibb", "tiv_Latn": "tiv", "urh_Latn": "urh",
       "bin_Latn": "bin", "fuv_Latn": "fuv", "ful_Latn": "ff", "ijc_Latn": "ijc", "kcg_Latn": "kcg",
       "arb_Arab": "ar", "spa_Latn": "es", "deu_Latn": "de"}


def _size_cat(n):
    for lim, name in [(1e3, "n<1K"), (1e4, "1K<n<10K"), (1e5, "10K<n<100K"),
                      (1e6, "100K<n<1M"), (1e7, "1M<n<10M"), (1e8, "10M<n<100M")]:
        if n < lim:
            return name
    return "n>100M"


# hub license id -> (display name, url). the license covers our compilation work,
# never the posts themselves, which stay their authors'
LICENSES = {
    "cc-by-nc-4.0": ("CC BY-NC 4.0", "https://creativecommons.org/licenses/by-nc/4.0/"),
    "cc-by-nc-sa-4.0": ("CC BY-NC-SA 4.0", "https://creativecommons.org/licenses/by-nc-sa/4.0/"),
    "cc-by-4.0": ("CC BY 4.0", "https://creativecommons.org/licenses/by/4.0/"),
    "odc-by": ("ODC-By 1.0", "https://opendatacommons.org/licenses/by/1-0/"),
}


def license_section(lic):
    name, url = LICENSES.get(lic, (lic, None))
    named = f"[{name}]({url})" if url else name
    return (
        f"The compilation (the selection, cleaning, pseudonymization and structure) is\n"
        f"released under {named}. The posts themselves remain the property of their\n"
        f"authors, and this license grants no rights to them beyond what the access terms\n"
        f"above allow: non-commercial research, with no attempt to re-identify anyone.\n"
        f"Nairaland's own terms of service also apply to the original content."
    )


def card(s, args):
    hf = f"{args.hf_user}/{args.repo_name}"
    total_lc = sum(v for k, v in s["lang_chars"].items() if k != "und") or 1
    langs = [k for k, v in s["lang_chars"].items() if k != "und" and v / total_lc >= 0.001]
    iso = sorted({ISO[k] for k in langs if k in ISO} | {"en"})

    lang_rows = "\n".join(
        f"| {LANG_NAMES.get(k, k)} (`{k}`) | {s['lang_posts'][k]:,} | {100 * v / total_lc:.2f}% |"
        for k, v in list(s["lang_chars"].items())[:12] if k != "und")
    board_rows = "\n".join(
        f"| {b} | {n:,} | {s['board_posts'].get(b, 0):,} |" for b, n in list(s["boards"].items())[:25])
    years = {y: n for y, n in s["posts_per_year"].items() if "2005" <= y <= str(dt.date.today().year)}
    year_rows = "\n".join(f"| {y} | {n:,} |" for y, n in years.items())
    pii = s["pii_replaced"]

    return f"""---
pretty_name: Nairaland Conversations
license: {args.license}
language:
{chr(10).join("  - " + c for c in iso)}
multilinguality: multilingual
size_categories:
  - {_size_cat(s["pages"])}
task_categories:
  - text-generation
tags:
  - nigeria
  - nigerian-english
  - nigerian-pidgin
  - forum
  - conversational
  - african-languages
configs:
  - config_name: default
    data_files:
      - split: train
        path: data/train-*.parquet
extra_gated_prompt: >-
  This dataset contains public forum posts by real people. Usernames are
  pseudonymized and contact details are scrubbed, but the text is otherwise
  as written. By requesting access you agree to use it for research only, to
  not attempt to re-identify any author, and to honor removal requests.
extra_gated_fields:
  Name: text
  Affiliation: text
  Intended use: text
  I agree to use this dataset for research only and not to re-identify authors: checkbox
---

# Nairaland Conversations

{s["pages"]:,} thread pages and {s["posts"]:,} posts from [Nairaland](https://www.nairaland.com),
the largest Nigerian discussion forum, with the thread structure kept intact: who
posted, when, and who they were quoting. About {s["chars"] / 1e9:.1f} billion characters of
Nigerian English, Nigerian Pidgin and code-switched conversation, the kind of text
that is badly underrepresented in public training data.

| | |
|---|---|
| thread pages | {s["pages"]:,} |
| posts | {s["posts"]:,} |
| characters (post text) | {s["chars"]:,} |
| distinct (pseudonymous) authors | {s["authors"]:,} |
| parquet shards | {s["shards"]} ({s["bytes"] / 1e9:.1f} GB, zstd) |
| built | {s["built_at"]} |

## Loading

```python
from datasets import load_dataset

ds = load_dataset("{hf}", split="train", streaming=True)
for page in ds.take(1):
    print(page["title"], page["board"])
    for post in page["posts"]:
        print(post["author_id"], post["posted_at"], post["text"][:80])
```

## Structure

One row per thread page (a long thread spans several pages).

| field | type | notes |
|---|---|---|
| `id` | string | hash of the canonical url |
| `url` | string | source page, kept for provenance and removal requests |
| `thread_id` | int | nairaland topic id, null for the few pages reached only via a post permalink |
| `page` | int | 0-based page of the thread |
| `board` | string | forum section (Politics, Romance, ...), parsed from the page title |
| `title` | string | thread title |
| `lang` | string | dominant language of the page, by characters |
| `n_posts` / `n_chars` | int | post count, total post text length |
| `crawled_at` | string | date the page was fetched |
| `text` | string | the whole page flattened as `author_id:` then the post, one block per post |
| `posts` | list | per post: `index`, `author_id`, `posted_at`, `edited_at`, `lang`, `text`, `quotes` |

`quotes` holds what a post replied to: the quoted author's pseudonym, the nairaland
post id that was quoted, and the quoted text. Quoted text is removed from the post's
own `text`, so a post's words stand alone. Timestamps are local time as nairaland
shows it (WAT, UTC+1); where the site omitted the year or date, it was filled in
from the crawl date.

## Languages

Tagged per post with [GlotLID](https://huggingface.co/cis-lmu/glotlid), on the first 400
characters. Posts under 20 characters are not tagged. GlotLID often confuses Nigerian
Pidgin with other English-based creoles (Jamaican, Cameroonian, Krio), so those labels
are folded into `pcm_Latn`, and predictions outside the languages you'd expect on a
Nigerian forum, or below 0.3 confidence, are marked `und`. Short, code-switched text is
hard for any language id model, so treat these as estimates, especially English vs Pidgin.

| language | posts | share of text |
|---|---|---|
{lang_rows}

## Boards (top 25 by pages)

| board | pages | posts |
|---|---|---|
{board_rows}

## Posts per year

| year | posts |
|---|---|
{year_rows}

## Collection

Crawled with [WebHarvester]({_repo_url()}), an open source, resumable, distributed crawler
built on Crawlee. The crawl was seeded at 15 discussion boards, stayed on the
nairaland domain, and followed links to a depth of 4. It skipped login, posting,
search, marketplace and advertising pages, and user profiles. Requests were throttled
per domain and honored HTTP 429 backoff.

## Cleaning and privacy

- Only thread pages are included. Profile pages, login pages and per-user post
  listings were removed ({sum(s["skipped"].values()):,} pages skipped as empty or unusable).
- {s["duplicates_removed"]:,} duplicate pages were removed (mostly post permalinks that
  resolve to a thread page already in the set).
- Usernames are replaced with stable pseudonyms (`u_` plus 12 hex characters, a keyed
  hash). The same user has the same id across the dataset, so conversations and
  reply chains stay intact, but the key is not published. Every @handle in post text,
  and bare references to usernames on the same page, are replaced with the same keyed
  pseudonyms ({pii.get("mention", 0):,} replacements), so a user mentioned on another page
  still maps to their one id.
- Email addresses ({pii.get("email", 0):,}), phone numbers ({pii.get("phone", 0):,}), BlackBerry
  Messenger PINs ({pii.get("pin", 0):,}) and 10 to 11 digit numbers such as bank account numbers
  ({pii.get("number", 0):,}) were replaced with `[EMAIL]`, `[PHONE]`, `[PIN]` and `[NUMBER]`.
- Links to social media profiles (Facebook, Twitter/X, Instagram, TikTok, LinkedIn,
  Telegram, WhatsApp) were replaced with `[SOCIAL]` ({pii.get("social", 0):,}).
- Leftover forum markup (`[quote]`, `[img]` and similar tags, {pii.get("markup", 0):,} of them)
  was stripped, keeping the text between the tags.

This is not perfect anonymization. People mention real names, places and life
details in their own posts, and a determined reader could link a pseudonym to a
public profile by searching for a post's text. If you find content about yourself
that you want removed, open a discussion on this page with the `url` and it will be
dropped from the next version.

## Limitations and bias

- It reflects who posts on Nairaland: mostly urban, internet-connected, and more male
  than the general population. Political and religious threads are heated, and the
  text includes insults, slurs and misinformation as written.
- Depth-limited crawl, so recent threads and the first pages of boards are
  overrepresented relative to the forum's full archive.
- Images, embedded media and formatting are not included. Posts that were only an
  image appear with empty text.
- The quote graph points at nairaland post ids, but posts in this dataset don't carry
  their own id, so a quote can be matched to its source by text and author, not by id.

## License

{license_section(args.license)}

## Citation

```bibtex
@misc{{olusola{dt.date.today().year}nairaland,
  title  = {{Nairaland Conversations: a Nigerian forum corpus with thread structure}},
  author = {{Olusola, Caleb}},
  year   = {{{dt.date.today().year}}},
  url    = {{https://huggingface.co/datasets/{hf}}}
}}
```
"""


def _repo_url():
    return "https://github.com/calebolusola/WebHarvester"


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default=os.environ.get("CRAWLER_DB"), help="postgres dsn (default $CRAWLER_DB)")
    ap.add_argument("--out", default="~/crawlerdata/release")
    ap.add_argument("--salt-file", default="~/crawlerdata/release.salt")
    ap.add_argument("--langid", help="path to the glotlid model.bin. skip to leave lang empty")
    ap.add_argument("--limit", type=int, help="stop after this many pages (test runs)")
    ap.add_argument("--shard-mb", type=int, default=400)
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) - 2))
    ap.add_argument("--license", default="cc-by-nc-4.0", help="hub license id for the card")
    ap.add_argument("--hf-user", default="caleb-mnb", help="hugging face user/org it'll live under")
    ap.add_argument("--repo-name", default="nairaland-conversations")
    ap.add_argument("--card-only", action="store_true",
                    help="just rewrite README.md from an existing stats.json, no rebuild")
    ap.add_argument("--compat-python",
                    help="python with an older pyarrow (<=19) to rewrite shards so old readers can load them")
    ap.add_argument("--compat-only", action="store_true",
                    help="just run the --compat-python rewrite on existing shards")
    args = ap.parse_args()
    if args.compat_only:
        if not args.compat_python:
            raise SystemExit("--compat-only needs --compat-python")
        import glob
        files = sorted(glob.glob(os.path.join(os.path.expanduser(args.out), "data", "*.parquet")))
        compat_rewrite(files, args.compat_python)
        return
    if args.card_only:
        out = os.path.expanduser(args.out)
        with open(os.path.join(out, "stats.json")) as f:
            stats = json.load(f)
        with open(os.path.join(out, "README.md"), "w") as f:
            f.write(card(stats, args))
        print(f"wrote {os.path.join(out, 'README.md')}")
        return
    if not args.db:
        raise SystemExit("need --db or $CRAWLER_DB")
    asyncio.run(build(args))


if __name__ == "__main__":
    main()
