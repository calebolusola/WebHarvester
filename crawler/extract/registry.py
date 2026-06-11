from urllib.parse import urlsplit

from . import nairaland
from .base import build_record, generic_extract

# domain substring -> extractor function taking a cleaned lxml tree
_EXTRACTORS = {
    "nairaland.com": nairaland.extract,
}


def _pick(url: str):
    host = (urlsplit(url).hostname or "").lower()
    for needle, fn in _EXTRACTORS.items():
        if host == needle or host.endswith("." + needle):
            return fn
    return generic_extract


def extract_record(url: str, html: bytes, http_status: int, store_html: bool) -> dict:
    # top-level, picklable entry point for the process pool
    return build_record(url, html, http_status, store_html, _pick(url))
