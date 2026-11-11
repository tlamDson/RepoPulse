"""Download GH Archive hour files (network code, kept out of the unit-tested modules)."""

from __future__ import annotations

import os
import time
import urllib.error
import urllib.request
from pathlib import Path

from benchmarks.reference import URL_TEMPLATE

_HEADERS = {"User-Agent": "benchmark-fetch/1.0"}


def download(name: str, dest_dir: Path, *, retries: int = 3, timeout: int = 300) -> Path | None:
    """Fetch ``name`` into ``dest_dir`` atomically; return None when the server has no file."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / name
    part = dest_dir / f"{name}.part"
    url = URL_TEMPLATE.format(name=name)
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as resp, part.open("wb") as out:
                while chunk := resp.read(1 << 20):
                    out.write(chunk)
            os.replace(part, dest)
            return dest
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            if attempt == retries:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt == retries:
                raise
        time.sleep(5 * attempt)
    return None
