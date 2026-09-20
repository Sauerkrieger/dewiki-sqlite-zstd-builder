#!/usr/bin/env python3
"""
bench_zstd.py — pick the highest zstd level that is (a) safe for the phone's
plain ZSTD_decompress (window log <= 27) and (b) reasonable to build.

Streams the first N real articles out of one dump part (constant RAM),
sanitizes them exactly like the pipeline, then compresses the corpus at
levels 19..22 (>=19 uses LDM + window_log=27, production configuration).

Usage: python scripts/bench_zstd.py [N_ARTICLES] [PART]
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import zstandard as zstd  # noqa: E402

from build_db import iter_dump_pages, make_compressor, sanitize_wiki_text  # noqa: E402

DEFAULT_PART = os.path.join(
    "wiki_build", "parts", "dewiki-latest-pages-articles1.xml-p1p297012.bz2")


def collect_corpus(n: int, part: str) -> list[bytes]:
    corpus: list[bytes] = []
    for title, raw in iter_dump_pages(part):
        if not title:
            continue
        text = sanitize_wiki_text(raw or "")
        if len(text) < 200:  # skip stubs for a representative sample
            continue
        corpus.append(text.encode("utf-8"))
        if len(corpus) % 200 == 0:
            print(f"  sampled {len(corpus)} articles...", flush=True)
        if len(corpus) >= n:
            break
    return corpus


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    part = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_PART

    print(f"Sampling {n} real articles from {os.path.basename(part)} ...")
    corpus = collect_corpus(n, part)
    raw_total = sum(len(a) for a in corpus)
    print(f"Corpus: {len(corpus)} articles, {raw_total/1e6:.1f} MB uncompressed\n")

    print(f"{'level':>5} {'size MB':>9} {'vs 19':>7} {'sec':>7} {'MB/s':>7}")
    base_size = None
    for level in (19, 20, 21, 22):
        cctx = make_compressor(level)
        t0 = time.perf_counter()
        total = 0
        for article in corpus:
            total += len(cctx.compress(article))
        dt = time.perf_counter() - t0
        if level == 19:
            base_size = total
        rel = "100.0%" if base_size == total else f"{100.0*total/base_size:.1f}%"
        print(f"{level:>5} {total/1e6:>9.2f} {rel:>7} {dt:>7.1f} {raw_total/1e6/dt:>7.1f}",
              flush=True)

    print("\nAll frames use window_log<=27 (default decoder limit) -> the phone's"
          "\nplain ZSTD_decompress can decode every level shown.")


if __name__ == "__main__":
    main()
