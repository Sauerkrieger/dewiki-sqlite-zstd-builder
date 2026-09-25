#!/usr/bin/env python3
"""
build_linkgraph.py — Pass 0 for the tier filters (SPEC §4.1, filter 1).

Scans the local dewiki dump parts (the SAME files build_db.py consumes) and
builds a tiny SQLite link graph:

    linkgraph.db:  targets(h INTEGER PRIMARY KEY, indegree INTEGER NOT NULL)

* `h` is the fnv1a32 hash of the NORMALIZED title (casefold + whitespace
  collapse — dump links and page titles differ in case/spacing).
* `indegree` counts how many MAIN-namespace pages link to that title, with
  REDIRECTS RESOLVED (a link to a redirect credits the redirect's target;
  redirect chains and cycles are handled). Redirect sources are NOT counted
  as linkers.

Why: incoming links are Wikipedia's own importance signal — authors literally
link what matters. The tier filter uses it as  in-degree < 4 AND < 300 words
-> Tier 2 (niche), which catches one-season footballers, railway stops,
club/parish stubs and episode articles whose TITLES match everyday query
words and would otherwise poison the RAG candidate window.

RAM-safe: one binary u32-array per part for the link hashes, one text file
for unique normalized titles, one for redirects. Nothing large is held in
RAM except the redirect map (resolved in one pass) and the Counter.

Usage:
    python scripts/build_linkgraph.py --parts-dir wiki_build/parts \
        --out wiki_build/linkgraph.db [--workers 8]

Resume: finished parts are skipped (sentinel files). Re-run after an
interrupted build; only `--out` is written at the end.

Verify a few titles after building:
    python scripts/build_linkgraph.py --parts-dir wiki_build/parts \
        --out wiki_build/linkgraph.db --check "Berlin,Lockpicking,Zweiter Weltkrieg"
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
import bz2
import array
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

FNV_OFFSET = 0x811C9DC5
FNV_PRIME = 0x01000193
MASK32 = 0xFFFFFFFF

RE_REDIRECT = re.compile(r"^\s*#\s*(?:redirect|weiterleitung)\b[^\[]*\[\[([^\]|#]+)",
                         re.IGNORECASE)


def fnv1a32(s: str) -> int:
    """FNV-1a 32-bit over the UTF-8 bytes (same scheme as the SymSpell dict)."""
    h = FNV_OFFSET
    for b in s.encode("utf-8"):
        h ^= b
        h = (h * FNV_PRIME) & MASK32
    return h


def norm_title(s: str) -> str:
    """Normalize a title/link the way MediaWiki resolves them: underscores ->
    spaces, whitespace collapse, and ONLY THE FIRST CHARACTER case-insensitive.

    Full casefold was WRONG: it collides case-variant pages — every [[Mond]]
    link then followed the "MOND" redirect (MOND theory) and the real article
    "Mond" ended up with indegree 0 while the theory article absorbed all its
    links. MediaWiki treats only the initial character insensitively
    ([[berlin]] -> "Berlin"), the rest stays case-sensitive ("MOND" != "Mond").
    """
    s2 = re.sub(r"\s+", " ", (s or "").replace("_", " ").strip())
    return s2[:1].upper() + s2[1:] if s2 else s2


def _local(tag) -> str:
    return tag.rsplit("}", 1)[-1].lower() if isinstance(tag, str) else ""


def _child(elem, name):
    for c in elem:
        if _local(c.tag) == name:
            return c
    return None


def _safe_text(elem) -> str:
    parts: list[str] = []

    def walk(e) -> None:
        if e.text:
            parts.append(e.text)
        for child in e:
            walk(child)
            if child.tail:
                parts.append(child.tail)

    if elem is None:
        return ""
    walk(elem)
    return "".join(parts)


RE_ANY_WIKILINK = re.compile(r"\[\[([^\[\]]+)\]\]")


def iter_pages(path: str):
    """Yield (title, is_redirect, redirect_target, link_targets) per main-ns page."""
    opener = bz2.open if path.endswith(".bz2") else open
    with opener(path, "rb") as fh:
        context = ET.iterparse(fh, events=("end",))
        for _event, elem in context:
            if _local(elem.tag) != "page":
                continue
            ns_elem = _child(elem, "ns")
            if ns_elem is None or (ns_elem.text or "0").strip() != "0":
                elem.clear()
                continue
            title_elem = _child(elem, "title")
            title = (title_elem.text or "").strip() if title_elem is not None else ""
            revision = _child(elem, "revision")
            raw = _safe_text(_child(revision, "text")) if revision is not None else ""
            m = RE_REDIRECT.match(raw[:300])
            if m:
                yield title, True, m.group(1), []
            else:
                targets = []
                for lm in RE_ANY_WIKILINK.finditer(raw):
                    target = lm.group(1).split("|", 1)[0].strip()
                    if target and ":" not in target.split("#")[0]:
                        # drop fragment, skip namespace links (File:, Kategorie:, ...)
                        targets.append(target.split("#")[0].strip())
                yield title, False, None, targets
            elem.clear()


def scan_part(args: tuple[str, str]) -> str:
    """Worker: write links.bin (u32 target hashes), titles.txt, redirects.txt."""
    path, work_dir = args
    base = os.path.basename(path)
    links_fp = os.path.join(work_dir, base + ".links.bin")
    titles_fp = os.path.join(work_dir, base + ".titles.txt")
    redir_fp = os.path.join(work_dir, base + ".redirects.txt")

    if os.path.exists(links_fp) and os.path.exists(titles_fp) and os.path.exists(redir_fp):
        return base + " (skipped)"

    hashes = array.array("I")
    seen_titles: set[int] = set()
    redirects: list[str] = []
    with open(titles_fp, "w", encoding="utf-8") as tfh, \
            open(redir_fp, "w", encoding="utf-8") as rfh:
        for title, is_redir, redir_target, targets in iter_pages(path):
            nt = norm_title(title)
            if not nt:
                continue
            h = fnv1a32(nt)
            if h not in seen_titles:
                seen_titles.add(h)
                tfh.write(f"{h:08x}\t{title}\n")
            if is_redir:
                rt = norm_title(redir_target)
                if rt:
                    redirects.append(f"{nt}\t{rt}\n")
            else:
                for t in targets:
                    ntarget = norm_title(t)
                    if ntarget and ntarget != nt:
                        hashes.append(fnv1a32(ntarget))
        with open(links_fp, "wb") as lfh:
            hashes.tofile(lfh)
        rfh.writelines(redirects)
    return base + f" ({len(seen_titles):,} titles, {len(hashes):,} links, {len(redirects):,} redirects)"


def resolve_redirect_chain(redir_map: dict[int, int], start: int) -> int:
    """Follow a redirect chain to its final target (cycle-safe, max 16 hops)."""
    seen = {start}
    cur = start
    for _ in range(16):
        nxt = redir_map.get(cur)
        if nxt is None or nxt in seen:
            break
        seen.add(nxt)
        cur = nxt
    return cur


def build_db(out_path: str, work_dir: str) -> None:
    """Resolve redirects, count in-degrees, write the final linkgraph.db."""
    redir_map: dict[int, int] = {}
    title_by_hash: dict[int, str] = {}
    for name in sorted(os.listdir(work_dir)):
        if name.endswith(".redirects.txt"):
            with open(os.path.join(work_dir, name), encoding="utf-8") as fh:
                for line in fh:
                    src, _, dst = line.rstrip("\n").partition("\t")
                    redir_map[fnv1a32(src)] = fnv1a32(dst)
        elif name.endswith(".titles.txt"):
            with open(os.path.join(work_dir, name), encoding="utf-8") as fh:
                for line in fh:
                    h, _, title = line.rstrip("\n").partition("\t")
                    title_by_hash.setdefault(int(h, 16), title)

    print(f">> {len(redir_map):,} redirects, {len(title_by_hash):,} unique titles — counting ...")
    counts: Counter[int] = Counter()
    for name in sorted(os.listdir(work_dir)):
        if not name.endswith(".links.bin"):
            continue
        hashes = array.array("I")
        with open(os.path.join(work_dir, name), "rb") as fh:
            hashes.fromfile(fh, os.path.getsize(os.path.join(work_dir, name)) // hashes.itemsize)
        for h in hashes:
            counts[resolve_redirect_chain(redir_map, h)] += 1

    if os.path.exists(out_path):
        os.remove(out_path)
    conn = sqlite3.connect(out_path)
    conn.executescript(
        "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;"
        "CREATE TABLE targets (h INTEGER PRIMARY KEY, indegree INTEGER NOT NULL);")
    conn.executemany("INSERT INTO targets (h, indegree) VALUES (?, ?)",
                     counts.items())
    conn.commit()
    # Human-readable sidecar: title for every counted target (for reports).
    with open(out_path + ".titles.tsv", "w", encoding="utf-8") as fh:
        for h, deg in sorted(counts.items(), key=lambda kv: -kv[1]):
            fh.write(f"{h:08x}\t{deg}\t{title_by_hash.get(h, '?')}\n")

    zero = sum(1 for _ in title_by_hash if _ not in counts)
    degs = sorted(counts.values())
    if degs:
        n = len(degs)
        pct = lambda p: degs[min(n - 1, int(n * p))]
        print(f">> {n:,} linked targets | indegree p10={pct(0.10)} p50={pct(0.50)} "
              f"p90={pct(0.90)} p99={pct(0.99)} | unlinked titles: {zero:,}")
    conn.close()
    print(f">> {out_path} ({os.path.getsize(out_path) / 1e6:.1f} MB)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--parts-dir", default=os.path.join("wiki_build", "parts"))
    ap.add_argument("--out", default=os.path.join("wiki_build", "linkgraph.db"))
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) - 2))
    ap.add_argument("--check", default="",
                    help="comma-separated titles: print their indegree and exit")
    ap.add_argument("--work-dir", default=None)
    args = ap.parse_args()

    work_dir = args.work_dir or (args.out + ".work")

    # Guard: the per-part caches in work_dir are only valid for the CURRENT
    # hashing/normalization scheme. Bump when norm_title/fnv1a32 change — a
    # stale cache would silently mix two normalizations and corrupt counts.
    os.makedirs(work_dir, exist_ok=True)
    sig_fp = os.path.join(work_dir, "signature")
    signature = f"v1:{FNV_OFFSET:x}:{FNV_PRIME:x}:firstchar-norm"
    if os.path.exists(sig_fp):
        if open(sig_fp).read().strip() == signature:
            print(f">> Work dir signature ok ({work_dir})")
        else:
            print(">> Work dir signature MISMATCH — clearing stale caches")
            for f in os.listdir(work_dir):
                if f != "signature":
                    try:
                        os.remove(os.path.join(work_dir, f))
                    except OSError:
                        pass
    with open(sig_fp, "w") as fh:
        fh.write(signature + "\n")

    if args.check:
        conn = sqlite3.connect(f"file:{args.out}?mode=ro", uri=True)
        for t in args.check.split(","):
            row = conn.execute("SELECT indegree FROM targets WHERE h = ?",
                               (fnv1a32(norm_title(t)),)).fetchone()
            print(f"  {row[0]:>6}  {t}" if row else f"  (nicht verlinkt)  {t}")
        conn.close()
        return

    dumps = sorted(
        os.path.join(args.parts_dir, f) for f in os.listdir(args.parts_dir)
        if f.startswith("dewiki-") and f.endswith(".bz2") and "pages-articles" in f
    )
    if not dumps:
        sys.exit(f"No dewiki-*.bz2 dump parts found in {args.parts_dir}")
    os.makedirs(work_dir, exist_ok=True)

    print(f">> Scanning {len(dumps)} parts with {args.workers} workers -> {work_dir}")
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(scan_part, (d, work_dir)): d for d in dumps}
        for i, fut in enumerate(as_completed(futs), 1):
            print(f"  [{i}/{len(dumps)}] {fut.result()} ({time.time() - t0:.0f}s)", flush=True)

    build_db(args.out, work_dir)


if __name__ == "__main__":
    main()
