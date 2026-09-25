#!/usr/bin/env python3
"""
audit_missing_articles.py — finds articles the OLD quality-template filter
dropped that schema v7 ({{Belege fehlen}} scoped-banner fix) would rescue, and
checks which of them are missing from the current device DB.

Background: the pre-v7 pipeline dropped EVERY page carrying {{Belege fehlen}},
even when the banner was scoped to a single section ("Dieser Abschnitt") —
killing long, valuable articles like "Lockpicking". This tool scans the local
dewiki dump parts (wiki_build/parts/*.bz2) and reports every page that:

  * the OLD filter (schema <= 6) rejected with reason "quality-template",
  * the NEW filter (schema 7) ACCEPTS,
  * and that pass the other stages (>= min_words, link ratio) — i.e. real
    content the old pipeline needlessly discarded.

Additionally (default): every reported title is probed against the current
DB (wikipedia_compressed.db, or --db PATH) so the report shows exactly what
is missing on the device right now.

Scanning a full 8 GB dump takes a while (~16 min with 8 workers, the dumps
dominate). The scan is parallelized over the parts (--workers). Results are
written to a JSON + a readable text report in wiki_build/audit_missing/.

Usage:
    python scripts/audit_missing_articles.py                 # full audit
    python scripts/audit_missing_articles.py --min-words 200 # only long articles
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import build_db  # noqa: E402


def _scan_part(job: dict) -> list[dict]:
    """Scan one dump part for pages rescued by the schema-v7 filter change."""
    path = job["path"]
    min_words = job["min_words"]
    max_link_ratio = job["max_link_ratio"]
    rescued: list[dict] = []

    # The OLD (pre-v7) STAGE-1.6 regex, copied verbatim: every page it hit was
    # dropped. Pages rejected by the old filter but ACCEPTED by the current
    # (v7) pipeline are the audit's findings — process_page applies the new
    # rules, so pages still carrying {{Werbung}} & co. are not reported.
    old_re = re.compile(
        r"\{\{\s*(?:werbung|ad|advertisement|promotion|promotional|pr|"
        r"glaskugel|glaskugel2|recentism|recentismus|aktueller\ event|"
        r"kurzlebig|zeitgeist|belege\ fehlen|belegt\ nicht)\s*(?:\||\}\})",
        re.IGNORECASE)

    for title, body in build_db.iter_dump_pages(path):
        if not old_re.search(body):
            continue
        kept, _reason = build_db.process_page(title, body, min_words, max_link_ratio)
        if kept is not None:
            rescued.append({"title": title,
                            "words": len(build_db.sanitize_wiki_text(body).split()),
                            "chars": len(body)})
    return rescued


def _titles_in_db(db_path: str, titles: set[str]) -> set[str]:
    """Which of the titles already exist in the DB (exact match, case-sensitive)."""
    found: set[str] = set()
    if not os.path.exists(db_path):
        print(f"!! DB not found, skipping existence check: {db_path}")
        return found
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cur = conn.cursor()
    batch = sorted(titles)
    for i in range(0, len(batch), 500):
        chunk = batch[i:i + 500]
        q = ("SELECT title FROM articles WHERE title IN (%s)"
             % ",".join("?" * len(chunk)))
        for (t,) in cur.execute(q, chunk):
            found.add(t)
    conn.close()
    return found


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--parts-dir", default=os.path.join("wiki_build", "parts"))
    ap.add_argument("--db", default="wikipedia_compressed.db")
    ap.add_argument("--min-words", type=int, default=50)
    ap.add_argument("--max-link-ratio", type=float, default=0.5)
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) - 2))
    ap.add_argument("--out-dir", default=os.path.join("wiki_build", "audit_missing"))
    ap.add_argument("--top", type=int, default=100,
                    help="how many rescued articles to list in the text report")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    dumps = sorted(
        os.path.join(args.parts_dir, f) for f in os.listdir(args.parts_dir)
        if f.startswith("dewiki-") and f.endswith(".bz2") and "pages-articles" in f
    )
    if not dumps:
        sys.exit(f"No dewiki-*.bz2 dump parts found in {args.parts_dir}")

    print(f">> Scanning {len(dumps)} dump parts with {args.workers} workers "
          f"(rescued-by-v7 filter) ...")
    t0 = time.time()
    rescued: list[dict] = []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_scan_part, {"path": d, "min_words": args.min_words,
                                            "max_link_ratio": args.max_link_ratio}): d
                   for d in dumps}
        for i, fut in enumerate(as_completed(futures), 1):
            part_rescued = fut.result()
            rescued.extend(part_rescued)
            print(f"  [{i}/{len(dumps)}] {os.path.basename(futures[fut])}: "
                  f"{len(part_rescued)} rescued ({time.time() - t0:.0f}s)", flush=True)

    rescued.sort(key=lambda r: -r["words"])
    json_path = os.path.join(args.out_dir, "rescued.json")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(rescued, fh, ensure_ascii=False, indent=2)

    print(f">> {len(rescued)} articles rescued by the v7 filter "
          f"({time.time() - t0:.0f}s total). Checking presence in {args.db} ...")
    in_db = _titles_in_db(args.db, {r["title"] for r in rescued})
    for r in rescued:
        r["in_current_db"] = r["title"] in in_db

    missing = [r for r in rescued if not r["in_current_db"]]
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(rescued, fh, ensure_ascii=False, indent=2)

    report = [
        "Artikel, die der alte Filter ({{Belege fehlen}} pauschal) verworfen hat",
        "und die schema v7 gerettet hätte:",
        "",
        f"  gerettet gesamt : {len(rescued)}",
        f"  davon in DB     : {len(rescued) - len(missing)}",
        f"  davon FEHLEN    : {len(missing)}",
        "",
        f"{'Wörter':>8}  {'in DB':>5}  Titel",
    ]
    for r in rescued[: args.top]:
        report.append(f"{r['words']:>8,}  {'JA' if r['in_current_db'] else 'NEIN':>5}  {r['title']}")
    if len(rescued) > args.top:
        report.append(f"  ... und {len(rescued) - args.top} weitere (siehe {json_path})")

    text_path = os.path.join(args.out_dir, "report.txt")
    with open(text_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(report) + "\n")
    print("\n".join(report))
    print(f"\n>> JSON:  {json_path}")
    print(f">> Report: {text_path}")


if __name__ == "__main__":
    main()
