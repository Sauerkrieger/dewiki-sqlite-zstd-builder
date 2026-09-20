#!/usr/bin/env python3
"""
SymSpell dictionary builder for OfflAIn (Säule 2: offline spell correction).

Reads the EXISTING wikipedia_compressed.db STRICTLY READ-ONLY (mode=ro — the
multi-GB DB is never modified or rebuilt) and extracts word frequencies from
article titles (weighted) + lead samples. It then writes a compact binary
dictionary consumed by the app's SymSpellCorrector (mmap'ed, zero-heap lookups,
Damerau-Levenshtein distance <= 2 for the most frequent tier of words).

Binary layout (all little-endian):
    0   magic            4 B  "OSSD"
    4   version          u16  = 1
    6   flags            u16  = 0
    8   max edit dist    u8   = 2
    9   reserved         u8
    10  tier1Count       u32  words WITH full d=2 delete index (0 .. tier1Count-1)
    14  wordCount        u32
    18  deleteCount      u32  entries in the sorted u64 delete table
    22  wordsOffset      u64  absolute offset of the u32 offsets array
    30  deletesOffset    u64  absolute offset of the sorted u64 table
    38  padding          26 B (header = 64 B)
    words section:
        u32 offsets[wordCount+1]   relative to the start of the words data
        u32 freq[wordCount]        corpus frequency (titles weighted) per word
        UTF-8 lowercase word bytes, concatenated
    deletes section:
        u64 entries sorted ascending: (fnv1a32(deleteString) << 32) | wordIndex

Lookup contract (SymSpell): the app generates all delete variants of the input
token and probes the sorted table by hash. Hash collisions can only ADD
candidates (each is verified with a real Damerau-Levenshtein check against the
input token), so a collision can never produce an incorrect suggestion.

Usage:
    python scripts/build_symspell_dict.py \
        --db wikipedia_compressed.db --out app/src/main/assets/symspell.dict \
        [--max-words 250000] [--tier1 80000] [--min-freq 2] [--title-weight 5]
"""

import argparse
import os
import re
import sqlite3
import struct
import sys
from collections import Counter

MAGIC = b"OSSD"
VERSION = 1
MAX_EDIT_DISTANCE = 2
HEADER_SIZE = 64

# German-aware word tokens: must start and end with a letter; may contain
# letters, digits, hyphens and underscores inside (MX-5, Bonn-Kölner, ...).
RE_TOKEN = re.compile(r"[^\W\d_](?:[\w-]*\w)?", re.UNICODE)


def extract_words(text: str) -> list[str]:
    """Lowercased letter-anchored tokens (>= 3 chars) from one text field."""
    out = []
    for tok in RE_TOKEN.findall(text.lower()):
        if len(tok) >= 3 and len(tok) <= 28 and not tok.isdigit():
            out.append(tok)
    return out


def collect_frequencies(db_path: str, title_weight: int) -> Counter:
    """Scan titles + lead samples from the EXISTING DB (read-only) and count."""
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.text_factory = str
    cur = conn.cursor()
    freqs: Counter = Counter()
    scanned = 0
    while True:
        batch = cur.execute(
            "SELECT rowid, title, content_sample FROM articles "
            "WHERE rowid > ? ORDER BY rowid LIMIT 200000;",
            (scanned,),
        ).fetchall()
        if not batch:
            break
        for _rowid, title, sample in batch:
            for tok in extract_words(title):
                freqs[tok] += title_weight
            if sample:
                for tok in extract_words(sample):
                    freqs[tok] += 1
        scanned = batch[-1][0]
        if scanned % 400000 < 200000:
            print(f"  scanned rowid {scanned:>10,} — vocab {len(freqs):>9,}", flush=True)
    conn.close()
    return freqs


def deletes1(word: str) -> set[str]:
    return {word[:i] + word[i + 1:] for i in range(len(word))}


def deletes2(word: str) -> set[str]:
    out: set[str] = set()
    for d1 in deletes1(word):
        for i in range(len(d1)):
            out.add(d1[:i] + d1[i + 1:])
    return out


def fnv1a32(s: str) -> int:
    """32-bit FNV-1a over UTF-8 bytes."""
    h = 0x811C9DC5
    for b in s.encode("utf-8"):
        h = ((h ^ b) * 0x01000193) & 0xFFFFFFFF
    return h


def build_dictionary(freqs, out_path: str, max_words: int,
                     tier1: int, min_freq: int) -> dict:
    """Select the top-[max_words] vocabulary and write the binary dictionary.

    Accepts a Counter OR any plain {word: freq} mapping (the tests pass plain
    dicts). Returns stats for tests/logging.
    """
    ranked = sorted(freqs.items(), key=lambda kv: -kv[1])
    vocab = [(w, f) for w, f in ranked if f >= min_freq][:max_words]
    tier1 = min(tier1, len(vocab))
    if tier1 <= 0 and vocab:
        tier1 = 1

    entries: set[int] = set()
    for idx, (word, _f) in enumerate(vocab):
        variants: set[str] = {word}  # delete-0: exact-match path
        variants |= deletes1(word)
        if idx < tier1:
            variants |= deletes2(word)
        for v in variants:
            entries.add((fnv1a32(v) << 32) | idx)
    table = sorted(entries)

    words_blob = bytearray()
    offsets = [0]
    for word, _f in vocab:
        words_blob += word.encode("utf-8")
        offsets.append(len(words_blob))

    words_data_start = HEADER_SIZE + 4 * (len(vocab) + 1) + 4 * len(vocab)
    deletes_offset = words_data_start + len(words_blob)

    with open(out_path, "wb") as f:
        f.write(MAGIC)
        f.write(struct.pack("<HHBB", VERSION, 0, MAX_EDIT_DISTANCE, 0))
        f.write(struct.pack("<III", tier1, len(vocab), len(table)))
        f.write(struct.pack("<QQ", HEADER_SIZE, deletes_offset))
        f.write(b"\x00" * 26)  # header padding to 64 B
        f.write(struct.pack(f"<{len(offsets)}I", *offsets))
        f.write(struct.pack(f"<{len(vocab)}I", *(f for _w, f in vocab)))
        f.write(bytes(words_blob))
        f.write(struct.pack(f"<{len(table)}Q", *table))

    size = os.path.getsize(out_path)
    stats = {
        "words": len(vocab),
        "tier1": tier1,
        "deletes": len(table),
        "bytes": size,
        "mb": round(size / 1_000_000, 1),
    }
    print(f"dictionary: {stats['words']:,} words ({stats['tier1']:,} d=2 tier), "
          f"{stats['deletes']:,} delete entries, {stats['mb']} MB -> {out_path}")
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="Build the OfflAIn SymSpell dictionary.")
    ap.add_argument("--db", default="wikipedia_compressed.db",
                    help="existing wiki DB (opened STRICTLY read-only)")
    ap.add_argument("--out", default="app/src/main/assets/symspell.dict")
    ap.add_argument("--max-words", type=int, default=250_000)
    ap.add_argument("--tier1", type=int, default=80_000,
                    help="top-N words that get the full d=2 delete index")
    ap.add_argument("--min-freq", type=int, default=2)
    ap.add_argument("--title-weight", type=int, default=5)
    ap.add_argument("--scan-only", action="store_true",
                    help="only print frequency statistics, write nothing")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        sys.exit(f"DB not found: {args.db} (read-only scan — the DB is never rebuilt)")

    print(f"scanning {args.db} (mode=ro) ...", flush=True)
    freqs = collect_frequencies(args.db, args.title_weight)
    print(f"vocabulary: {len(freqs):,} unique words")

    if args.scan_only:
        for w, f in freqs.most_common(30):
            print(f"  {f:>9,}  {w}")
        return

    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    build_dictionary(freqs, args.out, args.max_words, args.tier1, args.min_freq)


if __name__ == "__main__":
    main()
