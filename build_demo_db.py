#!/usr/bin/env python
"""
Builds a small demo wiki database (same schema as wikipedia_compressed.db)
containing the given demo articles, extracted from the full local DB.

Usage: python scripts/build_demo_db.py [out_path]
"""
import os
import sqlite3
import sys

import zstandard as zstd

SRC = os.path.join(os.path.dirname(__file__), "..", "wikipedia_compressed.db")
DEMOS = ["Dentin", "Quantenphysik", "Pizza", "Berliner Mauer", "Otto von Bismarck"]

# Synthetic article (not present in the source DB under this title) so the
# bookmark demo row "Quantencomputer" gets a lead preview.
SYNTHETIC = {
    "Quantencomputer": (
        "Ein Quantencomputer ist ein Rechner, dessen Funktion auf "
        "quantenmechanischen Zuständen beruht. Im Gegensatz zum klassischen "
        "Computer arbeitet er nicht mit Bits, sondern mit Qubits, die durch "
        "Superposition und Verschränkung miteinander gekoppelt sind. "
        "Quantengatter verknüpfen diese Qubits zu Schaltkreisen und bilden "
        "damit die Grundlagen der Quanteninformation und ihrer Anwendung in "
        "der Berechnung. Bisherige Quantencomputer befinden sich überwiegend "
        "im experimentellen Stadium; bekannte Algorithmen sind Shors "
        "Primfaktorzerlegung und Grover-Suche."
    ),
}


def main(out_path: str) -> None:
    if os.path.exists(out_path):
        os.remove(out_path)
    src = sqlite3.connect(SRC)
    dst = sqlite3.connect(out_path)
    c = dst.cursor()
    c.execute("CREATE TABLE chunks (id INTEGER PRIMARY KEY, data BLOB NOT NULL)")
    c.execute("""CREATE TABLE articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT UNIQUE,
        content_sample TEXT,
        chunk_id INTEGER NOT NULL,
        offset INTEGER NOT NULL,
        length INTEGER NOT NULL)""")
    c.execute("CREATE VIRTUAL TABLE fts_titles USING fts5(title, content_sample, content='articles', content_rowid='id')")
    comp = zstd.ZstdCompressor()

    for title in DEMOS:
        row = src.execute(
            "SELECT id, content_sample, chunk_id, offset, length FROM articles WHERE title = ?",
            (title,),
        ).fetchone()
        if row is None:
            print("missing in source db:", title)
            continue
        _src_id, sample, chunk_id, off, ln = row
        blob = src.execute("SELECT data FROM chunks WHERE id=?", (chunk_id,)).fetchone()[0]
        text = zstd.ZstdDecompressor().decompress(blob)
        article_text = text[off:off + ln].decode("utf-8")

        dst_chunk = comp.compress(article_text.encode("utf-8"))
        cur = c.execute("INSERT INTO chunks(data) VALUES (?)", (dst_chunk,))
        new_chunk_id = cur.lastrowid
        cur = c.execute(
            "INSERT INTO articles(title, content_sample, chunk_id, offset, length) VALUES (?,?,?,?,?)",
            (title, sample, new_chunk_id, 0, len(article_text.encode("utf-8"))),
        )
        c.execute(
            "INSERT INTO fts_titles(rowid, title, content_sample) VALUES (?,?,?)",
            (cur.lastrowid, title, sample),
        )
        print("added:", title)

    for title, text in SYNTHETIC.items():
        blob = text.encode("utf-8")
        cur = c.execute("INSERT INTO chunks(data) VALUES (?)", (comp.compress(blob),))
        cur = c.execute(
            "INSERT INTO articles(title, content_sample, chunk_id, offset, length) VALUES (?,?,?,?,?)",
            (title, text[:200], cur.lastrowid, 0, len(blob)),
        )
        c.execute(
            "INSERT INTO fts_titles(rowid, title, content_sample) VALUES (?,?,?)",
            (cur.lastrowid, title, text[:200]),
        )
        print("added (synthetic):", title)

    dst.commit()
    dst.close()
    src.close()
    print("wrote", out_path, os.path.getsize(out_path), "bytes")


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "preview", "demo_wikipedia.db")
    main(out)
