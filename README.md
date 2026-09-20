# dewiki-sqlite-zstd-builder

A high-performance Python pipeline for building a heavily compressed, fully queryable German Wikipedia SQLite database optimized for offline RAG, local LLMs, and resource-constrained edge devices (Android, Raspberry Pi, embedded).

Download Pre-built SQLite Database (~4.83 GB) on Hugging Face:
https://huggingface.co/datasets/sauerkrieger/dewiki-sqlite-zstd-fts5


## Overview & Problem Statement

Running retrieval-augmented generation (RAG) or large knowledge bases fully offline on mobile devices introduces strict hardware constraints:
- Storage Limits: Standard Wikipedia dumps or uncompressed SQLite databases easily exceed 15-30 GB.
- Memory Footprint: Heavy vector indices (HNSW) block gigabytes of RAM needed by local LLMs.
- Decompression Overhead: Compressing articles individually adds high frame header overhead and slow I/O.

This project solves these issues by pairing shared Zstandard compression frames with a zero-redundancy SQLite FTS5 trigram search index.


## Key Features

- Shared Zstandard Frames: Groups 100-200 articles into single zstd frames (level 19, LDM window_log=23) — eliminates per-article frame overhead while keeping decompression RAM under 8 MB per worker.
- Zero-Redundancy FTS5 Search: External-content SQLite FTS5 table (content='articles') with a trigram tokenizer for substring/typo-tolerant matching without duplicating raw text.
- Data Quality & Noise Filtering: Automatically strips PR/advertising templates, stub pages, and reality-TV/influencer biographies, while strictly preserving historical context ({{Veraltet}}).
- Resumable Parallel Building: Streams raw Wikimedia XML dumps directly (iterparse binary mode) with flat RAM usage; processes dump parts in parallel workers and merges them seamlessly.
- Compact Size: Compresses all ~2.66M articles of the German Wikipedia into a single ~4.83 GB .db file.


## Pre-built Database

If you don't want to build the database from raw Wikimedia dumps yourself, you can download the ready-to-use dataset:

Download wikipedia_compressed.db on Hugging Face:
https://huggingface.co/datasets/sauerkrieger/dewiki-sqlite-zstd-fts5


## Requirements & Installation

- Python 3.9+
- SQLite 3.34+ (with FTS5 trigram support enabled)
- zstandard Python package

pip install -r requirements.txt


## Quick Start / Usage

### 1. Build the Database

Option A: Parallel build from official split dump parts (Recommended)
python build_db.py --parts-dir wiki_build/parts --workers 6 --level 19 --chunk-size 200

Option B: Build from a single .xml.bz2 dump
python build_db.py --dump dewiki-latest-pages-articles.xml.bz2 --level 19

Option C: Build a small demo database
python build_demo_db.py preview/demo_wikipedia.db


### 2. Querying the Database (Python Example)

```python
import sqlite3
import zstandard as zstd

conn = sqlite3.connect("wikipedia_compressed.db")

# 1. Fast FTS5 Trigram Search
cur = conn.cursor()
cur.execute("SELECT rowid, title FROM fts_titles WHERE fts_titles MATCH 'Quantenphysik' LIMIT 5;")
results = cur.fetchall()

# 2. Fetch Article Metadata & Shared Chunk
rowid = results[0][0]
article = conn.execute(
    "SELECT title, chunk_id, offset, length FROM articles WHERE id = ?", (rowid,)
).fetchone()
title, chunk_id, offset, length = article

# 3. Decompress ONLY the Containing Chunk
blob = conn.execute("SELECT data FROM chunks WHERE id = ?", (chunk_id,)).fetchone()[0]
decompressed_chunk = zstd.ZstdDecompressor().decompress(blob)
article_text = decompressed_chunk[offset:offset + length].decode("utf-8")

print(f"=== {title} ===")
print(article_text[:300])
