# dewiki-sqlite-zstd-builder

A high-performance Python pipeline for building a heavily compressed, fully queryable German Wikipedia SQLite database optimized for offline RAG, local LLMs, and resource-constrained edge devices (Android, Raspberry Pi, embedded).

Download Pre-built SQLite Database (~4.83 GB) on Hugging Face:
https://huggingface.co/datasets/sauerkrieger/dewiki-sqlite-zstd-fts5


## Overview & Problem Statement

Running retrieval-augmented generation (RAG) or large knowledge bases fully offline on mobile devices introduces strict hardware constraints:
- **Storage Limits:** Standard Wikipedia dumps or uncompressed SQLite databases easily exceed 15–30 GB.
- **Memory Footprint:** Heavy vector indices (HNSW) block gigabytes of RAM needed by local LLMs.
- **Decompression Overhead:** Compressing articles individually adds high frame header overhead and slow I/O.

This project solves these issues by pairing **shared Zstandard compression frames** with a **zero-redundancy SQLite FTS5 index** (Trigram-compatible) and a **two-tier importance classification system** (Tier 1 core RAG vs. Tier 2 niche articles).


## Key Features

- **Shared Zstandard Frames:** Groups 100–200 articles into single zstd frames (level 19, LDM window_log=23) — eliminates per-article frame overhead while keeping decompression RAM under 8 MB per worker.
- **Zero-Redundancy FTS5 Search:** External-content SQLite FTS5 table (`content='articles'`) for fast substring and typo-tolerant matching without duplicating raw text.
- **LinkGraph & Tier Classification (v8):** Uses a Pass 0 pre-computation (`build_linkgraph.py`) to resolve redirects and compute article in-degrees. Short (< 300 words) and unlinked (< 4 incoming links) pages, franchise fiction subpages, and taxonomy stubs are demoted to **Tier 2** (Reader-only), keeping **Tier 1** (core RAG context) pristine.
- **Advanced Data Quality & Noise Filtering:** Automatically strips PR/advertising templates (`{{Werbung}}`, `{{PR}}`), stub pages, TV episode recaps, date/year navigation pages, and reality-TV/influencer biographies, while strictly preserving historical context (`{{Veraltet}}`) and article-level scoped templates (`{{Belege fehlen|Abschnitt}}`).
- **Resumable Parallel Building:** Streams raw Wikimedia XML dumps directly (`iterparse` binary mode) with flat RAM usage; processes dump parts in parallel workers and merges them seamlessly with schema validation and resume capabilities.
- **Compact Size:** Compresses all ~2.66M articles of the German Wikipedia into a single ~4.83 GB `.db` file (13,504 chunks).


## Pre-built Database

If you don't want to build the database from raw Wikimedia dumps yourself, you can download the ready-to-use dataset:

Download `wikipedia_compressed.db` on Hugging Face:
https://huggingface.co/datasets/sauerkrieger/dewiki-sqlite-zstd-fts5


## Requirements & Installation

- Python 3.9+
- SQLite 3.34+ (with FTS5 support enabled)
- Requirements from `requirements.txt`:

```bash
pip install -r requirements.txt

```

## Quick Start / Usage

### 1. Build the Database

#### Option A: Full Automated Pipeline via Shell Script (Recommended)

This runs Pass 0 (LinkGraph building for Tier classification) followed by parallel XML dump processing and DB finalization:

```bash
bash build_wiki_db.sh

```

#### Option B: Manual Step-by-Step Build

1. **Pass 0: Build LinkGraph (Incoming Links & Redirect Resolution)**
```bash
python build_linkgraph.py --parts-dir wiki_build/parts --output wiki_build/linkgraph.db

```


2. **Pass 1: Process Dump Parts & Build SQLite Database**
```bash
python build_db.py --parts-dir wiki_build/parts --workers 6 --level 19 --chunk-size 200 --linkgraph wiki_build/linkgraph.db

```



#### Option C: Build from a Single `.xml.bz2` Dump

```bash
python build_db.py --dump dewiki-latest-pages-articles.xml.bz2 --level 19

```

#### Option D: Build a Demo Database

```bash
python build_demo_db.py preview/demo_wikipedia.db

```

### 2. Audit & Quality Tools

Audit missing or discarded articles across dumps and check scope rules:

```bash
python audit_missing_articles.py --dump dewiki-latest-pages-articles.xml.bz2

```

### 3. Querying the Database (Python Example)

```python
import sqlite3
import zstandard as zstd

conn = sqlite3.connect("wikipedia_compressed.db")

# 1. Fast FTS5 Trigram Search (Filter Tier 1 for RAG)
cur = conn.cursor()
cur.execute("""
    SELECT a.id, a.title 
    FROM fts_titles f
    JOIN articles a ON a.id = f.rowid
    WHERE f.fts_titles MATCH 'Quantenphysik' AND a.tier = 1
    LIMIT 5;
""")
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

```

## Testing

Run the test suite covering sanitize regexes, template scoping, LinkGraph, tier logic, and end-to-end parallel merging:

```bash
pytest tests/test_pipeline.py


