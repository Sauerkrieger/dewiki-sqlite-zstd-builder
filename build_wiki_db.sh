#!/usr/bin/env bash
# build_wiki_db.sh — full workstation pipeline (SPEC §7 step 1+2).
#
#   1) Download the German Wikipedia dump (14 split parts, ~8 GB, in parallel
#      lanes; skipped when already present)
#   2) Build wikipedia_compressed.db directly from the raw parts — each part is
#      sanitized + zstd-compressed in its own process (parallel, resumable),
#      then all parts are merged into the final DB
#
# Requirements: python3, pip install zstandard
# (wikiextractor is no longer required: build_db.py parses the raw XML dump.)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKDIR="${WORKDIR:-./wiki_build}"
PARTS_DIR="$WORKDIR/parts"
OUT="${1:-app/src/main/assets/wikipedia_compressed.db}"
LEVEL="${LEVEL:-19}"
WORKERS="${WORKERS:-}"

mkdir -p "$PARTS_DIR" "$(dirname "$OUT")"

# 1) Download the 14 dump parts (parallel lanes, resumable, size-checked)
PARTS=(
  dewiki-latest-pages-articles1.xml-p1p297012.bz2
  dewiki-latest-pages-articles2.xml-p297013p1262093.bz2
  dewiki-latest-pages-articles3.xml-p1262094p2762093.bz2
  dewiki-latest-pages-articles3.xml-p2762094p3376257.bz2
  dewiki-latest-pages-articles4.xml-p3376258p4876257.bz2
  dewiki-latest-pages-articles4.xml-p4876258p6115464.bz2
  dewiki-latest-pages-articles5.xml-p6115465p7615464.bz2
  dewiki-latest-pages-articles5.xml-p7615465p9115464.bz2
  dewiki-latest-pages-articles5.xml-p9115465p9261244.bz2
  dewiki-latest-pages-articles6.xml-p9261245p10761244.bz2
  dewiki-latest-pages-articles6.xml-p10761245p12261244.bz2
  dewiki-latest-pages-articles6.xml-p12261245p13761244.bz2
  dewiki-latest-pages-articles6.xml-p13761245p13942242.bz2
  dewiki-latest-pages-articles6.xml-p13761245p13972523.bz2
)
BASE="https://dumps.wikimedia.org/dewiki/latest"

download_part() {
  local f="$1"
  local expected
  expected=$(curl -sI --max-time 30 "$BASE/$f" | tr -d '\r' | awk 'tolower($1)=="content-length:"{print $2}' | tail -1)
  if [ -n "$expected" ] && [ -f "$PARTS_DIR/$f" ] && [ "$(stat -c %s "$PARTS_DIR/$f" 2>/dev/null || echo 0)" = "$expected" ]; then
    echo ">> skip (complete): $f"
    return 0
  fi
  curl -sf --retry 8 --retry-delay 5 --retry-all-errors -C - -o "$PARTS_DIR/$f" "$BASE/$f"
}
export -f download_part
export PARTS_DIR BASE

echo ">> Downloading missing dump parts (3 parallel lanes) ..."
for lane in 0 1 2; do
  (
    for i in "${!PARTS[@]}"; do
      if (( i % 3 == lane )); then download_part "${PARTS[$i]}" || exit 1; fi
    done
  ) &
done
wait
echo ">> All dump parts present."

# 2) Build the DB directly from the parts (parallel workers + merge)
BUILD_ARGS=(--parts-dir "$PARTS_DIR" --db "$OUT" --level "$LEVEL")
if [ -n "$WORKERS" ]; then
  BUILD_ARGS+=(--workers "$WORKERS")
fi
python "$SCRIPT_DIR/build_db.py" "${BUILD_ARGS[@]}"

echo ">> Done: $OUT"
echo ">> The app copies this file (and the .gguf in assets/) into filesDir on first launch."
