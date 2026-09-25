#!/usr/bin/env python3
"""
build_db.py — Offline Wikipedia RAG database builder (SPEC §4.1 + §6).

Staged pipeline, executed strictly in this order:

    STAGE 1  Direct-stream parsing & coarse pre-filter
             * stream the bz2 dumps with ElementTree.iterparse in *binary* bz2
               mode; keep RAM flat by clearing the accumulated tree.
             * main namespace only (ns=0) + title prefix filter
               (Datei/File/Kategorie/Vorlage/Wikipedia/Portal/Diskussion/…).
             * drop redirects (#REDIRECT / #WEITERLEITUNG) and disambiguation
               pages ([[Kategorie:Begriffsklärung]]).
             * drop "Liste von …" / "Liste der …" title articles.
             * drop {{Stub}} / {{Substub}} articles.
             * drop quality/relevance-template pages: {{Werbung}}, {{PR}},
               {{Glaskugel}}, {{Recentism}} …
               ({{Veraltet}} is explicitly EXEMPT: historical value).
               {{Belege fehlen}} drops ONLY at article level (bare banner in
               the lead, or a scope parameter naming no section/table); a
               section-scoped banner ({{Belege fehlen|…|Dieser Abschnitt}})
               marks one part and keeps the page (schema v7).
               {{Zukunft}} only drops the page as a LINE-ANCHORED banner;
               inline {{Zukunft|2030}} date notes (infobox "nächste Wahl")
               are harmless and must NOT drop flagships like Berlin/München.
             * drop B-/Z-prominence & Reality-TV/internet-phenomenon
               biographies (title patterns + promo category links).
             * STAGE 1.8 (v8) junk filters: year/date titles, episode/recap
               titles, deletion banners ({{SLA}}/{{URV}}), disguised BKLs
               ("X steht für:") and extended list-title patterns drop the
               page; niche-but-valid content is demoted to Tier 2 (see
               decide_tier) — the RAG candidate search only sees Tier 1.

    STAGE 2  Regex sanitization (sanitize_wiki_text)
             * strip images/categories/<ref>…</ref>/{{templates}}/HTML tags/
               comments/external URLs/'''bold'''/''italic''/tables/bare URLs.
             * unwrap [[Link|Label]] -> Label and [url Label] -> Label.
             * PRESERVE paragraphs (\\n\\n), == headings == and bullet lists.
             * strip appendix sections: Einzelnachweise, Literatur, Weblinks,
               Siehe auch (with their sub-sections).
             * decode HTML entities (&amp; &lt; &gt; &quot; &nbsp; …) into
               clean UTF-8 (html.unescape) before the markup regexes.
             * remove IPA/phonetic brackets and stray geo-coordinate metadata.
             * clean leftover table pipe cascades (`|`, `||`, `|-`).
             * fold NBSP into plain spaces, collapse repeated spaces and cap
               cascading linebreaks at \n\n.

    STAGE 3  Content & quality filter (after cleaning)
             * fewer than --min-words (default 50) words  -> drop.
             * content-link / word ratio above --max-link-ratio (default 0.5)
               (pure navigation bars / family trees)     -> drop.

    STAGE 4  Chunked compression, storage & FTS5 indexing
             * group 100..200 accepted articles into a *shared* zstd frame
               (level 19, long-distance matching, window log 23) — removes the
               per-article frame overhead and maximises entropy density.
             * store the chunks in `chunks`; each `articles` row references its
               slice via (chunk_id, offset, length).
             * build `fts_titles` over (title, first 300 chars of the lead) as
               an EXTERNAL CONTENT table (`content='articles'`) so the index
               never duplicates the stored text; rank with
               bm25(fts_titles, 10.0, 1.0).
             * finish with `fts_titles optimize` + `VACUUM`.

Schema (SCHEMA_VERSION 5 — trigram FTS):
    chunks(id INTEGER PRIMARY KEY, data BLOB)
    articles(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT UNIQUE,
             content_sample TEXT, chunk_id INTEGER, offset INTEGER, length INTEGER)
    fts_titles USING fts5(title, content_sample,
                          content='articles', content_rowid='id')

Input modes:
    --parts-dir DIR         directory with the official split dump parts
                            (dewiki-latest-pages-articlesN.xml-p*.bz2).
                            Each part is processed in its own process (parallel
                            bz2 + zstd), producing resumable per-part DBs that
                            are merged into the final database at the end.
    --dump FILE.xml.bz2     single official Wikimedia XML dump, streamed with
                            iterparse (constant RAM).
    --extracted DIR         output of `wikiextractor --no-templates` (files
                            containing <doc ...>).

Usage:
    python build_db.py --parts-dir wiki_build/parts --workers 6 --level 19
    python build_db.py --dump dewiki-latest-pages-articles.xml.bz2 --level 19
    python build_db.py --extracted extracted_wiki/ --db out.db --chunk-size 200
"""

from __future__ import annotations

import argparse
import bz2
import html
import os
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ProcessPoolExecutor, as_completed

try:
    import zstandard as zstd
except ImportError:
    sys.exit("Missing dependency: pip install zstandard")


# Bump whenever the on-disk schema OR the pipeline filters change: stale
# per-part DBs (built with an older layout/filters) are then rebuilt instead of
# being silently merged.
# History: 2 = chunked zstd + external-content FTS5; 3 = STAGE 1 quality-
# relevance/template + promo filters, STAGE 2 entity/IPA/coordinate/pipe cleaning;
# 4 = one-off double-escape repair (fix_double_escapes.py stamp);
# 5 = FTS5 `tokenize='trigram'` (substring + typo-tolerant matching, needs
#     SQLite >= 3.34; full FTS rebuild required — the tokenizer is baked into
#     the index at INSERT time and cannot be changed in place).
# 6 = {{Zukunft}} no longer drops whole articles when used as an inline date
#     note — only a line-anchored banner is disqualifying. This was killing
#     every article whose infobox has a "nächste Wahl" row (Berlin, München,
#     Hamburg, Wien, Stuttgart, Bremen, Bayern, ...).
# 7 = {{Belege fehlen}} drops pages only at ARTICLE level: the template doc
#     (Vorlage:Belege fehlen) defines parameter 2 as the SCOPE ("Bezug") — a
#     banner carrying "Dieser Abschnitt"/"Die folgenden Abschnitte"/"die
#     folgende Tabelle", or a bare banner placed below a == heading ==, marks
#     ONE part of the page. The whole-page drop was killing long, valuable
#     articles like "Lockpicking" (4,379 words; one section banner under
#     == Sperrelemente ==).
# 8 = tier filters (RAG hygiene): junk hard-drops (year/date titles, episode
#     recap titles, deletion banners {{SLA}}/{{URV}}, disguised BKLs ("X steht
#     für:"), extended list-title patterns) plus the `articles.tier` column
#     (1 = RAG-visible core, 2 = Reader-only niche) driven by the in-degree
#     link graph (scripts/build_linkgraph.py): short (< 300 words) AND
#     unlinked (< 4 incoming links) -> Tier 2, plus demotions for franchise
#     fiction and taxonomy stubs; >= 2000 words are ALWAYS Tier 1. The chat
#     candidate search filters tier = 1 (db_jni.cpp falls back gracefully on
#     legacy DBs without the column).
SCHEMA_VERSION = 8

# ---------------------------------------------------------------- STAGE 2: sanitization

RE_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
RE_IMAGE_LINK = re.compile(r"\[\[(Datei|File|Image|Abbildung|Bild):[^\]]*\]\]", re.IGNORECASE)
RE_CATEGORY_LINK = re.compile(r"\[\[(Kategorie|Category):[^\]]*\]\]", re.IGNORECASE)
RE_REF_BLOCK = re.compile(r"<ref[^>/]*>.*?</ref\s*>", re.DOTALL | re.IGNORECASE)
RE_REF_SELF = re.compile(r"<ref[^>]*/>", re.IGNORECASE)
RE_TEMPLATE = re.compile(r"\{\{(?:[^{}]|\{[^{}]*\})*\}\}", re.DOTALL)
RE_HTML_TAG = re.compile(r"</?[a-zA-Z][^>]*>")
RE_WIKILINK = re.compile(r"\[\[(?:[^\[\]|]*\|)?([^\[\]|]+)\]\]")
RE_EXTERNAL_LINK = re.compile(r"\[(?:https?|ftp)://\S+\s+((?!(?:https?|ftp)://)[^\]]+)\]")
RE_EXTERNAL_URL_LABEL = re.compile(r"\[(?:https?|ftp)://\S+\s+(?:https?|ftp)://\S+\]")
RE_EXTERNAL_BARE = re.compile(r"\[(?:https?|ftp)://\S+\]")
RE_LEFTOVER_URL = re.compile(r"(?<![\w\"])(?:https?|ftp)://\S+")
RE_BOLD_ITALIC = re.compile(r"'{2,5}")
RE_TABLE = re.compile(r"^\{\|.*?\|\}$", re.DOTALL | re.MULTILINE)
# STAGE 2.4: leftover table markup after the table strip — pipe-only lines,
# `|-` row breaks (incl. attributes), leading/trailing pipes of surviving cell
# lines and mid-line cell separators between words.
RE_TABLE_ROW_JUNK = re.compile(r"^[ \t]*\|+[ \t]*$", re.MULTILINE)
RE_TABLE_ROW_BREAK = re.compile(r"^[ \t]*\|-+.*$", re.MULTILINE)
RE_PIPE_LINE_START = re.compile(r"^[ \t]*\|+[ \t]*", re.MULTILINE)
RE_PIPE_LINE_END = re.compile(r"[ \t]*\|+[ \t]*$", re.MULTILINE)
RE_PIPE_MID = re.compile(r"\|+")
# STAGE 2.2: IPA/phonetic brackets — "[ˈʔaːbə]", "[alˈbɛʁt, ˈbɛːɐ̯]" — anchored
# on stress marks so ordinary bracketed text is never touched. Geo coordinates:
# `{{Koordinate …}}`/`{{Coordinate …}}` templates (possibly nested braces) and
# stray degree-notation coordinate pairs ("52° 31′ N, 13° 24′ O").
RE_IPA = re.compile(r"\[[^\[\]\n]{0,120}?[ˈˌ][^\[\]\n]{0,120}?\]")
RE_COORD_TEMPLATE = re.compile(
    r"\{\{\s*(?:koordinate|coordinate|coord)[^{}]*(?:\{[^{}]*\}[^{}]*)*\}\}",
    re.IGNORECASE)
RE_COORD_TEXT = re.compile(
    r"\b\d{1,3}\s*°\s*\d{1,2}(?:[.,]\d+)?\s*[′']?(?:\s*\d{1,2}(?:[.,]\d+)?\s*[″\"]?)?\s*[NS]?"
    r"(?:\s*[,;/]\s*|\s+)"
    r"\d{1,3}\s*°\s*\d{1,2}(?:[.,]\d+)?\s*[′']?(?:\s*\d{1,2}(?:[.,]\d+)?\s*[″\"]?)?\s*[OEOW]?\b"
    r"|\b\d{1,3}(?:[.,]\d+)?\s*°\s*[NS]\s*[,;/]?\s*\d{1,3}(?:[.,]\d+)?\s*°\s*[EOW]\b")
RE_MULTI_SPACE = re.compile(r"[ \t]{2,}")

# == Heading == / === Sub-heading === (level = number of '=').
RE_HEADING = re.compile(r"^(={2,6})\s*(.*?)\s*\1\s*$")

# Appendix sections removed wholesale (SPEC STAGE 2.4).
APPENDIX_SECTIONS = {"einzelnachweise", "literatur", "weblinks", "siehe auch"}


def strip_appendix_sections(text: str) -> str:
    """Drop the appendix sections (with their sub-sections).

    A section starts at a heading whose title is in APPENDIX_SECTIONS and runs
    until the next heading of the same or a higher level; deeper sub-headings
    belong to the dropped section.
    """
    out: list[str] = []
    skip_level = 0
    for line in text.splitlines():
        match = RE_HEADING.match(line.strip())
        if match:
            level = len(match.group(1))
            name = match.group(2).strip().lower()
            if skip_level:
                if level > skip_level:
                    continue  # sub-heading inside a dropped section
                skip_level = 0  # same/higher level: the dropped section ends
            if name in APPENDIX_SECTIONS:
                skip_level = level
                continue
            out.append(line)
        elif skip_level:
            continue
        else:
            out.append(line)
    return "\n".join(out)


def sanitize_wiki_text(text: str) -> str:
    """SPEC STAGE 2: strip clutter, preserve paragraphs & headings for the Reader."""
    text = RE_COMMENT.sub("", text)
    # STAGE 2.1: decode HTML entities into clean UTF-8 BEFORE the markup
    # regexes, so escaped markup (&lt;ref&gt;, &lt;tag&gt;) is still recognized
    # and the stored text carries no entity byte overhead. The dump contains
    # double-escaped artifacts (&amp;amp;), so decode repeatedly until stable.
    for _ in range(5):
        decoded = html.unescape(text)
        if decoded == text:
            break
        text = decoded
    text = RE_IMAGE_LINK.sub("", text)
    text = RE_CATEGORY_LINK.sub("", text)
    text = RE_REF_BLOCK.sub("", text)
    text = RE_REF_SELF.sub("", text)
    text = RE_TEMPLATE.sub("", text)
    text = RE_HTML_TAG.sub("", text)
    # [[Link|Label]] -> Label ; [[Link]] -> Link
    text = RE_WIKILINK.sub(r"\1", text)
    # External links: [url label] -> label (unless the "label" is another URL).
    text = RE_EXTERNAL_URL_LABEL.sub("", text)
    text = RE_EXTERNAL_LINK.sub(r"\1", text)
    text = RE_EXTERNAL_BARE.sub("", text)
    text = RE_LEFTOVER_URL.sub("", text)
    text = RE_BOLD_ITALIC.sub("", text)
    text = RE_TABLE.sub("", text)
    # STAGE 2.4: clean leftover pipe cascades from stripped tables.
    text = RE_TABLE_ROW_JUNK.sub("", text)
    text = RE_TABLE_ROW_BREAK.sub("", text)
    text = RE_PIPE_LINE_START.sub("", text)
    text = RE_PIPE_LINE_END.sub("", text)
    text = RE_PIPE_MID.sub(" ", text)
    # STAGE 2.2: phonetic/IPA brackets and isolated geo-coordinate metadata.
    text = RE_IPA.sub(" ", text)
    text = RE_COORD_TEMPLATE.sub("", text)
    text = RE_COORD_TEXT.sub(" ", text)
    text = strip_appendix_sections(text)
    # STAGE 2.3: fold NBSP (&nbsp; / \u00a0) into plain spaces, collapse
    # repeated spaces and preserve double linebreaks (paragraph structure) —
    # the paragraph join below caps cascading linebreaks at exactly \n\n.
    text = text.replace("\u00a0", " ")
    lines = [RE_MULTI_SPACE.sub(" ", ln.strip()) for ln in text.splitlines()]
    paragraphs: list[str] = []
    buf: list[str] = []
    for ln in lines:
        if ln.strip():
            buf.append(ln.strip())
        elif buf:
            paragraphs.append("\n".join(buf))
            buf = []
    if buf:
        paragraphs.append("\n".join(buf))
    return "\n\n".join(paragraphs).strip()


# ---------------------------------------------------------------- STAGE 1: coarse filter

# Non-article titles that must never enter the DB (belt-and-suspenders;
# real dumps already carry these in non-zero namespaces).
RE_SKIP_TITLE = re.compile(
    r"^(?:Datei|File|Image|Kategorie|Category|Vorlage|Template|Hilfe|Help|"
    r"Portal|Wikipedia|Spezial|Diskussion|Talk|Benutzer|User|Medium|MediaWiki|"
    r"Modul|Module):", re.IGNORECASE)

# Filter 5c (schema v8): more list-like title patterns beyond "Liste von/der".
RE_LIST_TITLE = re.compile(
    r"^(?:Liste|Verzeichnis|Übersicht|Index)\s+(?:von|der|des|den|dem)\b",
    re.IGNORECASE)
RE_REDIRECT = re.compile(r"^\s*#\s*(redirect|weiterleitung)\b", re.IGNORECASE)
RE_DISAMBIG = re.compile(r"\[\[\s*(?:Kategorie|Category)\s*:\s*Begriffsklärung", re.IGNORECASE)
RE_STUB_TEMPLATE = re.compile(r"\{\{\s*(?:substub|stub)\s*(?:\||\}\})", re.IGNORECASE)

# STAGE 1.6: quality/relevance template filter. Articles carrying any of these
# templates are dropped wholesale: advertising/PR, speculation about the future
# ("Glaskugel"), short-lived internet phenomena and "recentism". These are
# inherently WHOLE-ARTICLE verdicts. `{{Veraltet}}` is deliberately NOT in
# this list — outdated articles can still hold valuable historical knowledge
# (see RE_OLD_TEMPLATE). `{{Belege fehlen}}` is handled separately below: it
# is often scoped to a single section and must not drop the whole page.
RE_QUALITY_TEMPLATE = re.compile(
    r"\{\{\s*(?:"
    r"werbung|ad|advertisement|promotion|promotional|"
    r"pr|"
    r"glaskugel|glaskugel2|"
    r"recentism|recentismus|aktueller\ event|kurzlebig|zeitgeist"
    r")\s*(?:\||\}\})",
    re.IGNORECASE)

# {{Belege fehlen}} needs SEPARATE handling (schema v7): it drops the page
# only when it applies to the WHOLE article. The template doc
# (Vorlage:Belege fehlen) defines parameter 2 as the SCOPE ("Bezug"); its
# default text is "Dieser Artikel oder nachfolgender Abschnitt". A banner
# carrying a sectioning parameter 2 ("Dieser Abschnitt", "Die folgenden
# Abschnitte", "die folgende Tabelle") or placed below a == heading == marks
# only that part — whole-page drops for it were killing long, valuable
# articles ("Lockpicking": 4,379 words, one section banner under
# == Sperrelemente ==).
RE_CITE_BANNER_START = re.compile(
    r"\{\{\s*(?:belege\s*fehlen|belegt\s*nicht)\s*[|}]", re.IGNORECASE)
RE_CITE_SCOPE_SECTION = re.compile(
    r"abschnitt|absatz|absätz|tabelle", re.IGNORECASE)  # "Absätze" (umlaut plural, Haus) also scopes
RE_NAMED_PARAM = re.compile(r"\s*(?:\d+|[A-Za-z_]\w*)\s*=")
RE_FIRST_HEADING = re.compile(r"(?m)^={2,6}")


def _article_level_cite_banner(raw: str) -> bool:
    """True when a {{Belege fehlen}} banner applies to the WHOLE article.

    Walks every banner invocation (brace-matched, safe against nested
    templates), extracts its top-level parameters and decides the scope:
      * parameter 2 (positional or named `2=`) present -> article-level only
        when its text names NO section/table ("Dieser Abschnitt" etc. keep);
      * parameter 2 absent -> article-level only when the banner sits    in the article LEAD (before the first heading); below a heading the default
        text ("Dieser Artikel oder nachfolgende Abschnitt") is being used for
        that section and the page stays. A scope naming ABSÄTZE (paragraphs,
        "die folgenden beiden Absätze" — the "Haus" case) also keeps the page.
    """
    first_heading = RE_FIRST_HEADING.search(raw)
    lead_end = first_heading.start() if first_heading else len(raw)
    for m in RE_CITE_BANNER_START.finditer(raw):
        start = m.start()
        depth = 0
        i = start
        n = len(raw)
        segments: list[str] = []
        seg_start = start + 2  # behind '{{' — the first segment is the NAME
        while i < n:
            two = raw[i:i + 2]
            if two == "{{":
                depth += 1
                i += 2
                continue
            if two == "}}":
                depth -= 1
                if depth == 0:
                    segments.append(raw[seg_start:i])
                    break
                i += 2
                continue
            if raw[i] == "|" and depth == 1:
                segments.append(raw[seg_start:i])
                seg_start = i + 1
            i += 1
        if depth != 0:
            continue  # unmatched braces — ignore this invocation
        # segments[0] is the template name; the parameters follow. Named
        # parameters (2=, Plural=) are matched separately; a hint text
        # containing '=' inside prose does not match the name pattern.
        positional: list[str] = []
        named_two: str | None = None
        for seg in segments[1:]:
            if RE_NAMED_PARAM.match(seg):
                key, _, value = seg.partition("=")
                if key.strip() == "2":
                    named_two = value
            else:
                positional.append(seg)
        scope = named_two if named_two is not None else (
            positional[1] if len(positional) >= 2 else None)
        if scope is not None:
            if not RE_CITE_SCOPE_SECTION.search(scope):
                return True  # explicit non-section scope (whole article)
        elif start < lead_end:
            return True  # bare banner in the lead: whole-article placement
    return False
# {{Zukunft}} family handled separately: on dewiki the inline form
# ("nächste Wahl: 2030{{Zukunft|2030}}" in infoboxes/tables) is a mere DATE
# NOTE — flagships like Berlin, München, Hamburg, Wien, Stuttgart, Bremen and
# Bayern carry it and are perfectly encyclopedic. Only a LINE-ANCHORED
# {{Zukunft}} banner marks a whole speculation article and drops the page.
RE_ZUKUNFT_BANNER = re.compile(
    r"(?m)^[ \t]*\{\{\s*(?:zukunftsmusik|zukunft\d?)\s*(?:\||\}\})",
    re.IGNORECASE)
# Exemption: `{{Veraltet}}` marks outdated but encyclopedic content — such
# articles are kept even when they also carry a quality template.
RE_OLD_TEMPLATE = re.compile(r"\{\{\s*veraltet\s*(?:\||\}\})", re.IGNORECASE)

# STAGE 1.7: B-/Z-prominence & Reality-TV/internet-phenomenon relevance filter.
# Titles following the German Wikipedia disambiguation convention
# "Name (Beruf)" are dropped when the parenthetical marks pure B-/Z-prominence
# or a short-lived TV/internet phenomenon. Conservative on purpose: regular
# professions (singer, actor, athlete, politician …) stay in.
RE_PROMO_TITLE = re.compile(
    r"\((?:"
    r"reality-?show-?teilnehmer(?:in)?|reality-?star|reality-?tv-?star|"
    r"webvideoproduzent(?:in)?|influencer(?:in)?|"
    r"keyaccounter|tokio\ hotel|"          # dewiki in-joke title patterns
    r"playmate|gogo-?tänzer(?:in)?|escort|internetphänomen|meme"
    r")\)$",
    re.IGNORECASE)
RE_PROMO_TITLE_YEAR = re.compile(
    r"\((?:"
    r"reality-?show-?teilnehmer(?:in)?|reality-?star|"
    r"webvideoproduzent(?:in)?|influencer(?:in)?|tiktoker(?:in)?|streamer(?:in)?|"
    r"sänger(?:in)?|band|musiker(?:in)?|schauspieler(?:in)?|modell|model|tänzer(?:in)?|"
    r"synchronsprecher(?:in)?|rapper(?:in)?"
    r"),\s*\*(?:19[5-9]\d|20[01]\d)\)$",
    re.IGNORECASE)
# Categories that mark targeted non-encyclopedic content: reality-TV
# participants and short-lived internet phenomena (raw-text check, like
# RE_DISAMBIG). Pure company PR is covered by the {{Werbung}}/title patterns
# instead — the full category namespace is far too large for a denylist.
RE_DROP_CATEGORY = re.compile(
    r"\[\[\s*(?:Kategorie|Category)\s*:[^\]]*(?:"
    r"reality-?show-?teilnehmer|reality-?tv-?teilnehmer|reality-?show-?kandidat|"
    r"teilnehmer\s+einer\s+reality-?show|"
    r"big[\s_-]*brother|dschungelcamp|"
    r"internetphänomen|webvideoproduzent"
    r")[^\]]*\]\]",
    re.IGNORECASE)

# Namespace prefixes used when counting "real" content links (STAGE 3).
RE_LINK_NAMESPACE = re.compile(
    r"^(?:Datei|File|Image|Abbildung|Bild|Kategorie|Category|Vorlage|Template|"
    r"Hilfe|Help|Portal|Wikipedia|Spezial|Diskussion|Talk|Benutzer|User|Medium|"
    r"MediaWiki|Modul|Module):", re.IGNORECASE)

RE_ANY_WIKILINK = re.compile(r"\[\[([^\[\]]+)\]\]")

# ------------------------------------------------- STAGE 1.8: junk filters (schema v8)
# The DB feeds a RAG pipeline: pages whose TITLES match frequent query words
# (years, months, episode numbers, character names) while carrying little
# knowledge crowd the top-40 candidate window. Pure noise drops outright;
# niche-but-valid content is demoted to Tier 2 (see decide_tier) — the chat
# candidate search only sees Tier 1, the Reader searches everything.

# Filter 3: year/calendar navigation pages — they match EVERY year or date
# in a question and, with bm25(title x10), always land in the candidates.
RE_YEAR_TITLE = re.compile(r"^\d{1,4}(?:\s+v\.\s*Chr\.)?$")
RE_DATE_TITLE = re.compile(
    r"^\d{1,2}\.\s+(?:januar|februar|märz|april|mai|juni|juli|august|"
    r"september|oktober|november|dezember)$", re.IGNORECASE)
RE_CENTURY_TITLE = re.compile(
    r"^\d{1,2}\.\s+jahrhundert(?:\s+v\.\s*chr\.?)?$", re.IGNORECASE)

# Filter 5a: deletion-process banners — the page is on its way out of
# Wikipedia (often vandalism, hoaxes or copyright violations).
RE_DELETE_BANNER = re.compile(
    r"\{\{\s*(?:sla|löschantrag(?:stext)?|urv)\s*[|}]", re.IGNORECASE)

# Filter 2: TV/serial fiction noise (arabic numerals only — roman-numeral
# film titles like "Episode IV" stay). Episode/season pages are plot recaps
# with zero reference value.
RE_EPISODE_TITLE = re.compile(
    r"\b(?:folge|staffel|episode|kapitel|teil)\s+\d+\b"
    r"|\b\d+\.\s*(?:folge|staffel|episode|kapitel|teil)\b"
    r"|\b(?:folgen?|staffeln?|episoden)liste\b"
    r"|\(episode\)", re.IGNORECASE)
# Franchise-scoped pages ("… (Figur)", "… (Orte)") are DEMOTED, not dropped:
# their titles still carry the series' proper names.
RE_FICTION_SCOPE_TITLE = re.compile(
    r"\((?:figur|figuren|personen?|charaktere?|orte?|schauplätze?|handlung|"
    r"welten?|universum)\)$", re.IGNORECASE)
RE_FICTION_CATEGORY = re.compile(
    r"\[\[\s*(?:Kategorie|Category)\s*:[^\]]*(?:"
    r"fernsehserien-?episode|episodenliste|"
    r"figur\s+(?:in|aus)|figuren\s+(?:in|aus)|personen?\s+(?:in|aus)"
    r")[^\]]*\]\]", re.IGNORECASE)

# Filter 4: taxonomy stubs — species articles carry {{Taxobox}}; the vast
# majority are tiny stubs relevant only to a specialist (latin-name) query.
RE_TAXON_BOX = re.compile(r"\{\{\s*taxobox\s*[|}]", re.IGNORECASE)

# Filter 5b: disguised BKLs — the app filters "X steht für:" leads at runtime
# (SPEC §3.1); doing it in the build keeps the Reader clean too. The colon
# separates BKL lists from legit leads ("ADSL steht für Asymmetric …").
RE_BKL_LEAD = re.compile(r"^[^\n]{0,150}\bsteht für\s*:", re.IGNORECASE)


# ------------------------------------------------------- tier decision (schema v8)
# Tier 1 = RAG-visible core (chat candidate search); Tier 2 = niche,
# searchable in the Reader only. Demotion is deliberate: a niche article
# that CANNOT hurt retrieval loses nothing by being invisible to the chat —
# but it stays readable (no knowledge hole in Tab 2).
TIER2_MIN_WORDS = 300       # short pages need external validation (links)
TIER2_MIN_INDEGREE = 4      # < 4 incoming links AND short -> niche
TIER1_LENGTH_RESCUE = 2000  # substance outranks popularity: always Tier 1
TAXON_STUB_WORDS = 150      # {{Taxobox}} pages below this are Tier 2


def decide_tier(title: str, raw: str, words: int, indegree: int | None) -> int:
    """1 = RAG-visible core, 2 = niche (Reader-only). SPEC §4.1 filters.

    *indegree* is the redirect-resolved incoming-link count from the Pass-0
    link graph (scripts/build_linkgraph.py); None disables the in-degree
    rule (no graph available — everything else stays Tier 1).
    """
    # Substance rescue first: long articles never leave Tier 1 — depth IS
    # the quality signal, and it protects specialist knowledge ("Lockpicking",
    # 4,379 words) no matter how popular it is.
    if words >= TIER1_LENGTH_RESCUE:
        return 1
    # Filter 2: franchise-scoped fiction pages (plot characters, settings).
    if RE_FICTION_SCOPE_TITLE.search(title) or RE_FICTION_CATEGORY.search(raw):
        return 2
    # Filter 4: taxonomy stubs.
    if RE_TAXON_BOX.search(raw) and words < TAXON_STUB_WORDS:
        return 2
    # Filter 1: short AND unlinked -> niche. Wikipedia's link graph is its
    # own importance signal — contributors link what matters. The CONJUNCTION
    # matters: a short but well-linked definition stays Tier 1.
    if indegree is not None and indegree < TIER2_MIN_INDEGREE and words < TIER2_MIN_WORDS:
        return 2
    return 1


def fnv1a32(s: str) -> int:
    """FNV-1a 32-bit over UTF-8 — MUST match scripts/build_linkgraph.py (the
    graph keys targets by this hash of the normalized title)."""
    h = 0x811C9DC5
    for b in s.encode("utf-8"):
        h ^= b
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def norm_title(s: str) -> str:
    """MediaWiki title normalization (MUST match build_linkgraph.py):
    underscores -> spaces, whitespace collapsed, only the FIRST character
    case-insensitive (full casefold collides case-variant pages — "MOND"
    vs "Mond")."""
    s2 = re.sub(r"\s+", " ", (s or "").replace("_", " ").strip())
    return s2[:1].upper() + s2[1:] if s2 else s2


class LinkGraph:
    """Read-only view of the Pass-0 link graph (scripts/build_linkgraph.py).

    Maps a page title to its redirect-resolved incoming-link count. Titles
    are matched through the same fnv1a32/casefold normalization the graph
    was built with, so title/namespace variations resolve identically.
    """

    def __init__(self, path: str):
        self.conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)

    def indegree(self, title: str) -> int:
        row = self.conn.execute(
            "SELECT indegree FROM targets WHERE h = ?",
            (fnv1a32(norm_title(title)),)).fetchone()
        return row[0] if row else 0

    def close(self) -> None:
        self.conn.close()


def count_words(text: str) -> int:
    """Word count of the cleaned prose (STAGE 3.1)."""
    return len(text.split())


def count_content_links(raw: str) -> int:
    """Number of real [[content links]] in the raw wikitext (STAGE 3.2).

    File/category/namespace links do not count — they are navigational noise or
    media references, not article-to-article links.
    """
    total = 0
    for match in RE_ANY_WIKILINK.finditer(raw):
        target = match.group(1).split("|", 1)[0].strip().lstrip(":").strip()
        if target and not RE_LINK_NAMESPACE.match(target):
            total += 1
    return total


def process_page(title: str, raw: str, min_words: int, max_link_ratio: float):
    """Run STAGES 1-3 on one page.

    Returns (title, sanitized_text) when the page is accepted, otherwise
    (None, reason) so the caller can count why articles were dropped.
    """
    title = (title or "").strip()
    raw = raw or ""
    if not title:
        return None, "empty-title"
    if RE_SKIP_TITLE.match(title):
        return None, "namespace-title"
    if RE_LIST_TITLE.match(title):
        return None, "list-article"
    if RE_REDIRECT.match(raw):
        return None, "redirect"
    if RE_DISAMBIG.search(raw):
        return None, "disambiguation"
    if RE_STUB_TEMPLATE.search(raw):
        return None, "stub"
    # STAGE 1.6: quality/relevance templates drop the page — unless it carries
    # {{Veraltet}} (outdated but historically valuable content is kept).
    # {{Zukunft}} only counts as a line-anchored banner (see RE_ZUKUNFT_BANNER):
    # inline {{Zukunft|2030}} date notes in infoboxes must NOT drop the page.
    # {{Belege fehlen}} counts only at ARTICLE level (see
    # _article_level_cite_banner): a section-scoped banner marks one part.
    if ((RE_QUALITY_TEMPLATE.search(raw) or RE_ZUKUNFT_BANNER.search(raw)
            or _article_level_cite_banner(raw))
            and not RE_OLD_TEMPLATE.search(raw)):
        return None, "quality-template"
    # STAGE 1.7: B-/Z-prominence & Reality-TV/internet-phenomenon filter.
    if RE_DROP_CATEGORY.search(raw):
        return None, "promo-category"
    if RE_PROMO_TITLE.search(title) or RE_PROMO_TITLE_YEAR.search(title):
        return None, "promo-title"

    # STAGE 1.8 (schema v8): pure-noise & navigation pages drop outright.
    if RE_YEAR_TITLE.match(title):
        return None, "year-title"
    if RE_DATE_TITLE.match(title) or RE_CENTURY_TITLE.match(title):
        return None, "date-title"
    if RE_EPISODE_TITLE.search(title):
        return None, "episode-title"
    if RE_DELETE_BANNER.search(raw):
        return None, "deletion-banner"

    sanitized = sanitize_wiki_text(raw)
    # STAGE 1.8: disguised BKL ("X steht für:" lead) — content-based, so it
    # also catches unlabeled disambiguation pages without the category.
    if RE_BKL_LEAD.match(sanitized):
        return None, "disambiguation"
    words = count_words(sanitized)
    if words < min_words:
        return None, "too-short"
    links = count_content_links(raw)
    if links > max_link_ratio * words:
        return None, "link-heavy"
    return title, sanitized


# ---------------------------------------------------------------- XML dump mode

# Nested XML elements whose text must never leak into wikitext
# (only relevant for malformed dumps where markup is not XML-escaped).
_SKIP_TEXT_TAGS = {"ref", "references", "gallery", "timeline", "math", "chem", "score", "graph"}


def _safe_text(elem) -> str:
    """All text below *elem*, skipping blocklisted nested elements.

    For well-formed dumps <text> has no children and this equals elem.text.
    For malformed dumps (raw <ref> tags inside <text>) it prevents reference
    content from leaking into the article while still returning ALL other
    text (findtext alone would truncate at the first nested element).
    """
    parts: list[str] = []

    def walk(e) -> None:
        tag = e.tag.rsplit("}", 1)[-1].lower()
        if tag in _SKIP_TEXT_TAGS:
            return
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


def _local(tag) -> str:
    """Local name of an XML tag, ignoring the namespace URI.

    Dumps vary between export-0.10 and export-0.11 namespaces — matching by
    local name keeps the parser working across schema versions (a hardcoded
    namespace silently yields ZERO pages on a schema change).
    """
    return tag.rsplit("}", 1)[-1].lower() if isinstance(tag, str) else ""


def _child(elem, name: str):
    """First direct child of *elem* whose local name is *name* (namespace-agnostic)."""
    for c in elem:
        if _local(c.tag) == name:
            return c
    return None


def iter_dump_pages(path: str):
    """Stream <page> elements from a (bz2-compressed) MediaWiki XML dump.

    STAGE 1.1: binary mode + namespace-agnostic local-name matching. iterparse
    on a text-mode stream (TextIOWrapper) is pathologically slow on Windows,
    and matching fully-qualified tags breaks when Wikimedia bumps the export
    schema version.

    RAM-safety: the root element accumulates one empty husk per processed
    <page>; clearing the root keeps memory constant across millions of pages
    (a 6 GB dump otherwise grows several hundred MB of husks per worker).
    """
    opener = bz2.open if path.endswith(".bz2") else open
    with opener(path, "rb") as fh:
        context = ET.iterparse(fh, events=("start", "end"))
        _event, root = next(context)  # <mediawiki> root
        for _event, elem in context:
            if _event != "end" or _local(elem.tag) != "page":
                continue
            ns_elem = _child(elem, "ns")
            ns_text = (ns_elem.text or "0") if ns_elem is not None else "0"
            if ns_text.strip() == "0":  # STAGE 1.2: main namespace only
                title_elem = _child(elem, "title")
                title = (title_elem.text or "") if title_elem is not None else ""
                revision = _child(elem, "revision")
                body = _safe_text(_child(revision, "text")) if revision is not None else ""
                yield title, body
            root.clear()  # drop the page husk (and any siblings) — constant RAM


def iter_extracted_pages(directory: str):
    """Stream <doc id=... title=...> articles from wikiextractor output."""
    for root, _dirs, files in os.walk(directory):
        for name in sorted(files):
            fp = os.path.join(root, name)
            opener = bz2.open if name.endswith(".bz2") else open
            try:
                with opener(fp, "rt", encoding="utf-8", errors="ignore") as fh:
                    title = None
                    body: list[str] = []
                    for line in fh:
                        if line.startswith("<doc "):
                            m = re.search(r'title="([^"]*)"', line)
                            title = m.group(1) if m else os.path.splitext(name)[0]
                            body = []
                        elif line.startswith("</doc"):
                            if title:
                                yield title, "\n".join(body)
                            title, body = None, []
                        elif title is not None:
                            body.append(line)
            except (OSError, EOFError):
                continue


# ---------------------------------------------------------------- STAGE 4: schema

SCHEMA = """
CREATE TABLE IF NOT EXISTS chunks (
    id INTEGER PRIMARY KEY,
    data BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT UNIQUE,
    content_sample TEXT,
    chunk_id INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    length INTEGER NOT NULL,
    tier INTEGER NOT NULL DEFAULT 1          -- 1 = RAG-visible, 2 = Reader-only (v8)
);
"""# External content table: FTS5 stores ONLY the inverted index; title/sample are
# read from `articles` on demand, so nothing is duplicated (SPEC STAGE 4.2).
# `tokenize='trigram'` (schema v5): every 3-char sliding window is indexed, so
# MATCH performs SUBSTRING matching ("mauer" hits "Berliner Mauer" mid-word) —
# the lexical base for typo-tolerant search. Restrictions handled by the app's
# query builder (DbRepository): tokens < 3 chars produce no trigrams and can
# never match; prefix queries need >= 3-char prefixes (SQLite >= 3.45).
# The trigram index is larger than unicode61 (~2-3x postings) — acceptable:
# title+lead only, external content, no duplicated text.
FTS_SCHEMA = (
    "CREATE VIRTUAL TABLE IF NOT EXISTS fts_titles USING fts5("
    "title, content_sample, content='articles', content_rowid='id', "
    "tokenize='trigram')"
)

LEAD_SAMPLE_CHARS = 300


def make_compressor(level: int) -> "zstd.ZstdCompressor":
    """Level 10-18: plain zstd. Level >= 19: long-distance matching with a
    bounded window (2^23 = 8 MB).

    Rationale: STAGE 4.1 groups 100..200 articles per frame, so the window only
    has to cover one chunk (a few hundred KB) — a window beyond that is dead
    memory. Measured on real dewiki chunks, window_log 27 + LDM produce
    byte-identical output to window_log 23 + LDM while pinning ~128 MB per
    worker; 8 MB windows keep several workers well under 100 MB of compressor
    memory and every frame stays far below the 2^27 default decoder limit of the
    phone's plain ZSTD_decompress.
    """
    if level < 19:
        return zstd.ZstdCompressor(level=level)
    try:
        params = zstd.ZstdCompressionParameters.from_level(
            level, window_log=23, enable_ldm=True)
        return zstd.ZstdCompressor(compression_params=params)
    except (AttributeError, ValueError, zstd.ZstdError):
        # Older binding or rejected parameter — fall back to plain LDM.
        return zstd.ZstdCompressor(level=level, enable_ldm=True)


def open_output_db(db_path: str) -> sqlite3.Connection:
    if tuple(int(p) for p in sqlite3.sqlite_version.split(".")[:2]) < (3, 34):
        sys.exit(f"FTS5 trigram tokenizer needs SQLite >= 3.34, found {sqlite3.sqlite_version}")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=OFF")
    cur.execute("PRAGMA synchronous=OFF")
    # 64 MB page cache per connection. With 6 parallel workers this stays
    # ~0.4 GB total — sequential inserts don't benefit from more.
    cur.execute("PRAGMA cache_size=-65536")
    conn.executescript(SCHEMA)  # `chunks` + `articles` (multiple statements)
    cur.execute(FTS_SCHEMA)
    cur.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
    return conn


class ArticleSink:
    """STAGE 4.1/4.2: buffers accepted articles and writes them as shared zstd chunks.

    Each chunk is ONE zstd frame containing the concatenated UTF-8 article
    texts; every article row stores its byte slice (offset, length) into the
    decompressed frame. This removes the per-article frame header and lets the
    compressor exploit redundancy across a hundred articles at once.
    """

    def __init__(self, conn: sqlite3.Connection, level: int, chunk_size: int,
                 build_fts: bool, commit_every: int):
        self.conn = conn
        self.cur = conn.cursor()
        self.cctx = make_compressor(level)
        self.chunk_size = max(1, chunk_size)
        self.build_fts = build_fts
        self.commit_every = max(1, commit_every)
        self._buf = bytearray()
        self._pending: list[tuple[str, str, int, int]] = []  # (title, sample, offset, length)
        self.articles = 0
        self.chunks = 0
        self.duplicates = 0
        self._since_commit = 0

    def add(self, title: str, text: str, tier: int = 1) -> None:
        data = text.encode("utf-8")
        sample = text[:LEAD_SAMPLE_CHARS].replace("\n", " ")
        self._pending.append((title, sample, len(self._buf), len(data), tier))
        self._buf += data
        if len(self._pending) >= self.chunk_size:
            self.flush()

    def flush(self) -> None:
        if not self._pending:
            return
        blob = self.cctx.compress(bytes(self._buf))
        self.cur.execute("INSERT INTO chunks (data) VALUES (?)", (blob,))
        chunk_id = self.cur.lastrowid
        for title, sample, offset, length, tier in self._pending:
            try:
                self.cur.execute(
                    "INSERT INTO articles (title, content_sample, chunk_id, offset, length, tier) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (title, sample, chunk_id, offset, length, tier))
            except sqlite3.IntegrityError:
                self.duplicates += 1  # duplicate title (UNIQUE) — skip this row
                continue
            self.articles += 1
            if self.build_fts:
                self.cur.execute(
                    "INSERT INTO fts_titles (rowid, title, content_sample) VALUES (?, ?, ?)",
                    (self.cur.lastrowid, title, sample))
        self.chunks += 1
        self._buf = bytearray()
        self._pending = []
        self._since_commit += 1
        if self._since_commit >= self.commit_every:
            self.conn.commit()
            self._since_commit = 0

    def finish(self) -> None:
        self.flush()
        self.conn.commit()


def finalize(conn: sqlite3.Connection) -> None:
    """STAGE 4.3: optimize the FTS index and compact the file."""
    conn.execute("INSERT INTO fts_titles(fts_titles) VALUES('optimize')")
    conn.commit()
    conn.execute("VACUUM")
    conn.commit()


def build(db_path: str, pages, level: int, commit_every: int, min_words: int,
          chunk_size: int, max_link_ratio: float,
          indegree_fn=None) -> None:
    """Single-process build from any page iterator (used by tests and --dump).

    *indegree_fn* maps a title to its incoming-link count (Pass-0 link
    graph); None disables the in-degree tier rule.
    """
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed existing {db_path}")

    conn = open_output_db(db_path)
    sink = ArticleSink(conn, level, chunk_size, build_fts=True, commit_every=commit_every)

    seen = skipped = 0
    for title, raw in pages:
        t, result = process_page(title, raw, min_words, max_link_ratio)
        if t is None:
            skipped += 1
            continue
        tier = decide_tier(t, raw, count_words(result),
                           indegree_fn(t) if indegree_fn else None)
        sink.add(t, result, tier)
        seen += 1
        if seen % 20000 == 0:
            size_gb = os.path.getsize(db_path) / 1e9
            print(f"  {seen:>9,} articles | skipped {skipped:,} | {size_gb:.2f} GB", flush=True)

    sink.finish()
    finalize(conn)
    conn.close()

    size_gb = os.path.getsize(db_path) / 1e9
    print(f"Done: {sink.articles:,} articles in {sink.chunks:,} chunks "
          f"(skipped {skipped:,}, dup {sink.duplicates:,}) -> {db_path} ({size_gb:.2f} GB)")


# ------------------------------------------------------- parallel part builder

RE_PART_DB = re.compile(r"\.db$")


def _part_db_path(parts_dir: str, dump_path: str) -> str:
    base = os.path.basename(dump_path)
    return os.path.join(parts_dir, base + ".db")


def _part_schema_version(db_path: str) -> int:
    """PRAGMA user_version of an existing part DB, or -1 when unreadable."""
    try:
        conn = sqlite3.connect(db_path)
        try:
            return int(conn.execute("PRAGMA user_version").fetchone()[0])
        finally:
            conn.close()
    except sqlite3.Error:
        return -1


def _build_part(job: dict) -> dict:
    """Worker: process ONE dump part into its own sqlite file.

    Writes to `<part>.db.tmp` and renames on success — a killed run leaves no
    half-written part DB, so re-running the build skips finished parts.
    """
    dump_path = job["dump"]
    out_path = job["out"]
    level = job["level"]
    min_words = job["min_words"]
    max_link_ratio = job["max_link_ratio"]
    chunk_size = job["chunk_size"]
    commit_every = job["commit_every"]
    graph = LinkGraph(job["linkgraph"]) if job.get("linkgraph") else None

    tmp_path = out_path + ".tmp"
    if os.path.exists(tmp_path):
        os.remove(tmp_path)

    conn = open_output_db(tmp_path)
    # Parts are intermediate: the FTS index is rebuilt once after the merge, so
    # building it per part here would be wasted work.
    sink = ArticleSink(conn, level, chunk_size, build_fts=False, commit_every=commit_every)

    skipped = 0
    for title, raw in iter_dump_pages(dump_path):
        t, result = process_page(title, raw, min_words, max_link_ratio)
        if t is None:
            skipped += 1
            continue
        tier = decide_tier(t, raw, count_words(result),
                           graph.indegree(t) if graph else None)
        sink.add(t, result, tier)
    sink.finish()
    if graph:
        graph.close()
    conn.close()

    os.replace(tmp_path, out_path)
    return {"dump": os.path.basename(dump_path), "articles": sink.articles,
            "skipped": skipped, "duplicates": sink.duplicates, "chunks": sink.chunks,
            "size_gb": os.path.getsize(out_path) / 1e9}


def _retry_locked(fn, attempts: int = 30, delay: float = 1.0):
    """Run a sqlite operation, tolerating transient 'locked' errors.

    On Windows, antivirus scanners briefly hold freshly written/attached
    database files exclusively — retrying for up to ~30 s is enough.
    """
    for i in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            if ("locked" not in msg and "busy" not in msg) or i == attempts - 1:
                raise
            time.sleep(delay)


def merge_parts(out_db: str, part_dbs: list[str]) -> None:
    """Merge per-part DBs into the final database, then build FTS + compact it."""
    if os.path.exists(out_db):
        os.remove(out_db)
    conn = open_output_db(out_db)
    # Autocommit: an open implicit transaction would keep the attached part
    # locked and make DETACH fail ("database p is locked").
    conn.isolation_level = None
    total = 0
    for pdb in part_dbs:
        _retry_locked(lambda: conn.execute("ATTACH DATABASE ? AS p", (pdb,)))
        base = conn.execute("SELECT COALESCE(MAX(id), 0) FROM chunks").fetchone()[0]
        n_chunks, max_chunk = conn.execute(
            "SELECT COUNT(*), COALESCE(MAX(id), 0) FROM p.chunks").fetchone()
        # Remapping relies on contiguous 1..N chunk ids (guaranteed by an
        # INTEGER PRIMARY KEY with no deletes) — refuse to merge otherwise.
        if n_chunks != max_chunk:
            raise SystemExit(
                f"{os.path.basename(pdb)}: non-contiguous chunk ids "
                f"({n_chunks} rows, max id {max_chunk}) — cannot remap safely.")
        before = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        _retry_locked(lambda: conn.execute(
            "INSERT INTO chunks (id, data) SELECT id + ?, data FROM p.chunks ORDER BY id",
            (base,)))
        # Page-id ranges are disjoint across parts -> duplicate titles are
        # virtually impossible; OR IGNORE keeps the merge robust anyway.
        _retry_locked(lambda: conn.execute(
            "INSERT OR IGNORE INTO articles (title, content_sample, chunk_id, offset, length, tier) "
            "SELECT title, content_sample, chunk_id + ?, offset, length, tier FROM p.articles",
            (base,)))
        inserted = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0] - before
        _retry_locked(lambda: conn.execute("DETACH DATABASE p"))
        total += inserted
        print(f"  merged {os.path.basename(pdb)}: {inserted:,} articles "
              f"({os.path.getsize(pdb) / 1e9:.2f} GB)", flush=True)

    # STAGE 4.2/4.3: build the external-content FTS index from the merged
    # articles table, then optimize + compact.
    print("  rebuilding FTS index ...", flush=True)
    _retry_locked(lambda: conn.execute("INSERT INTO fts_titles(fts_titles) VALUES('rebuild')"))
    _retry_locked(lambda: conn.execute("INSERT INTO fts_titles(fts_titles) VALUES('optimize')"))
    _retry_locked(lambda: conn.execute("VACUUM"))
    conn.close()
    print(f"Merged {total:,} articles -> {out_db} ({os.path.getsize(out_db) / 1e9:.2f} GB)")


def run_parts_mode(args: argparse.Namespace) -> None:
    parts_dir = args.parts_dir
    os.makedirs(parts_dir, exist_ok=True)

    dumps = sorted(
        os.path.join(parts_dir, f) for f in os.listdir(parts_dir)
        if f.startswith("dewiki-") and f.endswith(".bz2") and "pages-articles" in f
    )
    if not dumps:
        sys.exit(f"No dewiki-*.bz2 dump parts found in {parts_dir}")

    # Pass-0 link graph (scripts/build_linkgraph.py): enables the in-degree
    # tier rule. Missing graph = rule disabled (everything else Tier 1).
    linkgraph_arg = getattr(args, "linkgraph", "") or ""
    linkgraph_path = linkgraph_arg if linkgraph_arg and os.path.exists(linkgraph_arg) else ""
    if linkgraph_arg and not linkgraph_path:
        print(f">> WARNING: link graph {linkgraph_arg} not found — "
              f"in-degree tier rule disabled")
    elif linkgraph_path:
        print(f">> Link graph: {linkgraph_path}")

    jobs: list[dict] = []
    for d in dumps:
        out = _part_db_path(parts_dir, d)
        if os.path.exists(out) and _part_schema_version(out) == SCHEMA_VERSION:
            print(f">> skip (already built): {os.path.basename(d)}")
            continue
        jobs.append({"dump": d, "out": out, "level": args.level,
                     "min_words": args.min_words, "max_link_ratio": args.max_link_ratio,
                     "chunk_size": args.chunk_size, "commit_every": args.commit_every,
                     "linkgraph": linkgraph_path})

    if jobs:
        workers = min(args.workers, len(jobs))
        print(f">> Building {len(jobs)} parts with {workers} workers "
              f"(zstd level {args.level}, chunk size {args.chunk_size}) ...")
        done = 0
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_build_part, j): j for j in jobs}
            for fut in as_completed(futures):
                job = futures[fut]
                try:
                    res = fut.result()
                    done += 1
                    print(f"  [{done}/{len(jobs)}] {res['dump']}: {res['articles']:,} articles, "
                          f"skipped {res['skipped']:,}, {res['chunks']:,} chunks, "
                          f"{res['size_gb']:.2f} GB", flush=True)
                    if res["articles"] == 0:
                        raise SystemExit(
                            f"Part {res['dump']} yielded 0 articles — dump schema/parse mismatch? "
                            "Refusing to continue with an empty build.")
                except Exception as exc:  # keep other parts alive
                    print(f"  FAILED {job['dump']}: {exc}", flush=True)
                    raise

    part_dbs = [_part_db_path(parts_dir, d) for d in dumps]
    missing = [p for p in part_dbs if not os.path.exists(p)]
    if missing:
        sys.exit(f"Parts missing after build: {missing}")
    print(f">> Merging {len(part_dbs)} part DBs ...")
    merge_parts(args.db, part_dbs)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--parts-dir", help="directory containing the split dump parts (dewiki-*.bz2)")
    src.add_argument("--dump", help="Wikimedia XML dump (.xml or .xml.bz2)")
    src.add_argument("--extracted", help="wikiextractor output directory")
    ap.add_argument("--db", default="wikipedia_compressed.db")
    ap.add_argument("--level", type=int, default=19,
                    help="zstd level 1-19 (default 19; uses long-distance matching)")
    ap.add_argument("--chunk-size", type=int, default=200,
                    help="articles per shared zstd frame (spec: 100..200, default 200)")
    ap.add_argument("--min-words", type=int, default=50,
                    help="minimum words after cleaning (STAGE 3.1, default 50)")
    ap.add_argument("--max-link-ratio", type=float, default=0.5,
                    help="max content-links/words ratio (STAGE 3.2, default 0.5)")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) - 2),
                    help="parallel part workers (--parts-dir only)")
    ap.add_argument("--commit-every", type=int, default=2000)
    ap.add_argument("--linkgraph", default=os.path.join("wiki_build", "linkgraph.db"),
                    help="Pass-0 link graph (scripts/build_linkgraph.py); enables "
                         "the in-degree tier rule (missing file = disabled)")
    args = ap.parse_args()

    if not 1 <= args.level <= 19:
        ap.error("--level must be 1..19")
    if args.chunk_size < 1:
        ap.error("--chunk-size must be >= 1")

    if args.parts_dir:
        run_parts_mode(args)
        return

    pages = iter_dump_pages(args.dump) if args.dump else iter_extracted_pages(args.extracted)
    graph = None
    if args.linkgraph and os.path.exists(args.linkgraph):
        graph = LinkGraph(args.linkgraph)
        print(f">> Link graph: {args.linkgraph}")
    elif args.linkgraph:
        print(f">> Link graph {args.linkgraph} not found — in-degree tier rule disabled")
    print(f"Building {args.db} (zstd level {args.level}, chunk size {args.chunk_size}) ...")
    build(args.db, pages, args.level, args.commit_every, args.min_words,
          args.chunk_size, args.max_link_ratio,
          indegree_fn=(graph.indegree if graph else None))


if __name__ == "__main__":
    main()
