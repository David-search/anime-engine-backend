#!/usr/bin/env python3
"""
parser.py — faithful Python port of Amatsu's parser.js (mralanbourne/Amatsu,
MIT), the open-source sibling of the nyaa-scraper-stremio addon. Parses chaotic
fansub release titles to determine the episode, with strict season verification
so an S1 file is never served for an S2 request (the "S1-slop" bug).

Pure `re`, no deps. Functions mirror the JS originals 1:1.
"""
import re

_VIDEO_EXT = r"\.(mkv|mp4|avi|wmv|flv|webm|m4v|ts|mov)$"

def sanitize_filename(filename):
    """Strip ext, [group], resolution, codecs, source, audio, CRC32, OP/ED,
    version tags, years — so numeric extraction never reads 'x264' as ep 264
    or '(2019)' as an episode."""
    s = filename
    s = re.sub(r"\.(mkv|mp4|avi|wmv|flv|webm|m4v|ts|mov|srt|ass|ssa|vtt|sub|idx)$", "", s, flags=re.I)
    s = re.sub(r"^\[.*?\]", "", s)
    s = re.sub(r"\b(?:\d{3,4}x\d{3,4})\b", "", s, flags=re.I)
    s = re.sub(r"\b(?:2160|1080|810|720|576|540|480|360)[pix]*\b", "", s, flags=re.I)
    s = re.sub(r"\b(?:x|h)26[45]\b", "", s, flags=re.I)
    s = re.sub(r"\b(?:HEVC|AVC|FHD|HD|SD|10-?bits?|8-?bits?|12-?bits?|Hi10P|Hi444P)\b", "", s, flags=re.I)
    s = re.sub(r"\b(?:BD|BDRip|Blu-?ray|WEB-?DL|WEB-?Rip|DVD|DVDRip|TVRip|HDTV|CAM)\b", "", s, flags=re.I)
    s = re.sub(r"\b(?:FLAC|AAC|AC3|DTS|DTS-HD|TrueHD|Vorbis|Opus|MP3|PCM)\b", "", s, flags=re.I)
    s = re.sub(r"\b(?:Uncensored|Censored|Decensored|Uncen|Dual-?Audio|Multi-?Subs|RAW|Hentai)\b", "", s, flags=re.I)
    s = re.sub(r"\b(?:5\.1|2\.0|7\.1|2\.1)\b", "", s)
    s = re.sub(r"\[[a-fA-F0-9]{8}\]", "", s)
    s = re.sub(r"\b(?:NC)?(?:OP|ED|Opening|Ending)\s*\d*\b", " ", s, flags=re.I)
    s = re.sub(r"\b(?:v\d)\b", "", s, flags=re.I)
    s = re.sub(r"\b(?:19|20)\d{2}\b", "", s)
    return s

_LROMAN = [("ⅰ", "i"), ("ⅱ", "ii"), ("ⅲ", "iii"), ("ⅳ", "iv"), ("ⅴ", "v"),
           ("ⅵ", "vi"), ("ⅶ", "vii"), ("ⅷ", "viii"), ("ⅸ", "ix"), ("ⅹ", "x")]

def normalize_title(text):
    """Lowercase, translate Unicode roman numerals, flatten punctuation to
    spaces (keeps CJK letters via \\w)."""
    s = (text or "").lower()
    s = re.sub(r"[​-‍﻿]", "", s)
    s = s.replace("　", " ")
    for k, v in _LROMAN:
        s = s.replace(k, v)
    s = s.replace("_", " ")
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def verify_title_match(filename, search_titles):
    if not search_titles:
        return True
    stripped = normalize_title(filename)
    for title in search_titles:
        if not title:
            continue
        clean = normalize_title(title)
        if not clean:
            continue
        escaped = re.escape(clean)
        if re.search(r"(?:^|\s)" + escaped + r"(?:\s|$)", stripped, re.I):
            return True
        if re.search(r"[^\x00-\x7F]", clean):
            if clean.replace(" ", "") in stripped.replace(" ", ""):
                return True
    return False

# NOTE: "Part"/"Cour" are deliberately NOT season tags. Fansubbers number a
# split cour as "Part 3 - 06" where "3" is the COUR, not the TVDB season (ani.zip
# files Dr. Stone Cour 3 as season 4). Treating "Part 3" as season 3 made
# is_wrong_season() reject every legitimate split-cour release. Cour matching is
# handled separately via explicit_part() + the ani.zip season-relative map.
_SEASON_RX = re.compile(
    r"(?:s|season)\s*0*(\d+)\b|第\s*0*(\d+)\s*(?:季|期|기)|\b(\d+)(?:st|nd|rd|th)\s+season\b",
    re.I)
_PART_RX = re.compile(r"(?:part|cour)\s*0*(\d+)\b|\b(\d+)(?:st|nd|rd|th)\s+(?:part|cour)\b", re.I)

def explicit_season(filename):
    """Return the explicitly-tagged season int if present and unambiguous, else
    None (used for confidence, not rejection)."""
    seasons = set()
    for m in _SEASON_RX.finditer(filename.lower()):
        seasons.add(int(m.group(1) or m.group(2) or m.group(3)))
    if len(seasons) == 1:
        return next(iter(seasons))
    return None

def explicit_part(filename):
    """Return the explicitly-tagged Part/Cour number (a split-cour indicator,
    SEPARATE from the TVDB season), or None if absent/ambiguous. Used to reject a
    release belonging to a DIFFERENT cour of the same season."""
    parts = set()
    for m in _PART_RX.finditer((filename or "").lower()):
        parts.add(int(m.group(1) or m.group(2)))
    return next(iter(parts)) if len(parts) == 1 else None

def extract_sxxeyy(filename):
    """Return (embedded_season, embedded_episode) for an explicit SxxEyy /
    'Season N Episode M' title, else (None, None). The embedded season is NOT
    trusted as authoritative — TVDB groups relabel sequels as S01 — so the caller
    disambiguates via absolute mapping + airdate."""
    clean = sanitize_filename(filename)
    m = re.search(r"\bs(\d{1,2})\s*e\s*0*(\d{1,4})\b", clean, re.I)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"\bseason\s*(\d{1,2})\s*ep(?:isode)?\s*0*(\d{1,4})\b", clean, re.I)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None

def is_wrong_season(filename, expected_season):
    """Drop a release whose explicitly-tagged season != expected. Returns False
    when no explicit season tag is present (handled elsewhere)."""
    found_explicit = False
    found_right = False
    for m in _SEASON_RX.finditer(filename.lower()):
        s = int(m.group(1) or m.group(2) or m.group(3))
        found_explicit = True
        if s == expected_season:
            found_right = True
    if found_explicit:
        return not found_right
    return False

_EXPLICIT_EP_RX = re.compile(
    r"(?:ep(?:isode)?\.?\s*|\be\s*|ova\s*|oad\s*|special\s*|round\s*|act\s*|chapter\s*|part\s*|"
    r"vol(?:ume)?\.?\s*|第\s*|#\s*|s(\d+)\s*e|season\s*(\d+)\s*ep(?:isode)?\s*)"
    r"0*(\d+)(?:\s*(?:巻|話|话|集|화|회|편|v\d+))?(?:\D|$)", re.I)

def extract_episode_number(filename, expected_season=1):
    """Episode number from a title/filename. Returns int, or -1 if an embedded
    season != expected_season (explicit reject), or None if not found."""
    clean = sanitize_filename(filename)
    clean = re.sub(r"(?:第|시즌\s*)?0*\d+\s*(?:季|期|기)", "", clean, flags=re.I)
    clean = re.sub(r"\b\d+(?:st|nd|rd|th)\s+(?:Season|Part|Cour)\b", "", clean, flags=re.I)
    # 'Part N' / 'Cour N' is a split-cour tag, NOT episode N — strip it before
    # extraction so 'Part 3 - 06' reads as episode 6, not 3.
    clean = re.sub(r"\b(?:part|cour)\s*0*\d+\b", " ", clean, flags=re.I)

    m = _EXPLICIT_EP_RX.search(clean)
    if m:
        file_season = m.group(1) or m.group(2)
        if file_season is not None and int(file_season) != expected_season:
            return -1
        return int(m.group(3))

    m = re.search(r"(?:^|\s)\-\s+0*(\d+)(?:\D|$)", clean, re.I)
    if m:
        return int(m.group(1))

    m = re.search(r"\[0*(\d+)\]|\(0*(\d+)\)", clean, re.I)
    if m:
        return int(m.group(1) or m.group(2))

    clean = re.sub(r"\b(?:S|Season|Part|Cour)\s*0*\d+\b", "", clean, flags=re.I)
    clean = re.sub(r"[\[\]\(\)\{\}_\-\+~,#]", " ", clean).strip()
    for token in reversed(re.split(r"\s+", clean)):
        m = re.match(r"^e?0*(\d+)(?:v\d+)?$", token, re.I)
        if m:
            return int(m.group(1))
    return None

def extract_loose_episode(filename):
    clean = sanitize_filename(filename)
    clean = re.sub(r"\b(?:S|Season|Part|Cour)\s*0*\d+\b", "", clean, flags=re.I)
    clean = re.sub(r"(?:第|시즌\s*)?0*\d+\s*(?:季|期|기)", "", clean, flags=re.I)
    clean = re.sub(r"\b\d+(?:st|nd|rd|th)\s+(?:Season|Part|Cour)\b", "", clean, flags=re.I)
    clean = re.sub(r"\b\d+\s*-?\s*kai\b", "", clean, flags=re.I)
    for m in re.finditer(r"(?:^|[\s\[\]\(\)\{\}_\-\+~,#])0*(\d+)(?:v\d+)? ", clean, re.I):
        num = int(m.group(1))
        if 0 < num < 3000:
            return num
    return None

def get_batch_range(filename):
    """Return {'start','end'} if the title is an episode range (01-12, 01~24,
    01 to 24, Spanish '01 a 12', vol ranges), else None."""
    clean = sanitize_filename(filename)
    clean = re.sub(r"\b(?:s|season|part|cour)\s*0*\d+\s*(?:-|~|to|a|&|\+)\s*(?:s|season|part|cour)?\s*0*\d+\b", "", clean, flags=re.I)
    clean = re.sub(r"(?:第|시즌\s*)?0*\d+\s*(?:-|~|to|a|&|\+)\s*(?:第|시즌\s*)?0*\d+\s*(?:季|期|기)", "", clean, flags=re.I)
    clean = re.sub(r"\b\d+(?:st|nd|rd|th)\s+(?:Season|Part|Cour)\b", "", clean, flags=re.I)
    m = re.search(r"(?:^|\D)(?:第\s*|vol(?:ume)?\.?\s*|e?p?\.?\s*)?0*(\d+)\s*(?:-|~|to|a|&|\+)\s*(?:e?p?\.?\s*)?0*(\d+)(?:\s*(?:巻|話|话|集|화|회|편))?(?:\D|$)", clean, re.I)
    if m:
        start, end = int(m.group(1)), int(m.group(2))
        if end > start and end - start < 3000:
            return {"start": start, "end": end}
    return None

def is_season_batch(filename, expected_season):
    if is_wrong_season(filename, expected_season):
        return False
    clean = re.sub(r"\.(mkv|mp4|avi|wmv|flv|webm|m4v|ts|mov|srt|ass|ssa|vtt|sub|idx)$", "", filename, flags=re.I)
    has_season_tag = bool(re.search(
        r"(?:\bS|Season|Part|Cour)\s*0*%d\b|(?:第|시즌\s*)?0*%d\s*(?:季|期|기)|\b%d(?:st|nd|rd|th)\s+(?:season|part|cour)\b"
        % (expected_season, expected_season, expected_season), clean, re.I))
    has_batch_word = bool(re.search(
        r"\b(batch|complete|collection|boxset|box-set|box\b|bd-box|dvd-box|all episodes|all eps)\b|全集|완결|전편",
        clean, re.I))
    rng = get_batch_range(filename)
    if rng and rng["end"] > rng["start"]:
        return True
    if has_batch_word:
        return True
    ep = extract_episode_number(filename, expected_season)
    loose = extract_loose_episode(filename)
    if has_season_tag and ep in (None,) and loose is None and rng is None:
        return True
    if ep in (None,) and loose is None and rng is None:
        return True
    return False

def select_best_video_file(files, requested_ep, expected_season=1, is_movie=False):
    """Pick the ONE file for requested_ep out of a torrent's file list.
    files: list of dicts with 'name'/'path' and 'size'/'bytes'."""
    def nm(f):
        return (f.get("name") or f.get("path") or "")
    def sz(f):
        return f.get("size", f.get("bytes", 0)) or 0
    def base(f):
        return nm(f).split("/")[-1]

    vids = [f for f in (files or []) if re.search(_VIDEO_EXT, nm(f), re.I)]
    if not vids:
        return None
    ep = int(requested_ep)
    if ep > 1:
        is_movie = False
    if not is_movie:
        MIN, MAX = 30 * 1024**2, 20 * 1024**3
        vids = [f for f in vids if sz(f) == 0 or (MIN <= sz(f) <= MAX)]
    if not vids:
        return None

    def best(cands):
        return sorted(cands, key=lambda f: (nm(f).lower().endswith(".mkv"), sz(f)), reverse=True)[0]

    if is_movie:
        return best(vids)
    matches = [f for f in vids if extract_episode_number(base(f), expected_season) == ep]
    if not matches:
        matches = [f for f in vids if (lambda r: r and r["start"] <= ep <= r["end"])(get_batch_range(base(f)))]
    if not matches:
        matches = [f for f in vids if extract_loose_episode(base(f)) == ep]
    if matches:
        return best(matches)
    if ep == 1:
        clean = [f for f in vids if not re.search(
            r"trailer|promo|menu|teaser|ncop|nced|extra|interview|greeting|geeting|credit|making", nm(f), re.I)]
        if clean:
            return best(clean)
    if len(vids) == 1:
        return vids[0]
    return None
