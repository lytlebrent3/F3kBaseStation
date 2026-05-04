#!/usr/bin/env python3
"""
vault_pilot_match.py — Pilot name matching for F3XVault integration
=====================================================================
Matches pilot names between the F3K scorer roster and F3XVault pilot records.

Problem: Names may differ in capitalization, spacing, punctuation, middle
initials, suffixes, or abbreviations. Silent wrong matches are worse than
flagging a miss, so this module uses a tiered approach:

  Tier 1 — Exact match (normalized)         → confident, silent
  Tier 2 — Last name + first initial match   → confident, silent
  Tier 3 — Fuzzy score ≥ FUZZY_THRESHOLD     → confident with note
  Tier 4 — Best fuzzy score < threshold      → flagged as REVIEW
  Tier 5 — No match found                   → flagged as UNMATCHED

The CD sees a match report before the contest starts and can manually
resolve any REVIEW or UNMATCHED entries.

Walk-on and placeholder pilots (bib == 0, or name contains "Walk-on",
"Walk on", or last name is a bare digit) are automatically excluded
from matching.
"""

import re
import difflib
import unicodedata
from dataclasses import dataclass, field

# Fuzzy match threshold — scores below this go to REVIEW status.
# 0.85 means names must share ~85% of their character sequence.
FUZZY_THRESHOLD = 0.82


# ─────────────────────────────────────────────
#  Match result
# ─────────────────────────────────────────────

@dataclass
class PilotMatch:
    roster_name:  str           # name as it appears in scorer roster
    vault_name:   str           # "FirstName LastName" from F3XVault
    vault_id:     int           # F3XVault pilot_id
    vault_bib:    int           # bib number (0 = walk-on/placeholder)
    vault_class:  str           # pilot class string
    score:        float         # match confidence 0.0–1.0
    tier:         int           # 1=exact 2=last+initial 3=fuzzy 4=review
    method:       str           # human-readable match method
    status:       str           # "OK" | "REVIEW" | "UNMATCHED" | "SKIPPED"
    note:         str = ""      # extra info for CD

    @property
    def ok(self) -> bool:
        return self.status == "OK"


@dataclass
class UnmatchedRoster:
    roster_name:  str
    best_vault:   str = ""      # closest vault name found
    best_score:   float = 0.0
    note:         str = ""


@dataclass
class MatchReport:
    matched:    list[PilotMatch]        = field(default_factory=list)
    review:     list[PilotMatch]        = field(default_factory=list)
    unmatched:  list[UnmatchedRoster]   = field(default_factory=list)
    skipped:    list[str]               = field(default_factory=list)  # walk-ons etc.

    @property
    def pilot_map(self) -> dict[str, int]:
        """Returns {roster_name: vault_pilot_id} for all OK matches."""
        return {m.roster_name: m.vault_id for m in self.matched}

    @property
    def all_ok(self) -> bool:
        return not self.review and not self.unmatched

    def summary(self) -> str:
        lines = [
            f"Match summary: {len(self.matched)} OK  "
            f"{len(self.review)} REVIEW  "
            f"{len(self.unmatched)} UNMATCHED  "
            f"{len(self.skipped)} skipped",
        ]
        if self.review:
            lines.append("\nNeeds review (check before contest):")
            for m in self.review:
                lines.append(f"  REVIEW  '{m.roster_name}' ↔ '{m.vault_name}' "
                              f"(score={m.score:.2f}, {m.method})")
        if self.unmatched:
            lines.append("\nNo match found:")
            for u in self.unmatched:
                if u.best_vault:
                    lines.append(f"  UNMATCHED  '{u.roster_name}' "
                                 f"— closest: '{u.best_vault}' ({u.best_score:.2f})")
                else:
                    lines.append(f"  UNMATCHED  '{u.roster_name}'")
        return "\n".join(lines)


# ─────────────────────────────────────────────
#  Name normalization helpers
# ─────────────────────────────────────────────

def _normalize(name: str) -> str:
    """
    Normalize a name for comparison:
      - Unicode NFKD decomposition (strips accents)
      - lowercase
      - strip leading/trailing whitespace
      - collapse internal whitespace
      - remove punctuation except hyphens (keep hyphenated names together)
      - remove common suffixes: Jr, Sr, II, III, IV
    """
    # Unicode normalization
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower().strip()
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name)
    # Remove trailing suffixes (jr, sr, ii, iii, iv) — keep hyphenated parts
    name = re.sub(r"\b(jr\.?|sr\.?|ii|iii|iv)\s*$", "", name).strip()
    # Remove periods (initials) and commas
    name = name.replace(".", "").replace(",", "")
    # Collapse again after removals
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _tokens(name: str) -> list[str]:
    """Split normalized name into tokens."""
    return _normalize(name).split()


def _last_name(name: str) -> str:
    """Return the last token of a normalized name."""
    toks = _tokens(name)
    return toks[-1] if toks else ""


def _first_initial(name: str) -> str:
    """Return the first character of the first token."""
    toks = _tokens(name)
    return toks[0][0] if toks else ""


def _is_placeholder(first: str, last: str, bib: int) -> bool:
    """
    Return True for walk-on / placeholder entries that should never match.
    Conditions:
      - bib == 0 AND (last is a bare integer OR first contains 'walk')
      - last name is a bare digit string
    """
    if re.fullmatch(r"\d+", last.strip()):
        return True
    if "walk" in first.lower() or "walk" in last.lower():
        return True
    return False


def _fuzzy_score(a: str, b: str) -> float:
    """SequenceMatcher ratio between two normalized strings."""
    return difflib.SequenceMatcher(None, _normalize(a), _normalize(b)).ratio()


# ─────────────────────────────────────────────
#  Core matching logic
# ─────────────────────────────────────────────

def match_pilots(
    roster_names: list[str],
    vault_pilots: list[dict],
    fuzzy_threshold: float = FUZZY_THRESHOLD,
) -> MatchReport:
    """
    Match scorer roster names against F3XVault pilot records.

    Parameters
    ----------
    roster_names    List of pilot names from scorer config["roster"]
    vault_pilots    List of pilot dicts from vault_pull_contest() or
                    getEventPilots. Each dict must have:
                        pilot_id, pilot_first_name, pilot_last_name,
                        pilot_bib (optional), pilot_class (optional)
    fuzzy_threshold Override the default fuzzy match threshold (0.0–1.0)

    Returns MatchReport with .pilot_map for direct use in vault_post_round().
    """
    report = MatchReport()

    # ── Build vault candidate list (skip placeholders) ───────────
    candidates = []
    for p in vault_pilots:
        fn   = (p.get("pilot_first_name") or "").strip()
        ln   = (p.get("pilot_last_name")  or "").strip()
        pid  = p.get("pilot_id", 0)
        bib  = p.get("pilot_bib", 0) or 0
        cls  = p.get("pilot_class", "")
        full = f"{fn} {ln}".strip()

        if _is_placeholder(fn, ln, bib):
            report.skipped.append(full)
            continue
        candidates.append({
            "full":  full,
            "norm":  _normalize(full),
            "last":  _last_name(full),
            "init":  _first_initial(full),
            "id":    int(pid),
            "bib":   bib,
            "class": cls,
        })

    used_vault_ids: set[int] = set()   # prevent double-matching

    for roster_name in roster_names:
        rn    = roster_name.strip()
        rnorm = _normalize(rn)
        rlast = _last_name(rn)
        rinit = _first_initial(rn)

        # Filter to unused candidates only
        avail = [c for c in candidates if c["id"] not in used_vault_ids]

        # ── Tier 1: exact normalized match ───────────────────────
        t1 = [c for c in avail if c["norm"] == rnorm]
        if t1:
            c = t1[0]
            used_vault_ids.add(c["id"])
            report.matched.append(PilotMatch(
                roster_name = rn,
                vault_name  = c["full"],
                vault_id    = c["id"],
                vault_bib   = c["bib"],
                vault_class = c["class"],
                score       = 1.0,
                tier        = 1,
                method      = "exact",
                status      = "OK",
            ))
            continue

        # ── Tier 2: last name exact + first initial match ─────────
        t2 = [c for c in avail
              if c["last"] == rlast and c["init"] == rinit]
        if len(t2) == 1:
            c = t2[0]
            used_vault_ids.add(c["id"])
            report.matched.append(PilotMatch(
                roster_name = rn,
                vault_name  = c["full"],
                vault_id    = c["id"],
                vault_bib   = c["bib"],
                vault_class = c["class"],
                score       = 0.95,
                tier        = 2,
                method      = "last+initial",
                status      = "OK",
                note        = f"vault='{c['full']}'",
            ))
            continue

        # ── Tier 3 / 4: fuzzy match — score all candidates ───────
        scored = sorted(
            [(c, _fuzzy_score(rn, c["full"])) for c in avail],
            key=lambda x: x[1], reverse=True,
        )

        if not scored:
            report.unmatched.append(UnmatchedRoster(roster_name=rn))
            continue

        best_c, best_score = scored[0]

        if best_score >= fuzzy_threshold:
            # Tier 3: confident fuzzy match
            used_vault_ids.add(best_c["id"])
            status = "OK"
            tier   = 3
            method = f"fuzzy({best_score:.2f})"
            # If score is borderline, flag for review anyway
            if best_score < 0.90:
                status = "REVIEW"
                tier   = 4
                method = f"fuzzy({best_score:.2f}) — review"
                pm = PilotMatch(
                    roster_name = rn,
                    vault_name  = best_c["full"],
                    vault_id    = best_c["id"],
                    vault_bib   = best_c["bib"],
                    vault_class = best_c["class"],
                    score       = best_score,
                    tier        = tier,
                    method      = method,
                    status      = status,
                    note        = f"vault='{best_c['full']}'",
                )
                report.review.append(pm)
                continue
            used_vault_ids.add(best_c["id"])
            report.matched.append(PilotMatch(
                roster_name = rn,
                vault_name  = best_c["full"],
                vault_id    = best_c["id"],
                vault_bib   = best_c["bib"],
                vault_class = best_c["class"],
                score       = best_score,
                tier        = tier,
                method      = method,
                status      = "OK",
                note        = f"vault='{best_c['full']}'",
            ))
        else:
            # Tier 5: no confident match
            report.unmatched.append(UnmatchedRoster(
                roster_name = rn,
                best_vault  = best_c["full"],
                best_score  = best_score,
                note        = "Below threshold — check spelling",
            ))

    return report


# ─────────────────────────────────────────────
#  Manual override support
# ─────────────────────────────────────────────

def apply_overrides(report: MatchReport,
                    overrides: dict[str, int],
                    vault_pilots: list[dict]) -> MatchReport:
    """
    Apply CD-provided manual overrides to resolve REVIEW or UNMATCHED entries.

    overrides: {roster_name: vault_pilot_id}
        The CD specifies these after seeing the match report.
        Stored in config.json under "f3xvault" → "pilot_overrides".

    Example config.json entry:
        "f3xvault": {
            ...
            "pilot_overrides": {
                "Mike McCurdy": 3979,
                "Jon Garber": 6234
            }
        }
    """
    if not overrides:
        return report

    # Build id → vault pilot info map
    vault_by_id = {}
    for p in vault_pilots:
        pid  = int(p.get("pilot_id", 0))
        fn   = (p.get("pilot_first_name") or "").strip()
        ln   = (p.get("pilot_last_name")  or "").strip()
        vault_by_id[pid] = {
            "full":  f"{fn} {ln}".strip(),
            "bib":   p.get("pilot_bib", 0) or 0,
            "class": p.get("pilot_class", ""),
            "id":    pid,
        }

    # Move REVIEW entries that have overrides to matched
    still_review = []
    for m in report.review:
        if m.roster_name in overrides:
            pid = overrides[m.roster_name]
            vc  = vault_by_id.get(pid, {})
            report.matched.append(PilotMatch(
                roster_name = m.roster_name,
                vault_name  = vc.get("full", f"id={pid}"),
                vault_id    = pid,
                vault_bib   = vc.get("bib", 0),
                vault_class = vc.get("class", ""),
                score       = 1.0,
                tier        = 1,
                method      = "manual override",
                status      = "OK",
                note        = "CD-provided override",
            ))
        else:
            still_review.append(m)
    report.review = still_review

    # Resolve UNMATCHED entries that have overrides
    still_unmatched = []
    for u in report.unmatched:
        if u.roster_name in overrides:
            pid = overrides[u.roster_name]
            vc  = vault_by_id.get(pid, {})
            report.matched.append(PilotMatch(
                roster_name = u.roster_name,
                vault_name  = vc.get("full", f"id={pid}"),
                vault_id    = pid,
                vault_bib   = vc.get("bib", 0),
                vault_class = vc.get("class", ""),
                score       = 1.0,
                tier        = 1,
                method      = "manual override",
                status      = "OK",
                note        = "CD-provided override",
            ))
        else:
            still_unmatched.append(u)
    report.unmatched = still_unmatched

    return report


# ─────────────────────────────────────────────
#  Convenience wrapper for use in f3xvault.py
# ─────────────────────────────────────────────

def build_pilot_map_with_matching(
    roster_names: list[str],
    vault_pilots: list[dict],
    overrides: dict[str, int] | None = None,
    verbose: bool = True,
) -> tuple[dict[str, int], MatchReport]:
    """
    Full matching pipeline. Returns (pilot_map, report).

    pilot_map: {roster_name: vault_pilot_id} — safe to use for postScore.
               Contains only OK matches; REVIEW and UNMATCHED are excluded.

    The caller should check report.all_ok and print report.summary() for the CD.
    """
    report = match_pilots(roster_names, vault_pilots)

    if overrides:
        report = apply_overrides(report, overrides, vault_pilots)

    if verbose:
        print(f"[VAULT] {report.summary()}")
        if report.matched:
            for m in report.matched:
                if m.tier > 1:   # only log non-exact matches
                    print(f"  [{m.method}]  '{m.roster_name}' → '{m.vault_name}'")

    return report.pilot_map, report


# ─────────────────────────────────────────────
#  Test harness
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # Simulate the exact data from event 4277

    roster = [
        "John McNeil", "Justin Tolman", "Chuck Norris", "William Lalla",
        "Joseph Dougherty", "Benjamin Stewart", "Eitan Rotbart", "Milan Bregman",
        "Chris Bloom", "Edward LaCroix", "Scott Fintel", "Markus Kellerer",
        "Gary Fogel", "Florian Seibel", "Roland Sommer", "Charles Martin",
        "Walther Bednarz", "Mengchen Li", "Clint Christofferson", "Xiaoyang Zhao",
        "Matt Nelson", "Rick Jay", "Joe Nave", "Jonathan Hunter",
        "Adrian Kinimaka Jr", "Paul Reese", "Jon Finch", "Mark Chung",
        "Brendon Beardsley", "Mike Smith", "Lawrence Doan", "John Graham",
        "Joe Schuler", "Brent Lytle", "Jon Garber", "Ray Pili",
        "Arthur Markiewicz", "Douglas Maxwell", "Scott Mccurdy", "John Armstrong",
        "Malick Hernandez", "Lex Mierop", "Jens Buchert", "YO MAMA",
        "Kevin Jantz",
    ]

    # Simulated vault pilot list (subset, with deliberate mismatches for testing)
    vault_pilots = [
        {"pilot_id": 110,  "pilot_first_name": "John",      "pilot_last_name": "McNeil",          "pilot_bib": 1,  "pilot_class": "Open"},
        {"pilot_id": 3106, "pilot_first_name": "Justin",    "pilot_last_name": "Tolman",           "pilot_bib": 2,  "pilot_class": "Open"},
        {"pilot_id": 1229, "pilot_first_name": "Chuck",     "pilot_last_name": "Norris",           "pilot_bib": 3,  "pilot_class": "Open"},
        {"pilot_id": 5307, "pilot_first_name": "William",   "pilot_last_name": "Lalla",            "pilot_bib": 4,  "pilot_class": "Open"},
        {"pilot_id": 2061, "pilot_first_name": "Joseph",    "pilot_last_name": "Dougherty",        "pilot_bib": 5,  "pilot_class": "Open"},
        {"pilot_id": 5010, "pilot_first_name": "Eitan",     "pilot_last_name": "Rotbart",          "pilot_bib": 7,  "pilot_class": "Open"},
        {"pilot_id": 5347, "pilot_first_name": "Milan",     "pilot_last_name": "Bregman",          "pilot_bib": 8,  "pilot_class": "Open"},
        {"pilot_id": 2045, "pilot_first_name": "Chris",     "pilot_last_name": "Bloom",            "pilot_bib": 9,  "pilot_class": "Open"},
        {"pilot_id": 1285, "pilot_first_name": "Edward",    "pilot_last_name": "LaCroix",          "pilot_bib": 10, "pilot_class": "Open"},
        {"pilot_id": 4305, "pilot_first_name": "Scott",     "pilot_last_name": "Fintel",           "pilot_bib": 11, "pilot_class": "Open"},
        {"pilot_id": 5012, "pilot_first_name": "Markus",    "pilot_last_name": "Kellerer",         "pilot_bib": 12, "pilot_class": "Open"},
        {"pilot_id": 1089, "pilot_first_name": "Gary",      "pilot_last_name": "Fogel",            "pilot_bib": 13, "pilot_class": "Open"},
        {"pilot_id": 6420, "pilot_first_name": "Florian",   "pilot_last_name": "Seibel",           "pilot_bib": 14, "pilot_class": "Open"},
        {"pilot_id": 428,  "pilot_first_name": "Roland",    "pilot_last_name": "Sommer",           "pilot_bib": 15, "pilot_class": "Open"},
        {"pilot_id": 135,  "pilot_first_name": "Charles",   "pilot_last_name": "Martin",           "pilot_bib": 16, "pilot_class": "Open"},
        {"pilot_id": 2688, "pilot_first_name": "Walther",   "pilot_last_name": "Bednarz",          "pilot_bib": 17, "pilot_class": "Open"},
        {"pilot_id": 2270, "pilot_first_name": "Mengchen",  "pilot_last_name": "Li",               "pilot_bib": 18, "pilot_class": "Open"},
        {"pilot_id": 1617, "pilot_first_name": "Clint",     "pilot_last_name": "Christofferson",   "pilot_bib": 19, "pilot_class": "Open"},
        {"pilot_id": 7264, "pilot_first_name": "Xiaoyang",  "pilot_last_name": "Zhao",             "pilot_bib": 20, "pilot_class": "Open"},
        {"pilot_id": 116,  "pilot_first_name": "Matt",      "pilot_last_name": "Nelson",           "pilot_bib": 22, "pilot_class": "Open"},
        {"pilot_id": 2019, "pilot_first_name": "Rick",      "pilot_last_name": "Jay",              "pilot_bib": 23, "pilot_class": "Open"},
        {"pilot_id": 5525, "pilot_first_name": "Joe",       "pilot_last_name": "Nave",             "pilot_bib": 24, "pilot_class": "Open"},
        {"pilot_id": 6445, "pilot_first_name": "Jonathan",  "pilot_last_name": "Hunter",           "pilot_bib": 25, "pilot_class": "Open"},
        {"pilot_id": 6928, "pilot_first_name": "Adrian",    "pilot_last_name": "Kinimaka Jr",      "pilot_bib": 26, "pilot_class": "Open"},
        {"pilot_id": 3186, "pilot_first_name": "Paul",      "pilot_last_name": "Reese",            "pilot_bib": 27, "pilot_class": "Open"},
        {"pilot_id": 111,  "pilot_first_name": "Jon",       "pilot_last_name": "Finch",            "pilot_bib": 28, "pilot_class": "Open"},
        {"pilot_id": 115,  "pilot_first_name": "Mark",      "pilot_last_name": "Chung",            "pilot_bib": 29, "pilot_class": "Open"},
        {"pilot_id": 6403, "pilot_first_name": "Brendon",   "pilot_last_name": "Beardsley",        "pilot_bib": 30, "pilot_class": "Open"},
        {"pilot_id": 724,  "pilot_first_name": "Mike",      "pilot_last_name": "Smith",            "pilot_bib": 31, "pilot_class": "Open"},
        {"pilot_id": 113,  "pilot_first_name": "Lawrence",  "pilot_last_name": "Doan",             "pilot_bib": 32, "pilot_class": "Open"},
        {"pilot_id": 108,  "pilot_first_name": "John",      "pilot_last_name": "Graham",           "pilot_bib": 33, "pilot_class": "Open"},
        {"pilot_id": 5564, "pilot_first_name": "Joe",       "pilot_last_name": "Schuler",          "pilot_bib": 34, "pilot_class": "Open"},
        {"pilot_id": 2401, "pilot_first_name": "Brent",     "pilot_last_name": "Lytle",            "pilot_bib": 35, "pilot_class": "Open"},
        {"pilot_id": 221,  "pilot_first_name": "Ray",       "pilot_last_name": "Pili",             "pilot_bib": 37, "pilot_class": "Open"},
        {"pilot_id": 3979, "pilot_first_name": "Scott",     "pilot_last_name": "Mccurdy",          "pilot_bib": 40, "pilot_class": "Open"},
        {"pilot_id": 1817, "pilot_first_name": "John",      "pilot_last_name": "Armstrong",        "pilot_bib": 41, "pilot_class": "Open"},
        {"pilot_id": 7153, "pilot_first_name": "Malick",    "pilot_last_name": "Hernandez",        "pilot_bib": 42, "pilot_class": "Open"},
        {"pilot_id": 114,  "pilot_first_name": "Lex",       "pilot_last_name": "Mierop",           "pilot_bib": 43, "pilot_class": "Open"},
        {"pilot_id": 606,  "pilot_first_name": "Jens",      "pilot_last_name": "Buchert",          "pilot_bib": 44, "pilot_class": "Open"},
        {"pilot_id": 2894, "pilot_first_name": "YO",        "pilot_last_name": "MAMA",             "pilot_bib": 45, "pilot_class": "Open"},
        {"pilot_id": 2200, "pilot_first_name": "Kevin",     "pilot_last_name": "Jantz",            "pilot_bib": 46, "pilot_class": "Open"},
        # Walk-ons / placeholders — should be skipped
        {"pilot_id": 5603, "pilot_first_name": "Paul Lapinsky", "pilot_last_name": "1",           "pilot_bib": 0,  "pilot_class": "Open"},
        {"pilot_id": 5604, "pilot_first_name": "Walk-on",       "pilot_last_name": "2",           "pilot_bib": 0,  "pilot_class": "Open"},
        {"pilot_id": 5605, "pilot_first_name": "Walk-on",       "pilot_last_name": "3",           "pilot_bib": 0,  "pilot_class": "Open"},
    ]

    print("=" * 60)
    print("  Pilot name match test — event 4277 roster vs F3XVault")
    print("=" * 60)

    pilot_map, report = build_pilot_map_with_matching(
        roster, vault_pilots, verbose=False
    )

    print(f"\nResults:")
    print(f"  Matched (OK):    {len(report.matched)}")
    print(f"  Needs review:    {len(report.review)}")
    print(f"  Unmatched:       {len(report.unmatched)}")
    print(f"  Skipped (walk-on): {len(report.skipped)}")

    print(f"\nAll OK matches:")
    for m in sorted(report.matched, key=lambda x: x.roster_name):
        flag = f"  [{m.method}]" if m.tier > 1 else ""
        print(f"  {m.roster_name:30s} → vault_id={m.vault_id:5d}  bib={m.vault_bib:2d}{flag}")

    if report.review:
        print(f"\nREVIEW (need CD confirmation):")
        for m in report.review:
            print(f"  '{m.roster_name}' ↔ '{m.vault_name}'  score={m.score:.2f}")

    if report.unmatched:
        print(f"\nUNMATCHED (not in F3XVault event):")
        for u in report.unmatched:
            hint = f"  closest: '{u.best_vault}' ({u.best_score:.2f})" if u.best_vault else ""
            print(f"  '{u.roster_name}'{hint}")

    print(f"\nSkipped (walk-ons/placeholders):")
    for s in report.skipped:
        print(f"  '{s}'")

    # Test normalization edge cases
    print(f"\n{'=' * 60}")
    print("  Normalization edge case tests")
    print(f"{'=' * 60}")
    cases = [
        ("Scott McCurdy",    "Scott Mccurdy"),   # capitalization
        ("Jon Finch Jr.",    "Jon Finch"),        # suffix removal
        ("J. Finch",         "Jon Finch"),        # initial only (will be fuzzy)
        ("Brent  Lytle",     "Brent Lytle"),      # double space
        ("Clint Christofferson", "Clint Christofferson"),  # long name exact
        ("YO MAMA",          "YO MAMA"),          # all caps
        ("Eitan Rotbärt",    "Eitan Rotbart"),    # accent stripping
    ]
    for a, b in cases:
        score = _fuzzy_score(a, b)
        na, nb = _normalize(a), _normalize(b)
        match = "EXACT" if na == nb else (f"FUZZY {score:.2f}" if score >= FUZZY_THRESHOLD else f"MISS  {score:.2f}")
        print(f"  {match:12s}  '{a}' vs '{b}'")
