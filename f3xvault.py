#!/usr/bin/env python3
"""
f3xvault.py — F3XVault API integration for F3K Low Launch Scoring Server
=========================================================================
Handles all communication with the F3XVault API (http://www.f3xvault.com/api.php).

Authentication: every request includes login + password in POST body.
Credentials are read from config.json under the "f3xvault" section:

    "f3xvault": {
        "login":    "your_username",
        "password": "your_password",
        "event_id": 1234,           // set once event is created/known
        "enabled":  true
    }

Workflow
--------
Pre-contest (pull):
  1. vault_pull_contest()   — single call that returns everything needed:
                              pilot_map {name→vault_pilot_id},
                              round_tasks {round_num→task_code},
                              round_sub_counts {round_num→n_subs},
                              event metadata

Post-round (push, whenever CD is ready):
  1. vault_post_round()     — posts sub-flights for all pilots in a round
  2. vault_update_round_status() — marks round scored on F3XVault

Sub-flight format
-----------------
F3XVault stores individual flight times as "M:SS.s" strings in a `subs` array.
The number of subs depends on the task — pulled from `flight_type_sub_flights`
in the getEventInfo response. postScore uses sub1, sub2, ... subN fields.

Raw score and normalised score are computed server-side — we do NOT post them.

Task code mapping
-----------------
F3XVault uses codes like "f3k_h", "f3k_b2", "f3k_i". These map to our
single-letter task IDs. Variants (f3k_b vs f3k_b2) differ in cap and window
but map to the same task letter — the sub count from flight_type_sub_flights
is what actually drives postScore, not the letter itself.
"""

import json
import time
import urllib.request
import urllib.parse
import urllib.error
from threading import Lock

# ─────────────────────────────────────────────
#  Config loading
# ─────────────────────────────────────────────

_vault_cfg: dict = {}
_vault_lock = Lock()

API_URL  = "https://www.f3xvault.com/api.php"
TIMEOUT  = 10   # seconds per HTTP request

def load_vault_config(config: dict) -> bool:
    """
    Load F3XVault credentials from the server's config dict.
    Returns True if the integration is enabled and credentials are present.
    """
    global _vault_cfg
    cfg = config.get("f3xvault", {})
    _vault_cfg = {
        "login":            cfg.get("login", ""),
        "password":         cfg.get("password", ""),
        "enabled":          cfg.get("enabled", False),
        "penalty_per_foot": config.get("scoring", {}).get("penalty_per_foot", 0.5),
        # event_id intentionally omitted — always use _vault_pull_data['event_id']
        # after a pull. config event_id is only used as a startup display hint.
    }
    ok = bool(_vault_cfg["enabled"] and _vault_cfg["login"] and _vault_cfg["password"])
    if ok:
        hint = config.get("f3xvault", {}).get("event_id", 0)
        print(f"[VAULT] Enabled — paste event URL in scorer to pull (config hint: {hint or 'none'})")
    else:
        print("[VAULT] Disabled or missing credentials — F3XVault integration off")
    return ok


def vault_enabled() -> bool:
    return bool(_vault_cfg.get("enabled") and _vault_cfg.get("login"))


# ─────────────────────────────────────────────
#  Core HTTP helper
# ─────────────────────────────────────────────

def _vault_call(function: str, extra_params: dict | None = None) -> dict | None:
    """
    Make an authenticated POST to the F3XVault API.
    Returns parsed JSON dict on success, None on any failure.
    Credentials are never logged.
    """
    if not vault_enabled():
        return None

    params = {
        "login":         _vault_cfg["login"],
        "password":      _vault_cfg["password"],
        "function":      function,
        "output_format": "json",
    }
    if extra_params:
        params.update(extra_params)

    data = urllib.parse.urlencode(params).encode("utf-8")
    req  = urllib.request.Request(API_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        result = json.loads(raw)
        if result.get("response_code") != 1:
            print(f"[VAULT] {function} failed: {result.get('error_string','?')}")
            return None
        return result
    except urllib.error.HTTPError as e:
        print(f"[VAULT] {function} HTTP {e.code}: {e.reason}")
    except urllib.error.URLError as e:
        print(f"[VAULT] {function} network error: {e.reason}")
    except json.JSONDecodeError as e:
        print(f"[VAULT] {function} JSON parse error: {e}")
    except Exception as e:
        print(f"[VAULT] {function} unexpected error: {e}")
    return None


# ─────────────────────────────────────────────
#  Read functions
# ─────────────────────────────────────────────

def vault_check_user() -> dict | None:
    """
    Validate credentials. Returns user info dict or None.
    Call this at startup to confirm the integration is working.
    """
    result = _vault_call("checkUser")
    if result:
        print(f"[VAULT] Auth OK — user_id={result.get('user_id')} "
              f"pilot_id={result.get('pilot_id')}")
    return result


def vault_search_events(search_string: str = "", event_type: str = "f3k",
                        show_future: int = 1) -> list[dict]:
    """
    Search F3XVault for events. Returns list of event dicts.
    Use to confirm event_id before a contest.
    """
    result = _vault_call("searchEvents", {
        "string":          search_string,
        "event_type_code": event_type,
        "show_future":     show_future,
        "per_page":        20,
        "page":            1,
    })
    if not result:
        return []
    events = result.get("events", [])
    print(f"[VAULT] searchEvents returned {len(events)} events")
    return events


def vault_get_event_info(event_id: int | None = None) -> dict | None:
    """
    Get basic event information. Returns event dict or None.
    """
    eid = event_id or _vault_cfg.get("event_id")
    if not eid:
        print("[VAULT] get_event_info: no event_id configured")
        return None
    return _vault_call("getEventInfo", {"event_id": eid})


# ─────────────────────────────────────────────
#  F3XVault flight_type_code → scorer task letter
# ─────────────────────────────────────────────
# Verified from event 4361 round edit dropdown (all 22 task types).
# flight_type_id integers confirmed from the round edit page.
#
# Task D: original "Ladder" (f3k_d, id=9, 7 subs: :15,:30,:45,1:00,1:15,1:30,1:45)
#   replaced in 2020 by "Two Flights" (f3k_d2, id=26, 2 subs, 5:00 max).
#
# Task E: original "Poker" (f3k_e, id=10, 5 pilot-nominated targets, 10 min)
#   replaced in 2020 by 3-target variants:
#   f3k_e2 (id=27) = 3 targets in 10 min, f3k_e3 (id=28) = 3 targets in 15 min.
#
# Task C attempts: f3k_c (id=8)=x3, f3k_c4 (id=17)=x4, f3k_c3 (id=18)=x5.
#   Note: f3k_c3 is named for the 3:00 cap per attempt, NOT the attempt count.
#
# All event_task_time_choice values = 0 (working time is encoded in the code).
FLIGHT_TYPE_CODE_MAP = {
    # Task A — Last Flight (5:00 max)       vault_id
    "f3k_a":   "A",   # 7 min working time     6
    "f3k_a2":  "A",   # 10 min working time    19

    # Task B — Last Two Flights
    "f3k_b":   "B",   # 4:00 max / 10 min       7
    "f3k_b2":  "B",   # 3:00 max / 7 min       20

    # Task C — All Up Last Down (3:00 max per attempt)
    "f3k_c":   "C",   # x3 attempts / 3 subs    8
    "f3k_c4":  "C",   # x4 attempts / 4 subs   17
    "f3k_c3":  "C",   # x5 attempts / 5 subs   18

    # Task D — two variants
    "f3k_d":   "D",   # Original Ladder, 7 subs  9
    "f3k_d2":  "D",   # 2020 Two Flights, 2 subs 26

    # Task E — Poker variants
    "f3k_e":   "E",   # Original: 5 targets      10
    "f3k_e2":  "E",   # 2020: 3 targets, 10 min  27
    "f3k_e3":  "E",   # 2020: 3 targets, 15 min  28

    # Single-variant tasks
    "f3k_f":   "F",   # Three of Six (3:00 max)  11
    "f3k_g":   "G",   # Five Longest (2:00 max)  12
    "f3k_h":   "H",   # 1,2,3,4 Minute           13
    "f3k_i":   "I",   # Three Longest (3:20 max) 14
    "f3k_j":   "J",   # Last Three (3:00 max)    15
    "f3k_k":   "K",   # Big Ladder               21

    # Task L — Single Flight
    "f3k_l":   "L",   # 9:59 max / 10 min        29
    "f3k_l2":  "L",   # 6:59 max / 7 min         34

    # Task M, N
    "f3k_m":   "M",   # Huge Ladder 3/5/7 min    30
    "f3k_n":   "N",   # Best Flight (9:59 max)   33

    # Low Launch — custom, not a standard F3XVault task type
    "ll":      "LL",
}


def vault_pull_contest(event_id: int | None = None) -> dict:
    """
    Full pre-contest pull. One call replaces the need for separate
    getEventPilots + getEventInfo calls.

    The F3XVault pilot list is the authoritative source — pilots registered
    there, so their names and IDs are exact. No fuzzy matching is needed
    for the push as long as draw names match vault names exactly.
    Walk-on / placeholder pilots (bib=0 with numeric last name or
    "Walk-on" in the name) are automatically excluded.

    Returns dict with:
        ok              — True if pull succeeded
        event_id        — confirmed event ID
        event_name      — event name
        event_type      — "f3k" etc.
        start_date      — "MM/DD/YYYY"
        total_rounds    — total rounds in event
        pilot_map       — {full_name: vault_pilot_id}  (no placeholders)
        pilot_bibs      — {full_name: bib_number}
        pilot_classes   — {full_name: class_string}
        vault_roster    — ordered list of full_name strings (ready to use
                          as scorer roster, sorted by bib number)
        round_tasks     — {round_number: task_letter}  e.g. {1:'H', 2:'A'}
        round_sub_counts— {round_number: n_subs}       e.g. {1:4, 2:1}
        round_type_codes— {round_number: f3xvault_code} e.g. {1:'f3k_h'}
        raw             — the full event dict from F3XVault
        error           — error string if ok=False
    """
    eid = event_id
    if not eid:
        return {"ok": False, "error": "No event_id provided — paste the F3XVault event URL and pull"}

    result = _vault_call("getEventInfo", {"event_id": eid})
    if not result:
        return {"ok": False, "error": "getEventInfo call failed"}

    event = result.get("event", {})
    if not event:
        return {"ok": False, "error": "No event data in response"}

    # ── Pilot map — vault is the authoritative source ─────────────
    # Skip walk-ons and placeholders (bib=0 with digit last name or
    # "walk" anywhere in the name).
    import re as _re
    pilot_map    = {}   # "First Last" → pilot_id
    pilot_bibs   = {}   # "First Last" → bib
    pilot_classes= {}   # "First Last" → class string
    bib_order    = []   # [(bib, name)] for sorted roster

    for p in event.get("pilots", []):
        fn   = (p.get("pilot_first_name") or "").strip()
        ln   = (p.get("pilot_last_name")  or "").strip()
        pid  = p.get("pilot_id")
        bib  = int(p.get("pilot_bib") or 0)
        cls  = p.get("pilot_class", "")
        name = f"{fn} {ln}".strip()

        # Skip placeholders: bib=0 with numeric last name, or "walk" in name
        if _re.fullmatch(r"\d+", ln) or "walk" in fn.lower() or "walk" in ln.lower():
            print(f"[VAULT] Skipping placeholder: '{name}'")
            continue
        if not name or not pid:
            continue

        pilot_map[name]     = int(pid)
        pilot_bibs[name]    = bib
        pilot_classes[name] = cls
        bib_order.append((bib if bib > 0 else 9999, name))

    # Sort by bib number (walk-ons with bib=0 already excluded)
    vault_roster = [name for _, name in sorted(bib_order)]

    # ── Round task mapping ────────────────────────────────────────
    round_tasks       = {}   # round_number → task letter ("H", "B", etc.)
    round_sub_counts  = {}   # round_number → n subs to post
    round_type_codes  = {}   # round_number → raw f3xvault code ("f3k_h")
    round_type_ids    = {}   # round_number → flight_type_id integer
    round_time_choice = {}   # round_number → event_task_time_choice integer
    for t in event.get("tasks", []):
        rn   = t.get("round_number")
        code = (t.get("flight_type_code") or "").lower()
        subs = t.get("flight_type_sub_flights", 1)
        fid  = t.get("flight_type_id")
        tc   = t.get("event_task_time_choice", 0)
        if rn:
            round_type_codes[rn]  = code
            round_sub_counts[rn]  = subs
            round_tasks[rn]       = FLIGHT_TYPE_CODE_MAP.get(code, "?")
            round_type_ids[rn]    = fid
            round_time_choice[rn] = tc

    n_pilots = len(pilot_map)
    n_rounds = len(round_tasks)
    print(f"[VAULT] Pull complete: event_id={eid}  "
          f"pilots={n_pilots}  rounds={n_rounds}")
    for rn in sorted(round_tasks)[:5]:
        code = round_type_codes[rn]
        task = round_tasks[rn]
        subs = round_sub_counts[rn]
        print(f"  R{rn:2d}  {code:12s} → {task}  ({subs} subs)")
    if n_rounds > 5:
        print(f"  ... and {n_rounds - 5} more rounds")

    # ── Scrape round edit pages for web-form posting metadata ────
    # The API doesn't expose event_round_id or event_pilot_id values,
    # but these are required to post scores via the web form (which is
    # the only way to also trigger score recalculation).
    # We scrape each round's edit page to capture:
    #   - event_round_id (the internal round record ID)
    #   - per-pilot: flight_record_id, event_pilot_id, flight_type_id
    import re as _re
    round_web_meta = {}   # round_number → {event_round_id, flight_type_id,
                          #                  pilots: {name → {epid, frid}}}

    # First get event_round_ids from the event view page
    round_event_round_ids = {}
    try:
        params = urllib.parse.urlencode({
            "action":   "event",
            "function": "event_view",
            "event_id": str(eid),
            "login":    _vault_cfg.get("login", ""),
            "password": _vault_cfg.get("password", ""),
        }).encode()
        req = urllib.request.Request("https://www.f3xvault.com/", data=params, method="POST")
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
        for m in _re.finditer(
                r"event_round_id=(\d+)[^\"]*\"[^>]*>\s*Round\s*(\d+)", html):
            round_event_round_ids[int(m.group(2))] = int(m.group(1))
        if round_event_round_ids:
            print(f"[VAULT] Found event_round_ids for {len(round_event_round_ids)} rounds")
    except Exception as e:
        print(f"[VAULT] event_round_id fetch warning: {e}")

    # Scrape round edit pages for web-form posting metadata.
    # Field format: pilot_sub_flight_{sub_num}_{frid}_{epid}_{ftid}
    # - frid (flight_record_id): unique per pilot per round
    # - epid (event_pilot_id): event-level, same for a pilot across all rounds
    # - ftid (flight_type_id): same per round
    #
    # Strategy: build global_pilot_id → epid map once from round 1 by
    # cross-referencing getEventRound (has global pids in draw order) with
    # the round 1 edit page form fields (has epids in draw order).
    # Then scrape each round for frids (which vary per round per pilot).

    global_to_epid = {}  # global_pilot_id → event_pilot_id (event-level)
    first_rn = min(round_event_round_ids.keys()) if round_event_round_ids else None

    def _fetch_round_html(rn_erid):
        params = urllib.parse.urlencode({
            "action":         "event",
            "function":       "event_round_edit",
            "event_id":       str(eid),
            "event_round_id": str(rn_erid),
            "login":          _vault_cfg.get("login", ""),
            "password":       _vault_cfg.get("password", ""),
        }).encode()
        req = urllib.request.Request("https://www.f3xvault.com/", data=params, method="POST")
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.read().decode("utf-8", errors="replace")

    def _parse_pilot_fields(html):
        """Extract epid → {frid, ftid} from round edit page HTML."""
        fields = {}
        seen_epids = []
        for m in _re.finditer(r'name="pilot_sub_flight_1_(\d+)_(\d+)_(\d+)"', html):
            frid = int(m.group(1))
            epid = int(m.group(2))
            ftid = int(m.group(3))
            if epid not in fields:
                seen_epids.append(epid)
            fields[epid] = {'frid': frid, 'ftid': ftid}
        return fields, seen_epids  # seen_epids preserves HTML order

    # Build global→epid map using round 1
    first_round_html = None
    if first_rn and round_event_round_ids.get(first_rn):
        try:
            first_round_html = _fetch_round_html(round_event_round_ids[first_rn])
            _, epid_order = _parse_pilot_fields(first_round_html)

            # getEventRound returns pilots in same draw order as the form
            gr = _vault_call("getEventRound", {"event_id": eid, "round_number": first_rn})
            api_flights = gr.get("flights", []) if gr else []
            api_pid_order = [f["pilot_id"] for f in api_flights]

            for i, global_pid in enumerate(api_pid_order):
                if i < len(epid_order):
                    global_to_epid[global_pid] = epid_order[i]
            print(f"[VAULT] global→epid map built: {len(global_to_epid)} pilots")
        except Exception as e:
            print(f"[VAULT] epid map build failed: {e}")

    # Now scrape all rounds for per-round frids
    for rn, erid in sorted(round_event_round_ids.items()):
        try:
            if rn == first_rn and first_round_html:
                html_to_parse = first_round_html
            else:
                html_to_parse = _fetch_round_html(erid)
                time.sleep(0.3)

            pilot_fields, _ = _parse_pilot_fields(html_to_parse)
            round_web_meta[rn] = {
                'event_round_id': erid,
                'flight_type_id': next(iter(pilot_fields.values()), {}).get('ftid'),
                'pilot_fields':   pilot_fields,    # epid → {frid, ftid}
                'global_to_epid': global_to_epid,  # shared map
            }
        except Exception as e:
            print(f"[VAULT] Round {rn} edit page scrape failed: {e}")

    if round_web_meta:
        print(f"[VAULT] Scraped web form metadata for {len(round_web_meta)} rounds")

    return {
        "ok":                   True,
        "event_id":             eid,
        "event_name":           event.get("event_name", ""),
        "event_type":           event.get("event_type_code", ""),
        "start_date":           event.get("start_date", ""),
        "total_rounds":         event.get("total_rounds", 0),
        "pilot_map":            pilot_map,
        "pilot_bibs":           pilot_bibs,
        "pilot_classes":        pilot_classes,
        "vault_roster":         vault_roster,
        "round_tasks":          round_tasks,
        "round_sub_counts":     round_sub_counts,
        "round_type_codes":     round_type_codes,
        "round_type_ids":       round_type_ids,
        "round_time_choice":    round_time_choice,
        "round_event_round_ids":round_event_round_ids,
        "round_web_meta":       round_web_meta,
        "raw":                  event,
        "error":                "",
    }


def vault_get_event_pilots(event_id: int | None = None) -> list[dict]:
    """
    Get pilot roster for an event.
    Returns list of pilot dicts with pilot_id, first/last name, bib, etc.
    """
    eid = event_id or _vault_cfg.get("event_id")
    if not eid:
        return []
    result = _vault_call("getEventPilots", {"event_id": eid})
    if not result:
        return []
    pilots = result.get("pilots", [])
    print(f"[VAULT] getEventPilots: {len(pilots)} pilots in event {eid}")
    return pilots


def vault_get_event_round(round_number: int,
                          event_id: int | None = None) -> list[dict]:
    """
    Get flight data for a single round.
    Returns list of flight dicts each containing:
      pilot_id, rank, group, minutes, seconds, subs[{sub_num, sub_val}], dropped, etc.
    """
    eid = event_id or _vault_cfg.get("event_id")
    if not eid:
        return []
    result = _vault_call("getEventRound", {
        "event_id":     eid,
        "round_number": round_number,
    })
    if not result:
        return []
    return result.get("flights", [])


def vault_get_event_standings(event_id: int | None = None) -> list[dict]:
    """
    Get current standings for an event.
    Returns list of standing dicts with rank, pilot info, total score.
    """
    eid = event_id or _vault_cfg.get("event_id")
    if not eid:
        return []
    result = _vault_call("getEventStandings", {"event_id": eid})
    if not result:
        return []
    return result.get("standings", [])


def vault_search_pilots(name: str, country: str = "") -> list[dict]:
    """
    Search for a pilot by name. Use before createPilot to avoid duplicates.
    Returns list of pilot dicts with pilot_id, name, country.
    """
    result = _vault_call("searchPilots", {"string": name, "country": country})
    if not result:
        return []
    return result.get("pilots", [])


# ─────────────────────────────────────────────
#  Sub-flight format helpers
# ─────────────────────────────────────────────

def seconds_to_vault_sub(total_seconds: float) -> str:
    """
    Convert a flight duration in seconds (float) to F3XVault "M:SS.s" sub format.
    Examples:
        57.8  → "0:57.8"
        118.4 → "1:58.4"
        239.0 → "3:59.0"
        0.0   → "0:00.0"
    """
    if total_seconds <= 0:
        return "0:00.0"
    minutes  = int(total_seconds // 60)
    secs     = total_seconds - minutes * 60
    # Format: M:SS.s  (one decimal place on seconds, zero-padded to 2 digits)
    return f"{minutes}:{secs:04.1f}"


def vault_sub_to_seconds(sub_val: str) -> float:
    """
    Parse a F3XVault sub string "M:SS.s" into decimal seconds.
    Examples:
        "0:57.8"  → 57.8
        "1:58.4"  → 118.4
        "3:59.0"  → 239.0
    """
    try:
        m, s = sub_val.strip().split(":")
        return int(m) * 60 + float(s)
    except Exception:
        return 0.0


# ─────────────────────────────────────────────
#  Task → sub count mapping
# ─────────────────────────────────────────────

# Number of sub-flights per FAI task as stored in F3XVault.
# Tasks that score "best N" still send all flights as subs;
# F3XVault selects/scores them server-side.
# Tasks where the sub count is variable (E, G, I, N, LL)
# send however many flights were actually made.
TASK_SUB_COUNT = {
    "A":  1,    # last 1 flight
    "B":  2,    # last 2 flights
    "C":  3,    # 3 or 5 attempts (match num_attempts)
    "D":  2,    # first 2 flights
    "E":  3,    # poker — up to 3 targets
    "F":  6,    # best 3 of 6 — send all 6
    "G":  5,    # best 5
    "H":  4,    # 4:3:2:1 min targets
    "I":  3,    # best 3
    "J":  3,    # last 3
    "K":  5,    # big ladder — 5 targets in order
    "L":  1,    # one flight
    "M":  3,    # huge ladder — 3 targets in order
    "N":  1,    # best flight (send the best one)
    "LL": None, # variable — send all flights
}


# ─────────────────────────────────────────────
#  Score posting
# ─────────────────────────────────────────────

def _select_flights_for_task(task_code: str, task_flights: list[dict],
                              n_subs: int | None) -> list[dict]:
    """
    Select and order flights from task_flights according to FAI task rules.
    Routes by the task letter (from FLIGHT_TYPE_CODE_MAP) so all variants
    of the same task letter share scoring logic; caps/targets are read from
    the task code's specific parameters.

    Returns list of {'dur', 'lh_ft', 'cap'} dicts for sub formatting.
    """
    if not task_flights:
        return []

    letter = FLIGHT_TYPE_CODE_MAP.get(task_code.lower(), task_code)
    n      = len(task_flights)

    # Helper: adjusted score for a flight against a cap (no lh penalty for selection)
    def adj_score(f, cap):
        dur = f.get('dur', 0.0)
        return min(dur, cap) if cap else dur

    # Task-specific parameters keyed by variant
    PARAMS = {
        # Task A
        'f3k_a':  {'cap': 300, 'lastN': 1},
        'f3k_a2': {'cap': 300, 'lastN': 1},
        # Task B
        'f3k_b':  {'cap': 240, 'lastN': 2},
        'f3k_b2': {'cap': 180, 'lastN': 2},
        # Task C — cap 180, attempts from n_subs
        'f3k_c':  {'cap': 180, 'attempts': 3},
        'f3k_c4': {'cap': 180, 'attempts': 4},
        'f3k_c3': {'cap': 180, 'attempts': 5},
        # Task D
        'f3k_d':  {'targets': [30,45,60,75,90,105,120]},
        'f3k_d2': {'cap': 300, 'firstN': 2},
        # Task E — no cap, best N targets
        'f3k_e':  {'targets_n': 5},
        'f3k_e2': {'targets_n': 3},
        'f3k_e3': {'targets_n': 3},
        # Task L
        'f3k_l':  {'cap': 599},
        'f3k_l2': {'cap': 419},
    }
    p = PARAMS.get(task_code.lower(), {})

    if letter == 'A':
        cap = p.get('cap', 300)
        f   = task_flights[-1] if n else {'dur': 0.0, 'lh_ft': 0.0}
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': cap}]

    if letter == 'B':
        cap   = p.get('cap', 240)
        lastN = p.get('lastN', 2)
        fs    = task_flights[-lastN:] if n >= lastN else list(task_flights)
        while len(fs) < lastN:
            fs.append({'dur': 0.0, 'lh_ft': 0.0})
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': cap} for f in fs]

    if letter == 'C':
        take = n_subs or p.get('attempts', 3)
        fs   = list(task_flights[:take])
        while len(fs) < take:
            fs.append({'dur': 0.0, 'lh_ft': 0.0})
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': 180} for f in fs]

    if letter == 'D':
        if p.get('firstN'):
            # 2020 variant: BEST 2 flights by score, returned in chronological order
            cap      = p.get('cap', 300)
            scores   = [(adj_score(f, cap), i) for i, f in enumerate(task_flights)]
            top_idxs = set(i for _, i in sorted(scores, reverse=True)[:2])
            fs       = [task_flights[i] for i in sorted(top_idxs)]
            while len(fs) < 2:
                fs.append({'dur': 0.0, 'lh_ft': 0.0})
            return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': cap} for f in fs]
        else:
            # Original Ladder: pilot attempts each rung in order, may retry.
            # Only the first successful flight per rung counts.
            # Sub N = the flight that achieved rung N (capped at target), or 0:00.0.
            targets = p.get('targets', [30, 45, 60, 75, 90, 105, 120])
            rung    = 0
            result  = []
            for f in task_flights:
                if rung >= len(targets):
                    break
                tgt = targets[rung]
                if f.get('dur', 0.0) >= tgt:
                    result.append({'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': tgt})
                    rung += 1
                # Failed flights skipped — retry same rung
            # Pad unachieved rungs with zeros
            while len(result) < len(targets):
                result.append({'dur': 0.0, 'lh_ft': 0.0, 'cap': targets[len(result)]})
            return result

    if letter == 'E':
        tgt = p.get('targets_n', 3)
        # Poker — best N by raw duration, returned in chronological order
        scores   = [(f.get('dur', 0.0), i) for i, f in enumerate(task_flights)]
        top_idxs = set(i for _, i in sorted(scores, reverse=True)[:tgt])
        fs       = [task_flights[i] for i in sorted(top_idxs)]
        while len(fs) < tgt:
            fs.append({'dur': 0.0, 'lh_ft': 0.0})
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': None} for f in fs]

    if letter == 'F':
        # Best 3 of up to 6 attempts — select by score but return in chronological order
        cap    = 180
        pool   = task_flights[:6]
        scores = [(adj_score(f, cap), i) for i, f in enumerate(pool)]
        top_idxs = set(i for _, i in sorted(scores, reverse=True)[:3])
        fs = [pool[i] for i in sorted(top_idxs)]  # chronological order
        while len(fs) < 3:
            fs.append({'dur': 0.0, 'lh_ft': 0.0})
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': cap} for f in fs]

    if letter == 'G':
        # Best 5 flights — select by score but return in chronological order
        cap    = 120
        scores = [(adj_score(f, cap), i) for i, f in enumerate(task_flights)]
        top_idxs = set(i for _, i in sorted(scores, reverse=True)[:5])
        fs = [task_flights[i] for i in sorted(top_idxs)]  # chronological order
        while len(fs) < 5:
            fs.append({'dur': 0.0, 'lh_ft': 0.0})
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': cap} for f in fs]

    if letter == 'H':
        # F3XVault sub ordering: Sub 1 = 1-min target, Sub 4 = 4-min target (shortest first)
        # Assign longest flight to 4-min target, second longest to 3-min, etc.
        # Then reverse so sub1=1-min flight, sub4=4-min flight
        targets = [240, 180, 120, 60]
        fs      = sorted(task_flights, key=lambda f: f['dur'], reverse=True)[:4]
        while len(fs) < 4:
            fs.append({'dur': 0.0, 'lh_ft': 0.0})
        # Pair longest flight with longest target, then reverse to sub1=shortest
        pairs = list(zip(fs, targets))
        pairs.reverse()   # now [1-min pair, 2-min pair, 3-min pair, 4-min pair]
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': t}
                for f, t in pairs]

    if letter == 'I':
        # Best 3 flights — select by score but return in chronological order
        cap    = 200
        scores = [(adj_score(f, cap), i) for i, f in enumerate(task_flights)]
        top_idxs = set(i for _, i in sorted(scores, reverse=True)[:3])
        fs = [task_flights[i] for i in sorted(top_idxs)]  # chronological order
        while len(fs) < 3:
            fs.append({'dur': 0.0, 'lh_ft': 0.0})
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': cap} for f in fs]

    if letter == 'J':
        fs = task_flights[-3:] if n >= 3 else list(task_flights)
        while len(fs) < 3:
            fs.append({'dur': 0.0, 'lh_ft': 0.0})
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': 180} for f in fs]

    if letter == 'K':
        targets = [60, 90, 120, 150, 180]
        fs      = list(task_flights[:5])
        while len(fs) < 5:
            fs.append({'dur': 0.0, 'lh_ft': 0.0})
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': t}
                for f, t in zip(fs, targets)]

    if letter == 'L':
        cap = p.get('cap', 599)
        f   = max(task_flights, key=lambda f: f['dur']) if task_flights \
              else {'dur': 0.0, 'lh_ft': 0.0}
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': cap}]

    if letter == 'M':
        targets = [180, 300, 420]
        fs      = list(task_flights[:3])
        while len(fs) < 3:
            fs.append({'dur': 0.0, 'lh_ft': 0.0})
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': t}
                for f, t in zip(fs, targets)]

    if letter == 'N':
        f = max(task_flights, key=lambda f: f['dur']) if task_flights \
            else {'dur': 0.0, 'lh_ft': 0.0}
        return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': 599}]

    # LL and unknown — return all flights uncapped
    return [{'dur': f['dur'], 'lh_ft': f.get('lh_ft', 0.0), 'cap': None}
            for f in task_flights]


def _build_sub_params(task_id: str, scored_flights: list[dict],
                      task_flights: list[dict],
                      n_subs_override: int | None = None,
                      penalty_per_foot: float = 0.5,
                      use_ll_penalty: bool = False) -> tuple[dict, int]:
    """
    Build sub1..subN POST parameters for postScore.

    Uses scored_flights if populated; falls back to task_flights
    via _select_flights_for_task() — this handles the case where
    scored_flights is empty (completed rounds where live unit has moved on).

    Returns (sub_params, penalty_points).
    """
    subs          = {}
    penalty_total = 0.0
    # Sub count: use override (from pull), else fall back to task definition
    letter = FLIGHT_TYPE_CODE_MAP.get(task_id.lower(), task_id)
    sub_n  = n_subs_override or TASK_SUB_COUNT.get(letter)

    # Use scored_flights if available, otherwise derive from task_flights
    if scored_flights:
        selected = scored_flights  # has flight_idx, dur, lh_ft, cap
        use_scored = True
    else:
        selected = _select_flights_for_task(task_id, task_flights, sub_n)
        use_scored = False

    for i, sf in enumerate(selected, 1):
        if use_scored and sf.get("flight_idx", 0) < 0:
            subs[f"sub{i}"] = "0:00.0"
            continue
        dur   = sf.get("dur", 0.0)
        lh_ft = sf.get("lh_ft", 0.0)
        cap   = sf.get("cap")

        if use_ll_penalty:
            # Embed LL penalty into the sub value.
            # F3XVault's 'penalty' field is for scoring POINTS (DSQ etc), not seconds.
            # We adjust the sub duration directly: adj = max(0, capped - lh_ft * ppf).
            scorable = min(dur, cap) if cap else dur
            adj = scorable - lh_ft * penalty_per_foot
            subs[f"sub{i}"] = seconds_to_vault_sub(max(0.0, adj))
            penalty_total  += lh_ft * penalty_per_foot
        else:
            # Standard FAI — send capped raw duration
            scorable = min(dur, cap) if cap else dur
            subs[f"sub{i}"] = seconds_to_vault_sub(scorable)

    # Pad to expected sub count
    if sub_n and len(subs) < sub_n:
        for i in range(len(subs) + 1, sub_n + 1):
            subs[f"sub{i}"] = "0:00.0"

    return subs, round(penalty_total)


def vault_post_score(pilot_id: int, round_number: int,
                     task_id: str,
                     scored_flights: list[dict],
                     task_flights: list[dict],
                     group: str = "",
                     order: int = 0,
                     n_subs: int | None = None,
                     penalty_per_foot: float | None = None,
                     use_ll_penalty: bool = False,
                     event_id: int | None = None) -> bool:
    """
    Post a single pilot's round score to F3XVault.

    use_ll_penalty: if True, subs = raw flight durations and penalty field
    = launch height deduction in seconds. Works for any FAI task.
    """
    eid = event_id or _vault_cfg.get("event_id")
    if not eid:
        print("[VAULT] postScore: no event_id configured")
        return False
    if not pilot_id:
        print("[VAULT] postScore: no pilot_id")
        return False

    ppf = penalty_per_foot if penalty_per_foot is not None \
          else _vault_cfg.get("penalty_per_foot", 0.5)

    sub_params, penalty_pts = _build_sub_params(
        task_id, scored_flights, task_flights, n_subs, ppf,
        use_ll_penalty=use_ll_penalty
    )

    params = {
        "event_id": eid,
        "pilot_id": pilot_id,
        "round":    round_number,
        "group":    group,
        "order":    order,
    }
    params.update(sub_params)

    # Note: we do NOT send the 'penalty' field to F3XVault.
    # F3XVault's penalty field deducts scoring POINTS (e.g. DSQ penalties),
    # not seconds. Our Low Launch height penalty is already embedded in the
    # adjusted sub durations when use_ll_penalty=True.

    result = _vault_call("postScore", params)
    if result:
        sub_str = "  ".join(f"sub{k[3:]}={v}" for k, v in sub_params.items())
        pen_str = f"  penalty={penalty_pts}s" if penalty_pts else ""
        print(f"[VAULT] Posted R{round_number} pilot={pilot_id} "
              f"task={task_id}  {sub_str}{pen_str}")
        return True
    else:
        # Debug: print exact params that failed (omit password)
        safe = {k:v for k,v in params.items() if k != 'password'}
        print(f"[VAULT] postScore FAILED params: {safe}")
        return False


def vault_post_round_web(round_number: int, task_id: str,
                         pilot_scores: list[dict],
                         vault_pilot_map: dict[str, int],
                         round_web_meta: dict,
                         n_subs_override: int | None = None,
                         use_ll_penalty: bool = False,
                         event_id: int | None = None) -> dict:
    """
    Post scores for a round via F3XVault's web form (event_round_save).

    This is the correct approach because:
    1. postScore silently ignores writes to already-scored rounds
    2. updateEventRoundStatus does NOT trigger score recalculation
    3. The web form save both writes scores AND recalculates in one step

    round_web_meta: the per-round metadata dict from vault_pull_contest,
    containing event_round_id and pilot_fields (epid → {frid, ftid}).

    The pilot form field format is:
      pilot_sub_flight_{sub_num}_{flight_record_id}_{event_pilot_id}_{flight_type_id}

    We need to match global pilot_id → event_pilot_id. We do this by
    fetching getEventRound to get the global pilot_id order, then matching
    against the scraped event_pilot_ids by position.
    """
    eid = event_id or _vault_cfg.get("event_id")
    if not eid:
        return {"ok": False, "posted": [], "failed": [], "error": "No event_id"}

    meta = round_web_meta.get(round_number, {})
    event_round_id = meta.get("event_round_id")
    pilot_fields   = meta.get("pilot_fields", {})   # epid → {frid, ftid}
    flight_type_id = meta.get("flight_type_id")

    if not event_round_id or not pilot_fields:
        print(f"[VAULT] Round {round_number}: no web meta — falling back to postScore")
        return vault_post_round(
            round_number     = round_number,
            task_id          = task_id,
            pilot_scores     = pilot_scores,
            vault_pilot_map  = vault_pilot_map,
            round_sub_counts = {round_number: n_subs_override} if n_subs_override else None,
            use_ll_penalty   = use_ll_penalty,
            event_id         = event_id,
        )

    # Use pre-built global→epid map from pull metadata
    global_to_epid = meta.get('global_to_epid', {})
    epid_map = dict(global_to_epid)  # global_pilot_id → event_pilot_id

    if not epid_map:
        print(f"[VAULT] Round {round_number}: could not build epid map — falling back to postScore")
        return vault_post_round(
            round_number     = round_number,
            task_id          = task_id,
            pilot_scores     = pilot_scores,
            vault_pilot_map  = vault_pilot_map,
            round_sub_counts = {round_number: n_subs_override} if n_subs_override else None,
            use_ll_penalty   = use_ll_penalty,
            event_id         = event_id,
        )

    # Build the full web form POST body
    n_subs = n_subs_override or TASK_SUB_COUNT.get(
        FLIGHT_TYPE_CODE_MAP.get(task_id.lower(), task_id), 1)

    params = {
        "action":                   "event",
        "function":                 "event_round_save",
        "event_id":                 str(eid),
        "event_round_id":           str(event_round_id),
        "event_round_number":       str(round_number),
        "event_round_score_status": "on",
        "event_round_flyoff":       "0",
        "create_new_round":         "0",
        "login":                    _vault_cfg.get("login", ""),
        "password":                 _vault_cfg.get("password", ""),
    }

    posted = []
    failed = []

    for ps in pilot_scores:
        name       = ps.get("name", "")
        global_pid = vault_pilot_map.get(name)
        if not global_pid:
            print(f"[VAULT] No vault pilot_id for '{name}' — skipping")
            failed.append(name)
            continue

        epid = epid_map.get(global_pid)
        if not epid:
            print(f"[VAULT] No event_pilot_id for '{name}' (global_pid={global_pid}) — skipping")
            failed.append(name)
            continue

        pf   = pilot_fields.get(epid, {})
        frid = pf.get("frid")
        ftid = pf.get("ftid", flight_type_id)

        if not frid:
            print(f"[VAULT] No flight_record_id for '{name}' epid={epid} — skipping")
            failed.append(name)
            continue

        # Build sub values
        sub_params, _ = _build_sub_params(
            task_id          = task_id,
            scored_flights   = [],
            task_flights     = ps.get("task_flights", []),
            n_subs_override  = n_subs,
            penalty_per_foot = _vault_cfg.get("penalty_per_foot", 0.5),
            use_ll_penalty   = use_ll_penalty,
        )

        # Add sub fields in web form format
        for sub_num_str, val in sub_params.items():
            sub_num = sub_num_str[3:]  # "sub1" → "1"
            field   = f"pilot_sub_flight_{sub_num}_{frid}_{epid}_{ftid}"
            params[field] = val

        posted.append(name)
        sub_str = "  ".join(f"sub{k[3:]}={v}" for k, v in sub_params.items())
        print(f"[VAULT] Web R{round_number} {name}: {sub_str}")

    if posted:
        # POST the complete form
        try:
            data = urllib.parse.urlencode(params).encode()
            req  = urllib.request.Request("https://www.f3xvault.com/", data=data, method="POST")
            with urllib.request.urlopen(req, timeout=20) as r:
                if r.status == 200:
                    print(f"[VAULT] Round {round_number} web form posted — scores written + recalculated")
                else:
                    print(f"[VAULT] Round {round_number} web form returned status {r.status}")
                    failed.extend(posted)
                    posted.clear()
        except Exception as e:
            print(f"[VAULT] Round {round_number} web form POST error: {e}")
            failed.extend(posted)
            posted.clear()

    print(f"[VAULT] Round {round_number} web upload: {len(posted)} OK, {len(failed)} failed")
    return {"ok": len(failed) == 0, "posted": posted, "failed": failed}


def vault_save_round_web(round_number: int, event_round_id: int,
                         event_id: int | None = None) -> bool:
    """
    Trigger F3XVault to recalculate scores for a round by POSTing
    the event_round_save web form. This is required after postScore —
    updateEventRoundStatus marks the round scored but does NOT trigger
    score recalculation. This call does.

    event_round_id: the internal F3XVault round ID (e.g. 30753), captured
    during vault_pull_contest and stored in _vault_pull_data['round_event_round_ids'].
    """
    eid = event_id or _vault_cfg.get("event_id")
    if not eid or not event_round_id:
        return False

    params = {
        "action":                   "event",
        "function":                 "event_round_save",
        "event_id":                 str(eid),
        "event_round_id":           str(event_round_id),
        "event_round_number":       str(round_number),
        "event_round_score_status": "on",
        "event_round_flyoff":       "0",
        "create_new_round":         "0",
        "login":                    _vault_cfg.get("login", ""),
        "password":                 _vault_cfg.get("password", ""),
    }
    data = urllib.parse.urlencode(params).encode()
    try:
        req = urllib.request.Request(
            "https://www.f3xvault.com/", data=data, method="POST")
        with urllib.request.urlopen(req, timeout=15) as r:
            if r.status == 200:
                print(f"[VAULT] Round {round_number} saved/recalculated (event_round_id={event_round_id})")
                return True
    except Exception as e:
        print(f"[VAULT] save_round_web R{round_number} error: {e}")
    return False


def vault_update_round_status(round_number: int, scored: bool = True,
                              event_id: int | None = None) -> bool:
    """
    Mark a round as scored (scored=True) or unscored (scored=False).
    Called after all pilots in a round have been posted.
    Note: does NOT trigger score recalculation — use vault_save_round_web for that.
    """
    eid = event_id or _vault_cfg.get("event_id")
    if not eid:
        return False
    result = _vault_call("updateEventRoundStatus", {
        "event_id":                 eid,
        "round_number":             round_number,
        "event_round_score_status": 1 if scored else 0,
    })
    if result:
        print(f"[VAULT] Round {round_number} marked {'scored' if scored else 'unscored'}")
    return bool(result)


# ─────────────────────────────────────────────
#  Bulk round push
# ─────────────────────────────────────────────

def vault_post_round(round_number: int, task_id: str,
                     pilot_scores: list[dict],
                     vault_pilot_map: dict[str, int],
                     round_sub_counts: dict[int, int] | None = None,
                     use_ll_penalty: bool = False,
                     event_id: int | None = None,
                     mark_scored: bool = True,
                     event_round_id: int | None = None) -> dict:
    """
    Post all pilot scores for a completed round to F3XVault.

    IMPORTANT: F3XVault silently ignores postScore calls for rounds that are
    already marked scored. We unmark the round first, post all scores, then
    remark it scored. This ensures re-pushes always overwrite correctly.
    """
    posted = []
    failed = []
    n_subs = (round_sub_counts or {}).get(round_number)

    # Unmark scored so F3XVault accepts overwrites
    vault_update_round_status(round_number, scored=False, event_id=event_id)
    time.sleep(0.3)

    for ps in pilot_scores:
        name = ps.get("name", "")
        pid  = vault_pilot_map.get(name)
        if not pid:
            print(f"[VAULT] No vault pilot_id for '{name}' — skipping")
            failed.append(name)
            continue

        ok = vault_post_score(
            pilot_id         = pid,
            round_number     = round_number,
            task_id          = task_id,
            scored_flights   = ps.get("scored_flights", []),
            task_flights     = ps.get("task_flights", []),
            group            = ps.get("group", ""),
            order            = ps.get("order", 0),
            n_subs           = n_subs,
            penalty_per_foot = _vault_cfg.get("penalty_per_foot", 0.5),
            use_ll_penalty   = use_ll_penalty,
            event_id         = event_id,
        )
        if ok:
            posted.append(name)
        else:
            failed.append(name)
        time.sleep(0.3)

    if mark_scored and posted:
        vault_update_round_status(round_number, scored=True, event_id=event_id)
        if event_round_id:
            vault_save_round_web(round_number, event_round_id, event_id=event_id)
        else:
            print(f"[VAULT] Round {round_number}: no event_round_id — skipping web save (CD must save manually)")

    print(f"[VAULT] Round {round_number} upload: {len(posted)} OK, {len(failed)} failed")
    return {"ok": len(failed) == 0, "posted": posted, "failed": failed}


# ─────────────────────────────────────────────
#  Pilot map builder
# ─────────────────────────────────────────────

def build_vault_pilot_map(event_id: int | None = None) -> dict[str, int]:
    """
    Fetch the event pilot list and build a {full_name: pilot_id} map.
    Full name is "FirstName LastName" — same format used in the scorer roster.

    Returns empty dict if the call fails.
    """
    pilots = vault_get_event_pilots(event_id)
    pilot_map = {}
    for p in pilots:
        fn   = (p.get("pilot_first_name") or "").strip()
        ln   = (p.get("pilot_last_name")  or "").strip()
        pid  = p.get("pilot_id")
        name = f"{fn} {ln}".strip()
        if name and pid:
            pilot_map[name] = int(pid)
    print(f"[VAULT] Pilot map built: {len(pilot_map)} pilots")
    return pilot_map


# ─────────────────────────────────────────────
#  Startup check
# ─────────────────────────────────────────────

def vault_startup_check(config: dict) -> bool:
    """
    Load config and verify credentials. Call from server startup.
    Returns True if integration is live and working.
    """
    if not load_vault_config(config):
        return False
    result = vault_check_user()
    return result is not None


# ─────────────────────────────────────────────
#  CLI test harness
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    # Quick test — requires config.json with f3xvault section in current dir
    try:
        with open("config.json") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        print("No config.json found. Create one with:")
        print('  {"f3xvault": {"login": "...", "password": "...", '
              '"event_id": 0, "enabled": true}}')
        sys.exit(1)

    if not vault_startup_check(cfg):
        print("Startup check failed.")
        sys.exit(1)

    eid = _vault_cfg.get("event_id")
    if eid:
        print(f"\n{'='*55}")
        print(f"  vault_pull_contest(event_id={eid})")
        print(f"{'='*55}")
        pull = vault_pull_contest(eid)
        if pull["ok"]:
            print(f"\n  Event:   {pull['event_name']}")
            print(f"  Date:    {pull['start_date']}")
            print(f"  Rounds:  {pull['total_rounds']}")
            print(f"  Pilots:  {len(pull['pilot_map'])}")
            print(f"\n  Round task map:")
            for rn in sorted(pull["round_tasks"])[:16]:
                code = pull["round_type_codes"][rn]
                task = pull["round_tasks"][rn]
                subs = pull["round_sub_counts"][rn]
                print(f"    R{rn:2d}  {code:14s} → task {task}  "
                      f"({subs} sub{'s' if subs != 1 else ''})")
            print(f"\n  First 5 pilots:")
            for name, pid in list(pull["pilot_map"].items())[:5]:
                bib = pull["pilot_bibs"].get(name, "?")
                cls = pull["pilot_classes"].get(name, "")
                print(f"    #{bib:2}  {name:25s}  vault_id={pid}  [{cls}]")
        else:
            print(f"Pull failed: {pull['error']}")

    print(f"\n{'='*55}")
    print("  Sub format round-trip test")
    print(f"{'='*55}")
    test_cases = [57.8, 118.4, 179.0, 239.0, 0.0, 240.0, 419.0]
    for s in test_cases:
        vault_str = seconds_to_vault_sub(s)
        back      = vault_sub_to_seconds(vault_str)
        status    = "OK" if abs(s - back) < 0.01 else "MISMATCH"
        print(f"  {s:6.1f}s → '{vault_str}' → {back:.1f}s  {status}")
