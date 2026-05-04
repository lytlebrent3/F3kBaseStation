#!/usr/bin/env python3
"""
F3K Flight Scoring Server — v4
================================
Full FAI task engine (Tasks A–N + Low Launch).
Landing window (30s) enforcement for all tasks.
Task C: per-attempt sub-state machine (3 or 5 flights, no 4-flight option).
Pilot name management.  FAI normalisation.  Launch-height penalty scoring.

Run:
  python3 f3k_server.py [--sim] [--udp-port 5005] [--http-port 8080]
"""

import asyncio, json, math, os, random, socket, struct, time
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from threading import Thread
from datetime import datetime
from urllib.parse import urlparse, parse_qs

# ── F3XVault integration (optional — disabled if modules not present) ──
try:
    from f3xvault import (
        vault_startup_check, vault_pull_contest,
        vault_post_round, vault_update_round_status,
    )
    from vault_pilot_match import build_pilot_map_with_matching
    _VAULT_AVAILABLE = True
    print("[VAULT] f3xvault modules loaded")
except ImportError as _ve:
    _VAULT_AVAILABLE = False
    print(f"[VAULT] Modules not found ({_ve}) — F3XVault integration disabled")

# ── Vault runtime state ────────────────────────────────────────────────
_vault_pull_data   = {}    # result of last vault_pull_contest() call
_vault_pilot_map   = {}    # {roster_name: vault_pilot_id} after matching
_vault_match_report= None  # MatchReport object from last pull

# ─────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────
CONFIG_FILE = 'config.json'

def load_config():
    defaults = {
        'udp_port': 5005, 'http_port': 8080,
        'scoring': {'penalty_per_foot': 1.0},
        'units':   {'stale_timeout_s': 10, 'max_units': 200},
        'session': {'name': 'F3K Session', 'date': ''},
        'prep':    {'testing_s': 60},   # test flight window in seconds, default 1 min
        'roster':  [],
    }
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:
                cfg = json.load(f)
            if 'roster' not in cfg:
                cfg['roster'] = []
            print(f"[CFG] Loaded {CONFIG_FILE}  "
                  f"penalty={cfg['scoring']['penalty_per_foot']}s/ft  "
                  f"roster={len(cfg['roster'])} pilots")
            return cfg
        except Exception as e:
            print(f"[CFG] Error: {e} — using defaults")
    return defaults

def save_config():
    """Write current config + roster back to config.json."""
    try:
        cfg = {
            'udp_port':    UDP_PORT,
            'http_port':   HTTP_PORT,
            'scoring':     {'penalty_per_foot': PENALTY_PER_FOOT},
            'units':       {'stale_timeout_s': STALE_TIMEOUT_S, 'max_units': 200},
            'session':     {'name': SESSION_NAME, 'date': ''},
            'prep':        {'testing_s': round_settings['testing_s']},
            'roster':      roster,
        }
        with open(CONFIG_FILE, 'w') as f:
            json.dump(cfg, f, indent=2)
        print(f"[CFG] Saved {CONFIG_FILE}  roster={len(roster)} pilots")
    except Exception as e:
        print(f"[CFG] Save error: {e}")

config           = load_config()
PENALTY_PER_FOOT = config['scoring']['penalty_per_foot']
STALE_TIMEOUT_S  = config['units']['stale_timeout_s']
BROADCAST_ADDR      = config.get('network', {}).get('broadcast_addr', '192.168.0.255')
LOG_RETRY_INTERVAL_S  = int(config.get('network', {}).get('log_retry_interval_s', 60))
LOG_FETCH_DELAY_S     = float(config.get('network', {}).get('log_fetch_delay_s', 3.0))
                        # Seconds to wait after Packet 5 before first HTTP fetch.
                        # Gives the unit's HTTP server time to start after WiFi re-associates.
PREP_BROADCAST_INTERVAL_S = 5   # how often to send 0x21 during prep

UDP_PORT         = config['udp_port']
HTTP_PORT        = config['http_port']
SESSION_NAME     = config['session']['name']
TESTING_S_DEFAULT = int(config.get('prep', {}).get('testing_s', 60))

LANDING_WINDOW_S = 30   # seconds after working window closes
TASK_C_CAP_S     = 180  # max flight time per attempt
TASK_C_PREP_S    = 60   # no-fly preparation between attempts

# ── Vault startup ──────────────────────────────────────────────────────
if _VAULT_AVAILABLE:
    vault_startup_check(config)

# ─────────────────────────────────────────────
#  FAI Task Definitions
# ─────────────────────────────────────────────
TASKS = {
    'A':  {'id':'A',  'name':'Task A',  'desc':'Last Flight',
           'window':600, 'numFlights':99, 'lastN':1,
           'targets':[480], 'maxScore':480,
           'windowPickable':True, 'taskAStyle':True},
    'B':  {'id':'B',  'name':'Task B',  'desc':'Two Last Flights',
           'window':600, 'numFlights':99, 'lastN':2,
           'targets':[240,240], 'maxScore':480},
    'C':  {'id':'C',  'name':'Task C',  'desc':'All Up Last Down',
           'window':0, 'numFlights':3, 'exactFlights':True,
           'targets':[180,180,180,180,180], 'maxScore':540,
           'flightPickable':True, 'taskC':True},
    'D':  {'id':'D',  'name':'Task D',  'desc':'Two Flights',
           'window':600, 'numFlights':2, 'exactFlights':True,
           'targets':[300,300], 'maxScore':600},
    'E':  {'id':'E',  'name':'Task E',  'desc':'Poker',
           'window':600, 'numFlights':99, 'poker':True, 'pokerTargets':3,
           'targets':[], 'maxScore':600, 'windowPickable':True},
    'F':  {'id':'F',  'name':'Task F',  'desc':'3 of 6',
           'window':600, 'numFlights':6, 'bestOf':3,
           'capPerFlight':180, 'maxScore':540},
    'G':  {'id':'G',  'name':'Task G',  'desc':'Five Longest Flights',
           'window':600, 'numFlights':99, 'bestOf':5,
           'capPerFlight':120, 'maxScore':600},
    'H':  {'id':'H',  'name':'Task H',  'desc':'1-2-3-4 Min Targets',
           'window':600, 'numFlights':99, 'assignTargets':True,
           'targets':[240,180,120,60], 'maxScore':600},
    'I':  {'id':'I',  'name':'Task I',  'desc':'Three Longest Flights',
           'window':600, 'numFlights':99, 'bestOf':3,
           'capPerFlight':200, 'maxScore':600},
    'J':  {'id':'J',  'name':'Task J',  'desc':'Three Last Flights',
           'window':600, 'numFlights':99, 'lastN':3,
           'targets':[180,180,180], 'maxScore':540},
    'K':  {'id':'K',  'name':'Task K',  'desc':'Big Ladder',
           'window':600, 'numFlights':5, 'exactFlights':True,
           'targets':[60,90,120,150,180], 'maxScore':600},
    'L':  {'id':'L',  'name':'Task L',  'desc':'One Flight',
           'window':600, 'numFlights':1,
           'targets':[599], 'maxScore':599},
    'M':  {'id':'M',  'name':'Task M',  'desc':'Huge Ladder (Fly-off)',
           'window':900, 'numFlights':3, 'exactFlights':True,
           'targets':[180,300,420], 'maxScore':900},
    'N':  {'id':'N',  'name':'Task N',  'desc':'Best Flight',
           'window':600, 'numFlights':99, 'bestOf':1,
           'targets':[599], 'maxScore':599},
    'LL': {'id':'LL', 'name':'Low Launch', 'desc':'Low Launch (Custom)',
           'window':180, 'numFlights':99, 'scoredLL':True,
           'maxScore':None},
}

TASK_ORDER = ['LL','A','B','C','D','E','F','G','H','I','J','K','L','M','N']

# ─────────────────────────────────────────────
#  Task C sub-state machine
#  Phase: READY | SIGNAL | FLYING | LANDING_WIN | PREP | DONE
# ─────────────────────────────────────────────
TASKC_READY      = 'READY'
TASKC_FLYING     = 'FLYING'
TASKC_LANDING    = 'LANDING_WIN'
TASKC_PREP       = 'PREP'
TASKC_DONE       = 'DONE'

taskc_state = {
    'phase':        TASKC_READY,
    'num_attempts': 3,
    'attempt':      0,       # 1-indexed current attempt
    'phase_start':  None,    # time.time() when current phase started
}

def taskc_phase_elapsed(now=None):
    if taskc_state['phase_start'] is None:
        return 0.0
    return (now or time.time()) - taskc_state['phase_start']

def taskc_phase_remaining(duration_s, now=None):
    return max(0.0, duration_s - taskc_phase_elapsed(now))

def taskc_advance(now=None):
    """Advance Task C to the next phase. Called from build_state_json."""
    global taskc_state, _c_group_advanced
    now = now or time.time()
    ph  = taskc_state['phase']

    if ph == TASKC_FLYING:
        elapsed = taskc_phase_elapsed(now)
        if elapsed >= TASK_C_CAP_S:
            # Flight cap reached → open landing window
            taskc_state['phase']       = TASKC_LANDING
            taskc_state['phase_start'] = now
            print(f"[TASKC] Attempt {taskc_state['attempt']} → LANDING WIN")
            # Zero any units still airborne
            _taskc_zero_airborne()

    elif ph == TASKC_LANDING:
        elapsed = taskc_phase_elapsed(now)
        if elapsed >= LANDING_WINDOW_S:
            # Landing window closed → zero stragglers, start prep
            _taskc_zero_airborne()
            taskc_state['phase']       = TASKC_PREP
            taskc_state['phase_start'] = now
            print(f"[TASKC] Attempt {taskc_state['attempt']} → PREP")

    elif ph == TASKC_PREP:
        elapsed = taskc_phase_elapsed(now)
        if elapsed >= TASK_C_PREP_S:
            # Prep done — start next attempt or finish
            attempt = taskc_state['attempt']
            if attempt >= taskc_state['num_attempts']:
                taskc_state['phase'] = TASKC_DONE
                # Final normalisation
                _normalise_taskc()
                print("[TASKC] All attempts complete")
                # Contest auto-advance — same trigger point as WIN_CLOSED
                # for standard tasks. Guard reset happens below.
                _c_group_advanced = False
                contest_auto_advance()
            else:
                taskc_state['attempt']     += 1
                taskc_state['phase']        = TASKC_FLYING
                taskc_state['phase_start']  = now
                _c_group_advanced          = False  # reset for eventual DONE
                print(f"[TASKC] Attempt {taskc_state['attempt']} → FLYING")

def _taskc_zero_airborne():
    """Zero the current in-progress flight for any unit still airborne."""
    for u in units.values():
        if u['state'] in (STATE_FLIGHT, STATE_LAUNCH_WINDOW):
            # Record a zero flight for this attempt
            u['task_flights'].append({'dur': 0.0, 'lh_ft': 0.0, 'dq': True})
            _recompute_taskc_score(u)
            print(f"[TASKC] Unit {u['id']:02d} zeroed (still airborne)")

def _recompute_taskc_score(u):
    """Recompute Task C total for a single unit."""
    total = 0.0
    parts = []
    for f in u['task_flights']:
        raw     = min(f['dur'], TASK_C_CAP_S)
        adj     = 0.0 if f.get('dq') else raw - f['lh_ft'] * PENALTY_PER_FOOT
        total  += adj
        parts.append(f"{adj:+.1f}")
    u['task_raw_s']    = total
    u['task_detail']   = ' + '.join(parts) if parts else ''
    n = taskc_state['num_attempts']
    done = len(u['task_flights'])
    u['task_progress'] = f"Attempt {done}/{n}"

def _normalise_taskc():
    """Final FAI normalisation for Task C."""
    scored = [u for u in units.values() if u['task_flights']]
    if not scored:
        return
    best = max(u['task_raw_s'] for u in scored)
    for u in scored:
        if best > 0:
            u['normalised_score'] = round(u['task_raw_s'] / best * 1000, 1)
        else:
            u['normalised_score'] = 0.0

def taskc_start(num_attempts):
    """Initialise and start Task C."""
    global taskc_state
    taskc_state = {
        'phase':        TASKC_FLYING,
        'num_attempts': num_attempts,
        'attempt':      1,
        'phase_start':  time.time(),
    }
    # Reset all unit task flights
    for u in units.values():
        u['task_flights']     = []
        u['task_raw_s']       = 0.0
        u['task_detail']      = ''
        u['task_progress']    = f"Attempt 0/{num_attempts}"
        u['normalised_score'] = None
    print(f"[TASKC] Started — {num_attempts} attempts")

# ─────────────────────────────────────────────
#  Task Scoring Engine  (non-Task-C)
# ─────────────────────────────────────────────
def score_task(task_id, flights, task_cfg_override=None):
    """
    Score a pilot's flights for the given task.

    Formula per flight: adj = min(dur, cap) - lh_ft × K
    Task score = aggregate of selected adjusted flight scores.

    LL and Task C have their own scoring paths; all other tasks go
    through this function.
    """
    if not flights:
        return {'raw_s':0.0,'detail':'No flights yet','flights_used':[],'progress':''}

    t    = {**TASKS.get(task_id, {}), **(task_cfg_override or {})}
    K    = PENALTY_PER_FOOT
    n    = len(flights)

    def adj(dur, lh_ft, cap_s):
        """Capped duration minus launch penalty."""
        return min(dur, cap_s) - lh_ft * K

    def fmt(dur, lh_ft, cap_s):
        """Human-readable working: 'min(87.3,120)-11.0=76.3'"""
        capped = min(dur, cap_s)
        pen    = lh_ft * K
        a      = capped - pen
        cap_str = f"cap{cap_s}" if dur > cap_s else f"{dur:.1f}"
        return f"{cap_str}-{pen:.1f}={a:.1f}"

    # ── Low Launch ───────────────────────────────────────────────
    if task_id == 'LL':
        scores = [f['dur'] - f['lh_ft'] * K for f in flights]
        return {'raw_s':sum(scores),
                'detail':'  '.join(f"{s:+.1f}s" for s in scores),
                'flights_used':list(range(n)), 'progress':f"{n} flights"}

    # ── Task A — last flight, cap 480s (or 600s window variant) ──
    if task_id == 'A':
        c = t.get('targets',[480])[0]
        f = flights[-1]
        s = adj(f['dur'], f['lh_ft'], c)
        return {'raw_s':s,
                'detail':f"Last: {fmt(f['dur'],f['lh_ft'],c)}s",
                'flights_used':[n-1], 'progress':f"{n} flights"}

    # ── Task B — sum of last 2 flights, cap 240s each ────────────
    if task_id == 'B':
        fs     = flights[-2:] if n>=2 else flights
        scores = [adj(f['dur'],f['lh_ft'],240) for f in fs]
        return {'raw_s':sum(scores),
                'detail':' + '.join(f"{s:.1f}" for s in scores),
                'flights_used':list(range(max(0,n-2),n)),
                'progress':f"{n} flights, need ≥2"}

    # ── Task D — first 2 flights, cap 300s each ──────────────────
    if task_id == 'D':
        fs     = flights[:2]
        scores = [adj(f['dur'],f['lh_ft'],300) for f in fs]
        return {'raw_s':sum(scores),
                'detail':' + '.join(f"{s:.1f}" for s in scores),
                'flights_used':list(range(len(fs))),
                'progress':f"{n}/2 flights"}

    # ── Task E — Poker: best 3 flights, no cap ───────────────────
    if task_id == 'E':
        # No flight cap in Poker — penalty still applies
        scores = sorted([f['dur'] - f['lh_ft']*K for f in flights], reverse=True)[:3]
        return {'raw_s':sum(scores),
                'detail':' + '.join(f"{s:.1f}" for s in scores),
                'flights_used':[], 'progress':f"{n} flights, up to 3 targets"}

    # ── Task F — best 3 of up to 6, cap 180s each ────────────────
    if task_id == 'F':
        scores = sorted([adj(f['dur'],f['lh_ft'],180) for f in flights],
                        reverse=True)[:3]
        return {'raw_s':sum(scores),
                'detail':' + '.join(f"{s:.1f}" for s in scores),
                'flights_used':[], 'progress':f"{n}/6 flights"}

    # ── Task G — best 5, cap 120s each ───────────────────────────
    if task_id == 'G':
        scores = sorted([adj(f['dur'],f['lh_ft'],120) for f in flights],
                        reverse=True)[:5]
        return {'raw_s':sum(scores),
                'detail':' + '.join(f"{s:.1f}" for s in scores),
                'flights_used':[], 'progress':f"{n} flights"}

    # ── Task H — best 4 flights assigned to targets 4/3/2/1 min ─
    if task_id == 'H':
        targets = [240, 180, 120, 60]
        # Sort flights by duration descending, assign to targets
        best4 = sorted(flights, key=lambda f: f['dur'], reverse=True)[:4]
        while len(best4) < 4:
            best4.append({'dur':0.0,'lh_ft':0.0})
        scores = [adj(f['dur'],f['lh_ft'],tgt)
                  for f,tgt in zip(best4, targets)]
        detail = ' + '.join(f"{s:.1f}(cap{t})" for s,t in zip(scores,targets))
        return {'raw_s':sum(scores), 'detail':detail,
                'flights_used':[], 'progress':f"{n} flights, need 4"}

    # ── Task I — best 3, cap 200s each ───────────────────────────
    if task_id == 'I':
        scores = sorted([adj(f['dur'],f['lh_ft'],200) for f in flights],
                        reverse=True)[:3]
        return {'raw_s':sum(scores),
                'detail':' + '.join(f"{s:.1f}" for s in scores),
                'flights_used':[], 'progress':f"{n} flights"}

    # ── Task J — last 3 flights, cap 180s each ───────────────────
    if task_id == 'J':
        fs     = flights[-3:] if n>=3 else flights
        scores = [adj(f['dur'],f['lh_ft'],180) for f in fs]
        return {'raw_s':sum(scores),
                'detail':' + '.join(f"{s:.1f}" for s in scores),
                'flights_used':list(range(max(0,n-3),n)),
                'progress':f"{n} flights, need ≥3"}

    # ── Task K — Big Ladder: flights 1–5 capped at 60/90/120/150/180s
    if task_id == 'K':
        targets = [60, 90, 120, 150, 180]
        scores  = [adj(flights[i]['dur'], flights[i]['lh_ft'], targets[i])
                   if i < n else 0.0
                   for i in range(5)]
        detail  = ' + '.join(f"{s:.1f}(cap{t})"
                             for s,t in zip(scores, targets))
        return {'raw_s':sum(scores), 'detail':detail,
                'flights_used':list(range(min(n,5))),
                'progress':f"Flight {min(n,5)}/5"}

    # ── Task L — single flight, cap 599s ─────────────────────────
    if task_id == 'L':
        f = flights[-1]
        s = adj(f['dur'], f['lh_ft'], 599)
        return {'raw_s':s,
                'detail':f"Last: {fmt(f['dur'],f['lh_ft'],599)}s",
                'flights_used':[n-1], 'progress':"1 flight"}

    # ── Task M — Huge Ladder: 3 flights capped 180/300/420s ──────
    if task_id == 'M':
        targets = [180, 300, 420]
        scores  = [adj(flights[i]['dur'], flights[i]['lh_ft'], targets[i])
                   if i < n else 0.0
                   for i in range(3)]
        detail  = ' + '.join(f"{s:.1f}(cap{t})"
                             for s,t in zip(scores, targets))
        return {'raw_s':sum(scores), 'detail':detail,
                'flights_used':list(range(min(n,3))),
                'progress':f"Flight {min(n,3)}/3"}

    # ── Task N — single best flight, cap 599s ────────────────────
    if task_id == 'N':
        best_f = max(flights, key=lambda f: f['dur'])
        s      = adj(best_f['dur'], best_f['lh_ft'], 599)
        return {'raw_s':s,
                'detail':f"Best: {fmt(best_f['dur'],best_f['lh_ft'],599)}s",
                'flights_used':[], 'progress':f"{n} flights"}

    return {'raw_s':0.0,'detail':'Unknown task','flights_used':[],'progress':''}


def normalise_scores(unit_list, task_id):
    if task_id in ('LL', 'C') or not unit_list:
        if task_id != 'C':
            for u in unit_list:
                u['normalised_score'] = None
        return
    scores = [u.get('task_raw_s',0.0) for u in unit_list]
    max_s  = max(scores) if scores else 0.0
    for u in unit_list:
        u['normalised_score'] = round(u.get('task_raw_s',0.0)/max_s*1000,1) if max_s>0 else 0.0


# ─────────────────────────────────────────────
#  Window state machine (non-Task-C tasks)
#
#  Phases:
#    READY       window_start_time is None
#    RUNNING     elapsed < active_window_s
#    LANDING_WIN elapsed in [active_window_s, active_window_s + 30]
#    CLOSED      elapsed > active_window_s + 30
# ─────────────────────────────────────────────
WIN_READY      = 'READY'
WIN_PREP       = 'PREP'        # preparation time (CD-controlled)
WIN_TESTING    = 'TESTING'     # flight testing (part of prep)
WIN_NOFLY      = 'NO_FLY'      # mandatory no-fly before window
WIN_RUNNING    = 'RUNNING'
WIN_LANDING    = 'LANDING_WIN'
WIN_CLOSED     = 'CLOSED'

_landing_win_zeroed = False  # flag so we only zero once per window
_window_scored      = False  # flag: working window close scored airborne flights

# ── Preparation sequence state ───────────────────────────────────
# Tracks the pre-window sequence: PREP → TESTING → NO_FLY → RUNNING
# prep_start_time is set when operator presses START (begins prep).
# The working window opens automatically at the end of NO_FLY.
prep_start_time  = None   # time.time() when prep started
prep_phase       = None   # None | 'PREP' | 'TESTING' | 'NO_FLY'

# Prep timing — prep and no-fly are fixed at 60s each.
# Only flight testing duration is CD-configurable (1–5 min).
PREP_S     = 60    # hardcoded preparation time (1 min)
NOFLY_S    = 60    # hardcoded no-fly duration   (1 min)

round_settings = {
    'prep_s':    PREP_S,              # fixed 60s
    'testing_s': TESTING_S_DEFAULT,   # from config.json, default 60s
    'nofly_s':   NOFLY_S,             # fixed 60s
}

# ── Pause state ──────────────────────────────────────────────────
# Pause is allowed during PREP/TESTING/NO_FLY/RUNNING only.
# We accumulate paused time so elapsed calculations stay accurate.
paused            = False          # is a pause currently active?
pause_start_time  = None           # when current pause began
total_paused_s    = 0.0            # total seconds paused this window

def _effective_elapsed(start_time, now=None):
    """Wall-clock elapsed minus any paused duration."""
    if start_time is None: return 0.0
    now = now or time.time()
    extra = (now - pause_start_time) if paused and pause_start_time else 0.0
    return (now - start_time) - total_paused_s - extra

def prep_elapsed(now=None):
    return _effective_elapsed(prep_start_time, now)

def get_prep_phase(now=None):
    """
    Returns current phase of the pre-window sequence.
    Phases are determined by elapsed time since prep_start.

    Timeline (example: prep=300, testing=45, nofly=60):
      T+0:00   PREP begins
      T+3:15   TESTING begins  (300 - 45 - 60 = 195s into prep)
      T+4:00   NO_FLY begins   (300 - 60 = 240s into prep)
      T+5:00   RUNNING begins  (prep complete, window opens)
    """
    if prep_start_time is None:
        return None
    now     = now or time.time()
    elapsed = prep_elapsed(now)
    testing_s = round_settings['testing_s']
    total     = PREP_S + testing_s + NOFLY_S
    # Timeline: PREP (60s) → TESTING (n min) → NO_FLY (60s)
    if elapsed < PREP_S:                      return WIN_PREP
    if elapsed < PREP_S + testing_s:          return WIN_TESTING
    if elapsed < PREP_S + testing_s + NOFLY_S: return WIN_NOFLY
    return None  # prep complete — working window now active

def prep_phase_remaining(now=None):
    """Seconds remaining in the current prep phase."""
    now       = now or time.time()
    ph        = get_prep_phase(now)
    el        = prep_elapsed(now)
    testing_s = round_settings['testing_s']
    if ph == WIN_PREP:
        return max(0.0, PREP_S - el)
    if ph == WIN_TESTING:
        return max(0.0, (PREP_S + testing_s) - el)
    if ph == WIN_NOFLY:
        return max(0.0, (PREP_S + testing_s + NOFLY_S) - el)
    return 0.0

def get_window_phase(now=None):
    now = now or time.time()
    if active_task == 'C':
        return None   # Task C has its own state machine
    # If prep sequence is active, return its current phase
    if prep_start_time is not None and window_start_time is None:
        ph = get_prep_phase(now)
        if ph is not None:
            return ph
        # Prep complete — auto-open working window
        return WIN_RUNNING
    if window_start_time is None:
        return WIN_READY
    elapsed = _effective_elapsed(window_start_time, now)
    if elapsed < active_window_s:
        return WIN_RUNNING
    if elapsed < active_window_s + LANDING_WINDOW_S:
        return WIN_LANDING
    return WIN_CLOSED

def maybe_zero_landing_window(now=None):
    """
    WIN_LANDING: working window just expired. Snapshot every airborne unit's
    current duration as their final score. Score is frozen — physical landing
    does not change it.

    WIN_CLOSED: landing window expired. DQ any unit still airborne (never
    received a land packet and wasn't frozen). Normalise and auto-advance.
    """
    global _landing_win_zeroed, _window_scored
    now   = now or time.time()
    phase = get_window_phase(now)

    if phase == WIN_LANDING and not _window_scored:
        _window_scored = True
        _TASK_CAPS = {'A':480,'B':240,'D':300,'F':180,'G':120,
                      'I':200,'J':180,'L':599,'N':599,'C':TASK_C_CAP_S}
        for u in units.values():
            if u['state'] in (STATE_FLIGHT, STATE_LAUNCH_WINDOW):
                raw    = u.get('raw_time_s', 0.0)
                lh     = u.get('launch_height_ft', 0.0)
                cap    = _TASK_CAPS.get(active_task)
                capped = round(min(raw, cap), 1) if cap else round(raw, 1)
                u['task_flights'].append({
                    'dur': raw, 'lh_ft': lh, 'dq': False,
                    'capped_dur': capped,
                    'peak_alt_ft': round(u.get('peak_alt_ft', 0.0), 1),
                })
                u['window_scored'] = True
                result = score_task(active_task, u['task_flights'], task_cfg_ovr)
                u['task_raw_s']    = result['raw_s']
                u['task_detail']   = result['detail']
                u['task_progress'] = result['progress']
                print(f"[WIN] Unit {u['id']:02d} frozen at window close: {raw:.1f}s")

    if phase == WIN_CLOSED and not _landing_win_zeroed:
        _landing_win_zeroed = True
        for u in units.values():
            if u['state'] in (STATE_FLIGHT, STATE_LAUNCH_WINDOW) and not u.get('window_scored'):
                u['task_flights'].append({'dur':0.0,'lh_ft':0.0,'dq':True,
                                           'capped_dur':0.0,'peak_alt_ft':0.0})
                result = score_task(active_task, u['task_flights'], task_cfg_ovr)
                u['task_raw_s']    = result['raw_s']
                u['task_detail']   = result['detail']
                u['task_progress'] = result['progress']
                print(f"[WIN] Unit {u['id']:02d} DQ — never landed")
        active_units = [u for u in units.values() if not u['stale']]
        normalise_scores(active_units, active_task)
        for au in active_units:
            units[au['id']]['normalised_score'] = au['normalised_score']
        # Register active units as pending summary delivery
        _register_pending_summaries(active_units)
        contest_auto_advance()

    elif phase in (WIN_READY, WIN_RUNNING):
        _landing_win_zeroed = False
        _window_scored      = False
        for u in units.values():
            u['window_scored'] = False
        global _c_group_advanced
        _c_group_advanced = False

# ─────────────────────────────────────────────
#  Shared state
# ─────────────────────────────────────────────
units              = {}
pilots             = {}
roster             = list(config.get('roster', []))  # master pilot list
checkins           = {}       # {name: uid|None} — checked-in pilots with optional pre-assigned unit
groups             = []       # pre-loaded groups: [{'name':str,'pilots':{uid:name}}]
active_group_uids  = set()    # unit IDs in the currently active group (empty=show all)
active_task        = 'LL'
task_cfg_ovr       = {}
active_window_s   = 180   # LL default
window_start_time = None
prep_start_time   = None
_landing_win_zeroed = False
server_start = time.time()
packet_count = 0

PACKET_SIZE          = 14
WINDOW_CMD_PORT      = 5006   # scorer → units (broadcast)
LOG_ANNOUNCE_PORT    = 4214   # units → scorer (Packet 5)
UNIT_HTTP_PORT       = 80     # HTTP server on each unit
WINDOW_CMD_TYPE      = 0x20   # packet_type for window start
PREP_CMD_TYPE        = 0x21   # packet_type for prep countdown broadcast

LOG_ANNOUNCE_TYPE    = 0x10   # packet_type for log available
LOGS_DIR             = "logs" # local directory for retrieved CSVs

STATE_GROUND         = 0
STATE_LAUNCH_WINDOW  = 1
STATE_FLIGHT         = 2
STATE_LANDED         = 3
STATE_NAMES = {0:'GROUND',1:'LAUNCH WIN',2:'FLIGHT',3:'LANDED'}


def make_unit(uid):
    return {
        'id':uid, 'state':STATE_GROUND, 'state_name':'GROUND',
        'altitude_ft':0.0, 'peak_alt_ft':0.0, 'launch_height_ft':0.0,
        'duration_s':0.0, 'battery_pct':100,
        'last_seen':time.time(), 'last_seen_str':'', 'stale':False,
        'raw_time_s':0.0, 'penalty_s':0.0, 'adjusted_score_s':0.0,
        'task_flights':[], 'task_raw_s':0.0,
        'task_detail':'', 'task_progress':'', 'normalised_score':None,
        'flight_count':0, 'best_raw_s':0.0,
        'best_adjusted_s':None, 'best_launch_ft':0.0, 'best_task_raw_s':0.0,
        'window_scored':    False,
        'alt_history':[]    # [[elapsed_s, alt_ft], ...] during working window
    }


def decode_packet(data):
    if len(data) != PACKET_SIZE:
        return None
    try:
        return {
            'unit_id':          data[0],
            'state':            data[1],
            'altitude_ft':      ((data[2]<<8)|data[3])/10.0,
            'timestamp_ms':     (data[4]<<24)|(data[5]<<16)|(data[6]<<8)|data[7],
            'duration_s':       ((data[8]<<8)|data[9])/10.0,
            'peak_alt_ft':      float(data[10]),
            'launch_height_ft': ((data[11]<<8)|data[12])/10.0,
            'battery_pct':      data[13],
        }
    except Exception as e:
        print(f"[UDP] Decode error: {e}"); return None


def update_unit(pkt, src_ip=None):
    global packet_count
    packet_count += 1
    uid = pkt['unit_id']
    if uid < 1 or uid > 200: return

    now = time.time()
    if uid not in units:
        units[uid] = make_unit(uid)
        print(f"[SERVER] New unit: {uid:02d}")

    u          = units[uid]
    prev_state = u['state']

    u['state']            = pkt['state']
    u['state_name']       = STATE_NAMES.get(pkt['state'],'UNKNOWN')
    u['altitude_ft']      = pkt['altitude_ft']
    u['peak_alt_ft']      = pkt['peak_alt_ft']
    u['launch_height_ft'] = pkt['launch_height_ft']
    u['duration_s']       = pkt['duration_s']
    u['battery_pct']      = pkt['battery_pct']
    u['last_seen']        = now
    u['last_seen_str']    = datetime.now().strftime('%H:%M:%S')
    u['stale']            = False
    if src_ip:
        unit_ips[uid] = src_ip

    # Record altitude history during the working window (RUNNING phase only)
    if active_task != 'C' and window_start_time is not None:
        elapsed = _effective_elapsed(window_start_time, now)
        if 0 <= elapsed <= active_window_s + LANDING_WINDOW_S:
            history = u['alt_history']
            # Sample at most every 0.5s to cap points at ~2400 per 10min window
            if not history or (elapsed - history[-1][0]) >= 0.5:
                history.append([round(elapsed, 1), round(pkt['altitude_ft'], 1)])
                if len(history) > 3600:  # hard cap
                    history.pop(0)

    if pkt['state'] in (STATE_LAUNCH_WINDOW, STATE_FLIGHT, STATE_LANDED):
        raw = pkt['duration_s']; lh = pkt['launch_height_ft']
        u['raw_time_s']       = raw
        u['penalty_s']        = round(lh * PENALTY_PER_FOOT, 2)
        u['adjusted_score_s'] = round(raw - lh * PENALTY_PER_FOOT, 2)

    if pkt['state'] == STATE_LANDED and prev_state != STATE_LANDED:
        u['flight_count'] += 1
        raw = pkt['duration_s']; lh = pkt['launch_height_ft']

        # Score frozen at window close — physical landing doesn't change score
        if u.get('window_scored'):
            print(f"[WIN] Unit {uid:02d} physical landing — score frozen")
            return

        adj = round(raw - lh * PENALTY_PER_FOOT, 2)

        # Per-task flight time cap — stored so the flight log can
        # display raw vs scorable without re-running score_task
        _TASK_CAPS = {
            'A':480,'B':240,'D':300,'F':180,'G':120,
            'I':200,'J':180,'L':599,'N':599,
            'C':TASK_C_CAP_S,
        }
        _cap_val  = _TASK_CAPS.get(active_task)
        _capped   = round(min(raw, _cap_val), 1) if _cap_val else round(raw, 1)

        if active_task == 'C':
            u['task_flights'].append({
                'dur':raw, 'lh_ft':lh, 'dq':False,
                'capped_dur':_capped,
                'peak_alt_ft':round(u.get('peak_alt_ft', 0.0), 1),
            })
            _recompute_taskc_score(u)
        else:
            u['task_flights'].append({
                'dur':raw, 'lh_ft':lh,
                'capped_dur':_capped,
                'peak_alt_ft':round(u.get('peak_alt_ft', 0.0), 1),
            })
            result = score_task(active_task, u['task_flights'], task_cfg_ovr)
            u['task_raw_s']    = result['raw_s']
            u['task_detail']   = result['detail']
            u['task_progress'] = result['progress']

        if raw > u['best_raw_s']:          u['best_raw_s'] = raw
        if u['best_adjusted_s'] is None or adj > u['best_adjusted_s']:
            u['best_adjusted_s'] = adj; u['best_launch_ft'] = lh
        if u['task_raw_s'] > u['best_task_raw_s']:
            u['best_task_raw_s'] = u['task_raw_s']

        print(f"[SERVER] Unit {uid:02d} ({pilots.get(uid,'?')}) landed  "
              f"dur:{raw:.1f}s  lh:{lh:.1f}ft  adj:{adj:.1f}s")

    if active_task != 'C':
        active_units = [u for u in units.values() if not u['stale']]
        normalise_scores(active_units, active_task)
        for au in active_units:
            units[au['id']]['normalised_score'] = au['normalised_score']


def mark_stale():
    now = time.time()
    for u in units.values():
        u['stale'] = (now - u['last_seen']) > STALE_TIMEOUT_S


def build_state_json():
    now = time.time()
    mark_stale()

    # Advance Task C state machine
    if active_task == 'C' and taskc_state['phase'] not in (TASKC_READY, TASKC_DONE):
        taskc_advance(now)

    # Check landing window for standard tasks
    if active_task != 'C':
        maybe_zero_landing_window(now)

    # Filter to active group if one is set
    all_units = sorted(units.values(), key=lambda u: u['id'])
    if active_group_uids:
        unit_list = [u for u in all_units if u['id'] in active_group_uids]
    else:
        unit_list = all_units
    # Fall back to checkins reverse map when no group is active
    checkin_by_uid = {uid: name for name, uid in checkins.items() if uid is not None}
    for u in unit_list:
        u['pilot_name'] = pilots.get(u['id']) or checkin_by_uid.get(u['id'], '')

    # Clamp displayed duration_s once the working window has closed.
    # Flights still airborne during LANDING_WIN or CLOSED should show
    # at most the window duration — the display timer stops even though
    # firmware keeps counting until the physical landing.
    _win_phase_now = get_window_phase(now) if active_task != 'C' else None
    _should_clamp  = _win_phase_now in (WIN_LANDING, WIN_CLOSED)
    if active_task == 'C':
        _should_clamp = taskc_state['phase'] in (TASKC_LANDING, TASKC_PREP, TASKC_DONE)
    if _should_clamp:
        for u in unit_list:
            if u['state'] in (STATE_FLIGHT, STATE_LAUNCH_WINDOW):
                _cap = TASK_C_CAP_S if active_task == 'C' else active_window_s
                u['duration_s'] = min(u['duration_s'], float(_cap))

    # ── Periodic prep countdown broadcast (0x21) ────────────────
    _maybe_broadcast_prep_countdown(now)

    # ── Auto-transition: prep complete → open working window ────
    global window_start_time, prep_start_time, _landing_win_zeroed, total_paused_s, paused, pause_start_time
    if active_task != 'C' and prep_start_time is not None and window_start_time is None:
        if get_prep_phase(now) is None:
            # Prep sequence finished — open the working window now.
            # Reset total_paused_s: any pause time accrued during prep
            # must not carry over into the working window elapsed calculation.
            window_start_time = now
            _landing_win_zeroed = False
            _window_scored      = False
            total_paused_s    = 0.0
            paused            = False
            pause_start_time  = None
            print("[SERVER] Prep complete — working window opened automatically")
            _broadcast_window_start(int(active_window_s))

    # ── Task C: same prep sequence, then auto-start attempts ─────
    # prep_start_time drives PREP→TESTING→NO_FLY for Task C too.
    # When prep completes, call taskc_start() automatically.
    if active_task == 'C' and prep_start_time is not None:
        if taskc_state['phase'] == TASKC_READY and get_prep_phase(now) is None:
            # Prep done — launch Task C
            total_paused_s = 0.0
            paused         = False
            pause_start_time = None
            num = taskc_state.get('num_attempts', 3)
            taskc_start(num)
            print("[SERVER] Task C prep complete — attempts started automatically")

    # ── Window state for UI ──────────────────────────────────────
    if active_task == 'C':
        ph = taskc_state['phase']
        tc_attempt = taskc_state['attempt']
        tc_total   = taskc_state['num_attempts']

        if ph == TASKC_READY:
            # Show prep phases if prep sequence is running
            if prep_start_time is not None and get_prep_phase(now) is not None:
                prep_ph = get_prep_phase(now)
                rs      = round_settings
                win_phase = ('PREP'    if prep_ph == WIN_PREP else
                             'TESTING' if prep_ph == WIN_TESTING else 'NO_FLY')
                win_remaining = prep_phase_remaining(now)
                win_total     = (PREP_S                if prep_ph == WIN_PREP else
                                 rs['testing_s']        if prep_ph == WIN_TESTING else
                                 NOFLY_S)
            else:
                win_phase = 'READY'; win_remaining = None; win_total = TASK_C_CAP_S
        elif ph == TASKC_FLYING:
            win_phase     = 'RUNNING'
            win_remaining = taskc_phase_remaining(TASK_C_CAP_S, now)
            win_total     = TASK_C_CAP_S
        elif ph == TASKC_LANDING:
            win_phase     = 'LANDING_WIN'
            win_remaining = taskc_phase_remaining(LANDING_WINDOW_S, now)
            win_total     = LANDING_WINDOW_S
        elif ph == TASKC_PREP:
            win_phase     = 'NO_FLY'   # inter-attempt no-fly period
            win_remaining = taskc_phase_remaining(TASK_C_PREP_S, now)
            win_total     = TASK_C_PREP_S
        else:
            win_phase = 'DONE'; win_remaining = 0; win_total = 0

        win_running = ph in (TASKC_FLYING, TASKC_LANDING, TASKC_PREP)
        taskc_info  = {'phase':ph, 'attempt':tc_attempt, 'total':tc_total}
    else:
        ph = get_window_phase(now)
        tc_attempt = 0; tc_total = 0; taskc_info = None
        rs = round_settings

        if ph == WIN_READY:
            win_phase = 'READY'; win_remaining = float(active_window_s)
            win_total = active_window_s; win_running = False

        elif ph == WIN_PREP:
            win_phase     = 'PREP'
            win_remaining = prep_phase_remaining(now)
            win_total     = rs['prep_s'] - rs['testing_s'] - rs['nofly_s']
            win_running   = True

        elif ph == WIN_TESTING:
            win_phase     = 'TESTING'
            win_remaining = prep_phase_remaining(now)
            win_total     = rs['testing_s']
            win_running   = True

        elif ph == WIN_NOFLY:
            win_phase     = 'NO_FLY'
            win_remaining = prep_phase_remaining(now)
            win_total     = rs['nofly_s']
            win_running   = True

        elif ph == WIN_RUNNING:
            elapsed = _effective_elapsed(window_start_time, now)
            win_phase = 'RUNNING'; win_remaining = max(0.0, active_window_s - elapsed)
            win_total = active_window_s; win_running = True
        elif ph == WIN_LANDING:
            elapsed    = _effective_elapsed(window_start_time, now)
            lw_elapsed = elapsed - active_window_s
            win_phase = 'LANDING_WIN'; win_remaining = max(0.0, LANDING_WINDOW_S - lw_elapsed)
            win_total = LANDING_WINDOW_S; win_running = True
        else:
            win_phase = 'CLOSED'; win_remaining = 0; win_total = 0; win_running = False

    # Audio state-diff engine
    if not getattr(audio_tick, '_disabled', False):
        tc = taskc_state
        audio_tick(
            win_phase, win_remaining, win_total,
            tc['phase'], tc['attempt'], tc['num_attempts'],
            active_task,
            TASKS.get(active_task,{}).get('name',''),
            TASKS.get(active_task,{}).get('desc',''),
            active_window_s,
        )

    return json.dumps({
        'units':            [{**u,
            'alt_history': u['alt_history'][::2] if len(u.get('alt_history',[])) > 600
                           else u.get('alt_history',[])}
            for u in unit_list],
        'pilots':           {str(k):v for k,v in pilots.items()},
        'active_task':      active_task,
        'task_info':        TASKS.get(active_task,{}),
        'task_list':        [{'id':tid,'name':TASKS[tid]['name'],'desc':TASKS[tid]['desc']}
                             for tid in TASK_ORDER],
        'uptime_s':         int(now - server_start),
        'packet_count':     packet_count,
        'timestamp':        datetime.now().strftime('%H:%M:%S'),
        'penalty_per_foot': PENALTY_PER_FOOT,
        'session_name':     SESSION_NAME,
        'win_phase':        win_phase,
        'win_remaining':    win_remaining,
        'win_total':        win_total,
        'win_running':      win_running,
        'taskc':            taskc_info,
        'landing_window_s': LANDING_WINDOW_S,
        'round_settings':   round_settings,
        'paused':           paused,
        'roster':           roster,
        'checkins':         checkins,
        'debug':            {str(k): v for k,v in debug_data.items()},
        'log_status':       {str(k): v for k,v in _log_status.items()},
        'groups':           groups,
        'active_group_uids':sorted(active_group_uids),
        'contest':          contest_state_dict(),
        'vault': {
            'available':   _VAULT_AVAILABLE,
            'pulled':      bool(_vault_pull_data),
            'event_id':    _vault_pull_data.get('event_id', 0),
            'event_name':  _vault_pull_data.get('event_name', ''),
            'matched':     len(_vault_pilot_map),
            'review':      len(_vault_match_report.review) if _vault_match_report else 0,
            'unmatched':   len(_vault_match_report.unmatched) if _vault_match_report else 0,
        },
    })


# ─────────────────────────────────────────────
#  UDP Listener
# ─────────────────────────────────────────────
class UDPListener(asyncio.DatagramProtocol):
    def datagram_received(self, data, addr):
        pkt = decode_packet(data)
        if pkt:
            update_unit(pkt, addr[0])
        else:
            print(f"[UDP] Bad packet ({len(data)}b) from {addr}")
    def error_received(self, exc):
        print(f"[UDP] Error: {exc}")

async def start_udp(host, port):
    loop = asyncio.get_running_loop()
    print(f"[UDP] Listening {host}:{port}")
    await loop.create_datagram_endpoint(UDPListener, local_addr=(host, port))



# ─────────────────────────────────────────────
#  Port 4211 — GPS Data
#  Port 4212 — Flight Metrics (5Hz viz + launch events)
#
#  These listeners are completely independent of the
#  scoring system on port 4210. Packets are silently
#  dropped if the format is unrecognised.
#
#  GPS Packet — 16 bytes, big-endian:
#    0     unit_id         uint8
#    1     fix_quality     uint8   0=none 1=GPS 2=DGPS 6=est
#    2     satellites      uint8
#    3     hdop_x10        uint8   e.g. 12 → 1.2 HDOP
#    4-7   latitude_e5     int32   e.g. 3385000 → 33.85000°
#    8-11  longitude_e5    int32
#    12-13 altitude_m_x10  int16   MSL altitude in decimetres
#    14-15 spare           uint8×2
#
#  Flight Metrics Packet — 20 bytes, big-endian:
#    0     unit_id         uint8
#    1     packet_type     uint8   0x00=5Hz viz  0x01=launch event
#
#    Type 0x00 — 5 Hz visualization frame:
#    2-3   altitude_ft_x10 uint16  current altitude × 10
#    4-5   sink_rate_x10   int16   ft/s × 10, negative = descending
#    6     state           uint8   mirrors scoring packet state
#    7-19  spare           uint8×13
#
#    Type 0x01 — Launch event (one-shot per launch):
#    2-3   launch_ht_ft_x10   uint16  peak altitude during window × 10
#    4-5   peak_climb_fpm_x10 uint16  peak climb rate ft/min × 10
#    6-7   time_to_peak_ms    uint16  ms from threshold crossing to peak
#    8-9   climb_energy_x10   uint16  sum of alt samples during ascent × 10
#    10-19 spare              uint8×10
# ─────────────────────────────────────────────

GPS_PORT    = 4211
FLIGHT_PORT = 4212
DEBUG_PORT  = 4213

GPS_PACKET_SIZE    = 16
FLIGHT_PACKET_SIZE = 20

PKT_TYPE_VIZ    = 0x00   # 5 Hz visualization frame
PKT_TYPE_LAUNCH = 0x01   # One-shot launch event

# Per-unit state — populated by incoming packets
# Both dicts keyed by unit_id (int 1-20)
gps_data     = {}   # uid → latest GPS fix dict
unit_ips     = {}   # uid → last known IP address (from UDP source)
_window_id   = 0    # monotonic counter, incremented each window open
_log_queue   = []   # [(uid, window_number, window_id, ip)] pending retrieval
_log_status  = {}   # uid → {'status':..., 'window_num':..., 'file':..., 'size':...}
              # status: none | announced | fetching | complete | error
_pending_summaries = {}  # window_id → set of uids still awaiting summary
_summary_window_id = None  # window_id for current pending group
_summary_timeout_t = None  # deadline: score without summaries after this
_last_prep_broadcast = 0.0  # time.time() of last 0x21 prep countdown broadcast
_log_retry_queue = {}       # uid → {ip, window_num, window_id, attempts, next_retry_t}
SUMMARY_TIMEOUT_S  = int(config.get('network', {}).get('summary_timeout_s', 20))

debug_data   = {}   # uid → latest Packet 4 debug/health dict
flight_data  = {}   # uid → latest 5Hz frame dict
launch_events = {}  # uid → list of launch event dicts (newest first, max 10)


def decode_gps_packet(data):
    """Decode a 16-byte GPS packet. Returns dict or None."""
    if len(data) != GPS_PACKET_SIZE:
        return None
    try:
        uid, fix_q, sats, hdop_x10 = struct.unpack_from('>BBBB', data, 0)
        lat_e5,  = struct.unpack_from('>i', data, 4)
        lon_e5,  = struct.unpack_from('>i', data, 8)
        alt_x10, = struct.unpack_from('>h', data, 12)
        if uid < 1 or uid > 200:
            return None
        return {
            'unit_id':     uid,
            'fix_quality': fix_q,
            'satellites':  sats,
            'hdop':        round(hdop_x10 / 10.0, 1),
            'latitude':    round(lat_e5  / 1e5, 5),
            'longitude':   round(lon_e5  / 1e5, 5),
            'altitude_m':  round(alt_x10 / 10.0, 1),
            'timestamp':   time.time(),
        }
    except Exception as e:
        print(f"[GPS] Decode error: {e}")
        return None


def decode_flight_packet(data):
    """Decode a 20-byte flight metrics packet. Returns dict or None."""
    if len(data) != FLIGHT_PACKET_SIZE:
        return None
    try:
        uid, pkt_type = struct.unpack_from('>BB', data, 0)
        if uid < 1 or uid > 200:
            return None
        ts = time.time()

        if pkt_type == PKT_TYPE_VIZ:
            alt_x10,  = struct.unpack_from('>H', data, 2)
            sink_x10, = struct.unpack_from('>h', data, 4)
            state,    = struct.unpack_from('>B', data, 6)
            return {
                'unit_id':       uid,
                'packet_type':   'viz',
                'altitude_ft':   round(alt_x10  / 10.0, 1),
                'sink_rate_fps': round(sink_x10 / 10.0, 1),
                'state':         state,
                'timestamp':     ts,
            }

        elif pkt_type == PKT_TYPE_LAUNCH:
            lh_x10,  = struct.unpack_from('>H', data, 2)
            clmb_x10,= struct.unpack_from('>H', data, 4)
            ttp_ms,  = struct.unpack_from('>H', data, 6)
            energy,  = struct.unpack_from('>H', data, 8)
            return {
                'unit_id':          uid,
                'packet_type':      'launch',
                'launch_height_ft': round(lh_x10   / 10.0, 1),
                'peak_climb_fpm':   round(clmb_x10 / 10.0, 1),
                'time_to_peak_ms':  ttp_ms,
                'climb_energy':     round(energy   / 10.0, 1),
                'timestamp':        ts,
            }

        else:
            print(f"[FLIGHT] Unknown packet_type 0x{pkt_type:02x} from unit {uid}")
            return None

    except Exception as e:
        print(f"[FLIGHT] Decode error: {e}")
        return None


def update_gps(pkt):
    """Store latest GPS fix for a unit."""
    uid = pkt['unit_id']
    prev = gps_data.get(uid, {})
    gps_data[uid] = pkt
    # Log once when fix is first acquired or re-acquired
    if pkt['fix_quality'] > 0 and prev.get('fix_quality', 0) == 0:
        print(f"[GPS] Unit {uid:02d} fix acquired: "
              f"sats={pkt['satellites']}  hdop={pkt['hdop']}  "
              f"alt={pkt['altitude_m']}m  "
              f"({pkt['latitude']:.5f}, {pkt['longitude']:.5f})")


def update_flight(pkt):
    """Store latest flight metrics for a unit."""
    uid = pkt['unit_id']
    if pkt['packet_type'] == 'viz':
        flight_data[uid] = pkt
    elif pkt['packet_type'] == 'launch':
        if uid not in launch_events:
            launch_events[uid] = []
        launch_events[uid].insert(0, pkt)   # newest first
        launch_events[uid] = launch_events[uid][:10]  # keep last 10
        print(f"[FLIGHT] Unit {uid:02d} launch event  "
              f"ht={pkt['launch_height_ft']}ft  "
              f"climb={pkt['peak_climb_fpm']}fpm  "
              f"ttp={pkt['time_to_peak_ms']}ms")


def build_sensor_state_json():
    """
    Build JSON response for /gps_state and /imu_state endpoints.
    Returns a dict ready for json.dumps().
    """
    return {
        'gps':    {str(uid): d for uid, d in gps_data.items()},
        'flight': {str(uid): d for uid, d in flight_data.items()},
        'launches': {str(uid): evts
                     for uid, evts in launch_events.items()},
        'timestamp': time.strftime('%H:%M:%S'),
    }


# ── Async UDP listeners ───────────────────────────────────────

class GPSListener(asyncio.DatagramProtocol):
    def datagram_received(self, data, addr):
        pkt = decode_gps_packet(data)
        if pkt:
            update_gps(pkt)
        else:
            print(f"[GPS] Bad packet ({len(data)}b) from {addr}")
    def error_received(self, exc):
        print(f"[GPS] UDP error: {exc}")


def decode_debug_packet(data):
    """Decode a 14-byte Packet 4 debug/health packet. Returns dict or None."""
    if len(data) != 14:
        return None
    try:
        uid      = data[0]
        if uid < 1 or uid > 200:
            return None
        rssi     = struct.unpack_from('>b', data, 1)[0]   # signed int8
        cpu      = data[2]
        heap_x10 = struct.unpack_from('>H', data, 3)[0]  # free_heap_kb × 10
        loop_avg = struct.unpack_from('>H', data, 5)[0]
        loop_max = struct.unpack_from('>H', data, 7)[0]
        temp_x100= struct.unpack_from('>h', data, 9)[0]  # signed int16
        state    = data[11]
        return {
            'unit_id':    uid,
            'rssi_dbm':   rssi,
            'cpu_pct':    cpu,
            'heap_kb':    round(heap_x10 / 10.0, 1),
            'loop_avg_us':loop_avg,
            'loop_max_us':loop_max,
            'temp_c':     round(temp_x100 / 100.0, 2),
            'state':      state,
            'timestamp':  time.time(),
        }
    except Exception as e:
        print(f"[DEBUG] Decode error: {e}")
        return None


class FlightListener(asyncio.DatagramProtocol):
    def datagram_received(self, data, addr):
        pkt = decode_flight_packet(data)
        if pkt:
            update_flight(pkt)
        else:
            print(f"[FLIGHT] Bad packet ({len(data)}b) from {addr}")
    def error_received(self, exc):
        print(f"[FLIGHT] UDP error: {exc}")


def decode_log_announcement(data):
    """Decode 14-byte Packet 5 log announcement."""
    if len(data) != 14:
        print(f"[LOG] Announce: wrong size {len(data)}b (expected 14)")
        return None
    try:
        uid      = data[0]
        ptype    = data[1]
        if uid < 1 or uid > 200:
            print(f"[LOG] Announce: uid {uid} out of range")
            return None
        if ptype != LOG_ANNOUNCE_TYPE:
            print(f"[LOG] Announce: unexpected type 0x{ptype:02x} (expected 0x{LOG_ANNOUNCE_TYPE:02x}) from uid {uid}")
            return None
        win_num  = (data[2] << 8) | data[3]
        win_id   = (data[4]<<24)|(data[5]<<16)|(data[6]<<8)|data[7]
        file_sz  = (data[8]<<24)|(data[9]<<16)|(data[10]<<8)|data[11]
        return {'unit_id': uid, 'window_number': win_num,
                'window_id': win_id, 'file_size': file_sz}
    except Exception as e:
        print(f"[LOG] Announce decode error: {e}")
        return None


class LogAnnounceListener(asyncio.DatagramProtocol):
    def datagram_received(self, data, addr):
        ptype_str = f"{data[1]:02x}" if len(data) > 1 else "XX"
        print(f"[LOG] Packet received from {addr[0]}: {len(data)}b type=0x{ptype_str}")
        pkt = decode_log_announcement(data)
        if not pkt:
            return
        uid  = pkt['unit_id']
        wnum = pkt['window_number']
        wid  = pkt['window_id']
        ip   = unit_ips.get(uid) or addr[0]
        st   = _log_status.get(uid, {})
        # Ignore duplicates (unit sends 5x) and already-fetched logs
        if st.get('status') in ('fetching', 'complete'):
            return
        # Allow re-fetch if a previous attempt failed (fetch_attempts set)
        if st.get('window_num') == wnum and st.get('status') == 'announced':
            if not st.get('fetch_attempts', 0):   # first announcement, not a retry
                return
        # Ignore suspiciously small logs — likely an empty window
        # (e.g. unit closed a zero-flight window when a new 0x20 arrived).
        # A real 3-minute window at 8 Hz is ~50KB minimum.
        MIN_LOG_BYTES = 1000
        if pkt['file_size'] < MIN_LOG_BYTES:
            print(f"[LOG] Unit {uid:02d} window={wnum} ignored — "
                  f"file too small ({pkt['file_size']} bytes), likely empty window")
            _log_status[uid] = {'status': 'none', 'window_id': wid,
                                'window_num': wnum, 'file': None, 'size': 0}
            return
        print(f"[LOG] Unit {uid:02d} log ready: window={wnum} id={wid} "
              f"size={pkt['file_size']} ip={ip}")
        # Mark as fetching immediately to block duplicate announcements
        _log_status[uid] = {'status': 'fetching', 'window_id': wid,
                             'window_num': wnum, 'file': None,
                             'size': pkt['file_size']}
        # Fetch in background thread
        from threading import Thread
        Thread(target=_fetch_unit_log, args=(uid, wnum, ip), daemon=True).start()
    def error_received(self, exc):
        print(f"[LOG] UDP error: {exc}")


class DebugListener(asyncio.DatagramProtocol):
    def datagram_received(self, data, addr):
        pkt = decode_debug_packet(data)
        if pkt:
            debug_data[pkt['unit_id']] = pkt
        else:
            print(f"[DEBUG] Bad packet ({len(data)}b) from {addr}")
    def error_received(self, exc):
        print(f"[DEBUG] UDP error: {exc}")


async def start_sensor_udp(host):
    """
    Start GPS (4211) and flight metrics (4212) listeners.
    Failures are non-fatal — server continues without sensor data
    if ports are unavailable (e.g. already in use from prior run).
    """
    loop = asyncio.get_running_loop()
    try:
        await loop.create_datagram_endpoint(
            GPSListener, local_addr=(host, GPS_PORT))
        print(f"[GPS]    Listening {host}:{GPS_PORT}")
    except OSError as e:
        print(f"[GPS]    Port {GPS_PORT} unavailable ({e}) — GPS disabled")

    try:
        await loop.create_datagram_endpoint(
            FlightListener, local_addr=(host, FLIGHT_PORT))
        print(f"[FLIGHT] Listening {host}:{FLIGHT_PORT}")
    except OSError as e:
        print(f"[FLIGHT] Port {FLIGHT_PORT} unavailable ({e}) — flight metrics disabled")

    try:
        await loop.create_datagram_endpoint(
            DebugListener, local_addr=(host, DEBUG_PORT))
        print(f"[DEBUG]  Listening {host}:{DEBUG_PORT}")
    except OSError as e:
        print(f"[DEBUG]  Port {DEBUG_PORT} unavailable ({e}) — debug health disabled")

    try:
        await loop.create_datagram_endpoint(
            LogAnnounceListener, local_addr=(host, LOG_ANNOUNCE_PORT))
        print(f"[LOG]    Listening {host}:{LOG_ANNOUNCE_PORT}")
    except OSError as e:
        print(f"[LOG]    Port {LOG_ANNOUNCE_PORT} unavailable ({e}) — log announce disabled")

def _contest_next_group():
    """
    Skip to the next group in the current round, or the first group
    of the next round if this is the last group. Saves current group
    results first regardless of window state.
    """
    global _c_round_idx, _c_group_idx
    if _c_round_idx is None:
        print("[CONTEST] next_group: no active round")
        return
    rnd    = contest_rounds[_c_round_idx]
    n_grps = len(rnd['groups'])
    next_gi = _c_group_idx + 1
    # Save current group results if not already complete
    grp = rnd['groups'][_c_group_idx]
    if grp['status'] != 'complete':
        contest_save_group_results()
    if next_gi < n_grps:
        contest_activate_group(_c_round_idx, next_gi, auto=False)
        print(f"[CONTEST] Skipped to R{_c_round_idx+1} G{next_gi+1}")
    else:
        # Last group — complete round and advance
        _contest_complete_round(_c_round_idx)
        next_ri = _c_round_idx + 1
        if next_ri < len(contest_rounds):
            rnd_next = contest_rounds[next_ri]
            if not rnd_next['groups']:
                gs = contest.get('num_units', 8)
                contest_draw_groups(next_ri, gs)
            contest_activate_group(next_ri, 0, auto=False)
            print(f"[CONTEST] Round complete, skipped to R{next_ri+1} G1")
        else:
            _c_round_idx = None
            _c_group_idx = None
            print("[CONTEST] All rounds complete")
            save_contest()


def _broadcast_window_start(window_secs):
    """
    Broadcast the window start command (Packet 5006) to all units.
    14 bytes big-endian: type(0x20) | 0xFF | window_secs(2) | window_id(4) | spare(6)
    Also resets per-unit log status for the new window.
    """
    global _window_id
    _window_id += 1
    wid = _window_id
    buf = bytearray(14)
    buf[0]  = WINDOW_CMD_TYPE   # 0x20
    buf[1]  = 0xFF              # broadcast marker
    buf[2]  = (window_secs >> 8) & 0xFF
    buf[3]  = window_secs & 0xFF
    buf[4]  = (wid >> 24) & 0xFF
    buf[5]  = (wid >> 16) & 0xFF
    buf[6]  = (wid >>  8) & 0xFF
    buf[7]  = wid & 0xFF
    # bytes 8-13 spare = 0x00
    try:
        import socket as _sock
        s = _sock.socket(_sock.AF_INET, _sock.SOCK_DGRAM)
        s.setsockopt(_sock.SOL_SOCKET, _sock.SO_BROADCAST, 1)
        s.sendto(bytes(buf), (BROADCAST_ADDR, WINDOW_CMD_PORT))
        s.close()
        print(f"[WINDOW] Broadcast window start: id={wid} secs={window_secs}")
    except Exception as e:
        print(f"[WINDOW] Broadcast error: {e}")
    # Reset log status for all known units
    for uid in list(units.keys()):
        _log_status[uid] = {'status': 'none', 'window_id': wid,
                             'window_num': None, 'file': None, 'size': 0}


def _maybe_broadcast_prep_countdown(now):
    """Called from get_state_dict — fires 0x21 every 5s during prep."""
    global _last_prep_broadcast
    if (active_task == 'C' or prep_start_time is None
            or window_start_time is not None
            or get_prep_phase(now) is None):
        return
    if now - _last_prep_broadcast >= PREP_BROADCAST_INTERVAL_S:
        # Total seconds until the scoring window opens = all remaining prep phases
        testing_s   = round_settings['testing_s']
        total_prep  = PREP_S + testing_s + NOFLY_S
        elapsed     = prep_elapsed(now)
        time_to_win = max(0, int(total_prep - elapsed))
        _broadcast_prep_countdown(time_to_win, int(active_window_s))


def _broadcast_prep_countdown(countdown_secs, window_secs):
    """
    Broadcast prep countdown packet (0x21) to all units.
    14 bytes: type(0x21)|0xFF|countdown_secs(2)|window_secs(2)|window_id(4)|spare(4)
    Units use this to start a local timer so they open the window autonomously
    even if WiFi drops before the 0x20 window-open packet arrives.
    """
    global _last_prep_broadcast
    cdown = max(0, int(countdown_secs))
    wid   = _window_id   # use current window_id (same one 0x20 will carry)
    buf   = bytearray(14)
    buf[0] = PREP_CMD_TYPE   # 0x21
    buf[1] = 0xFF
    buf[2] = (cdown >> 8) & 0xFF
    buf[3] = cdown & 0xFF
    buf[4] = (window_secs >> 8) & 0xFF
    buf[5] = window_secs & 0xFF
    buf[6] = (wid >> 24) & 0xFF
    buf[7] = (wid >> 16) & 0xFF
    buf[8] = (wid >>  8) & 0xFF
    buf[9] =  wid & 0xFF
    # bytes 10-13 spare = 0x00
    try:
        import socket as _sock
        s = _sock.socket(_sock.AF_INET, _sock.SOCK_DGRAM)
        s.setsockopt(_sock.SOL_SOCKET, _sock.SO_BROADCAST, 1)
        s.sendto(bytes(buf), (BROADCAST_ADDR, WINDOW_CMD_PORT))
        s.close()
        print(f"[PREP] Broadcast countdown: {cdown}s until window, "
              f"window={window_secs}s id={wid}")
    except Exception as e:
        print(f"[PREP] Broadcast error: {e}")
    _last_prep_broadcast = time.time()


def _fetch_unit_log(uid, window_num, ip):
    """
    Fetch sensor log and score summary from a flight unit.
    Per ICD v1.6:
      - Sensor log:    GET /log?n=NNN  (no deletion — del=0)
      - Score summary: GET /summary?n=NNN  (never deleted, ~200 bytes)
    Both saved to logs/ directory. Status updated in _log_status.
    """
    import urllib.request as _req
    import urllib.error as _uerr
    os.makedirs(LOGS_DIR, exist_ok=True)

    # Brief delay to let the unit HTTP server come up after WiFi re-association
    if LOG_FETCH_DELAY_S > 0:
        time.sleep(LOG_FETCH_DELAY_S)

    def _get(url, out_path, label):
        """Fetch one URL, save to out_path. Returns True on success."""
        print(f"[LOG] Fetching {label} unit {uid:02d} → {url}")
        try:
            with _req.urlopen(url, timeout=8) as resp:
                expected = int(resp.headers.get('Content-Length') or 0)
                data = resp.read()
            # Reject partial transfers
            if expected and len(data) < expected:
                print(f"[LOG] Unit {uid:02d} {label} incomplete: "
                      f"{len(data)}/{expected} bytes — discarding")
                return None
            with open(out_path, 'wb') as f:
                f.write(data)
            print(f"[LOG] Unit {uid:02d} {label} saved: {out_path} ({len(data)} bytes)")
            return data
        except _uerr.HTTPError as e:
            print(f"[LOG] Unit {uid:02d} {label} → {url} failed: {e}")
            return None
        except _uerr.IncompleteRead as e:
            # Partial transfer - delete the truncated file and treat as failure
            print(f"[LOG] Unit {uid:02d} {label} incomplete: {e} - discarding")
            try: os.remove(out_path)
            except OSError: pass
            return None
        except Exception as e:
            print(f"[LOG] Unit {uid:02d} {label} error: {e}")
            return None

    # ── 1. Score summary (small, ~200 bytes, never deleted) ───────
    summary_path = os.path.join(LOGS_DIR, f"summary_{uid:03d}_{window_num:03d}.csv")
    summary_data = _get(
        f"http://{ip}/summary?n={window_num}",
        summary_path, "summary"
    )

    # ── 2. Sensor log (no deletion - logs preserved on unit) ────────
    log_path = os.path.join(LOGS_DIR, f"window_{uid:03d}_{window_num:03d}.csv")
    log_data = _get(f"http://{ip}/log?n={window_num}", log_path, "sensor log")

    # ── 3. Update status ──────────────────────────────────────────
    if log_data is not None or summary_data is not None:
        _log_status[uid].update({
            'status':       'complete',
            'file':         log_path if log_data else None,
            'size':         len(log_data) if log_data else 0,
            'summary_file': summary_path if summary_data else None,
            'summary_size': len(summary_data) if summary_data else 0,
        })
        # Success — remove from retry queue if present
        if uid in _log_retry_queue:
            del _log_retry_queue[uid]
            _save_retry_queue()
            print(f'[LOG] Unit {uid:02d} removed from retry queue')
        # Parse summary (fast ~200 bytes) then repopulate alt_history
        if summary_data:
            _parse_summary(uid, summary_data.decode('utf-8', errors='replace'))
            has_flights = bool(_log_status.get(uid, {}).get('summary_flights'))
            wid = _log_status.get(uid, {}).get('window_id')
            if wid and wid in _pending_summaries:
                if has_flights:
                    _pending_summaries[wid].discard(uid)
                    _check_summary_complete(wid)
                else:
                    print(f'[LOG] Unit {uid:02d} summary has no flights — '
                          f'keeping UDP score, not blocking rescore')
                    _pending_summaries[wid].discard(uid)
                    _check_summary_complete(wid)
        # Repopulate alt_history after status=complete so badge clears immediately
        if log_data:
            _repopulate_alt_history(uid, log_data.decode('utf-8', errors='replace'))
    else:
        # Both fetches failed.
        # Reset to 'announced' so remaining Packet 5 duplicates can
        # re-trigger a fetch attempt (unit sends 5x at 2s intervals).
        # If all 5 are exhausted, enqueue for persistent retry.
        st = _log_status.get(uid, {})
        attempts_so_far = st.get('fetch_attempts', 0) + 1
        if attempts_so_far < 4:   # still have more announcements coming
            _log_status[uid] = {
                'status':         'announced',
                'window_id':      st.get('window_id', 0),
                'window_num':     window_num,
                'file':           None, 'size': 0,
                'fetch_attempts': attempts_so_far,
            }
            print(f'[LOG] Unit {uid:02d} fetch attempt {attempts_so_far} failed — '
                  f'will retry on next announcement')
        else:
            # All announcement retries exhausted — hand off to retry queue
            _enqueue_log_retry(uid, window_num, ip)


def _enqueue_log_retry(uid, window_num, ip):
    """Add or update a unit in the persistent retry queue."""
    existing = _log_retry_queue.get(uid, {})
    attempts = existing.get('attempts', 0)
    _log_retry_queue[uid] = {
        'ip':           ip,
        'window_num':   window_num,
        'window_id':    _log_status.get(uid, {}).get('window_id', 0),
        'attempts':     attempts,
        'next_retry_t': time.time() + LOG_RETRY_INTERVAL_S,
    }
    _log_status[uid]['status'] = 'queued'
    _log_status[uid]['retry_attempts'] = attempts
    print(f"[LOG] Unit {uid:02d} queued for retry "
          f"(attempt {attempts}, next in {LOG_RETRY_INTERVAL_S}s)")
    _save_retry_queue()


def _save_retry_queue():
    """Persist retry queue to contest.json so server restarts keep pending fetches."""
    try:
        if not os.path.exists('contest.json'):
            return
        with open('contest.json') as f:
            cj = json.load(f)
        cj['log_retry_queue'] = {
            str(k): {kk: vv for kk, vv in v.items() if kk != 'next_retry_t'}
            for k, v in _log_retry_queue.items()
        }
        with open('contest.json', 'w') as f:
            json.dump(cj, f, indent=2)
    except Exception as e:
        print(f"[LOG] Retry queue save error: {e}")


def _load_retry_queue():
    """Restore retry queue from contest.json on startup."""
    global _log_retry_queue
    try:
        if not os.path.exists('contest.json'):
            return
        with open('contest.json') as f:
            cj = json.load(f)
        q = cj.get('log_retry_queue', {})
        for uid_s, v in q.items():
            uid = int(uid_s)
            _log_retry_queue[uid] = {
                'ip':           v.get('ip', ''),
                'window_num':   v.get('window_num', 0),
                'window_id':    v.get('window_id', 0),
                'attempts':     v.get('attempts', 0),
                'next_retry_t': time.time() + 10,  # retry soon after restart
            }
            _log_status[uid] = _log_status.get(uid, {})
            _log_status[uid]['status'] = 'queued'
            _log_status[uid]['retry_attempts'] = v.get('attempts', 0)
        if _log_retry_queue:
            print(f"[LOG] Restored {len(_log_retry_queue)} pending log retrieval(s)")
    except Exception as e:
        print(f"[LOG] Retry queue load error: {e}")


def _summary_timeout_worker():
    """
    Background daemon thread. Wakes every 2s and fires rescore for any
    pending summary window whose timeout has elapsed.
    This is required because _check_summary_complete is only called reactively
    when a summary arrives — if the unit never delivers, the timeout must still fire.
    """
    while True:
        time.sleep(2)
        if not _pending_summaries or _summary_timeout_t is None:
            continue
        if time.time() < _summary_timeout_t:
            continue
        # Timeout elapsed — fire rescore for each pending window
        for wid in list(_pending_summaries.keys()):
            print(f"[SCORE] Timeout elapsed for window_id={wid} — forcing rescore")
            _check_summary_complete(wid)


def _log_retry_worker():
    """
    Background daemon thread. Wakes every 10s, fires fetches for units
    whose next_retry_t has elapsed. Removes unit on success.
    No attempt cap — retries indefinitely until success or server shutdown.
    """
    while True:
        time.sleep(10)
        now = time.time()
        for uid in list(_log_retry_queue.keys()):
            entry = _log_retry_queue.get(uid)
            if not entry or now < entry['next_retry_t']:
                continue
            ip         = entry['ip']
            window_num = entry['window_num']
            attempts   = entry['attempts'] + 1
            _log_retry_queue[uid]['attempts'] = attempts
            if uid in _log_status:
                _log_status[uid]['retry_attempts'] = attempts
            # Skip if unit is currently stale (likely mid-flight, WiFi off)
            u = units.get(uid)
            if u and u.get('stale', False):
                print(f"[LOG] Skip retry unit {uid:02d} — unit stale (mid-flight), "
                      f"will retry when unit reconnects")
                # Reschedule without incrementing attempts
                _log_retry_queue[uid]['next_retry_t'] = time.time() + LOG_RETRY_INTERVAL_S
                continue
            print(f"[LOG] Retry fetch unit {uid:02d} "
                  f"window={window_num} attempt={attempts} ip={ip}")
            _fetch_unit_log(uid, window_num, ip)


def _parse_summary(uid, csv_text):
    """
    Parse summary_NNN.csv and store per-flight scores in _log_status.
    Columns: flight_num, start_ms, end_ms, duration_s,
             launch_height_ft, joed_score, secsft_score
    Last row is the totals row (flight_num='T').
    """
    try:
        lines = [l.strip() for l in csv_text.splitlines() if l.strip()]
        if len(lines) < 2:
            return
        # Skip header row
        flights = []
        totals  = None
        for line in lines[1:]:
            parts = line.split(',')
            if len(parts) < 7:
                continue
            if parts[0].strip().upper() == 'T':
                totals = {
                    'total_dur_s':      _safe_float(parts[3]),
                    'avg_launch_ft':    _safe_float(parts[4]),
                    'avg_joed':         _safe_float(parts[5]),
                    'sum_secsft':       _safe_float(parts[6]),
                }
            else:
                flights.append({
                    'flight_num':       _safe_int(parts[0]),
                    'duration_s':       _safe_float(parts[3]),
                    'launch_height_ft': _safe_float(parts[4]),
                    'joed_score':       _safe_float(parts[5]),
                    'secsft_score':     _safe_float(parts[6]),
                })
        _log_status[uid]['summary_flights'] = flights
        _log_status[uid]['summary_totals']  = totals
        print(f"[LOG] Unit {uid:02d} summary: {len(flights)} flights, "
              f"secsft={totals['sum_secsft'] if totals else '?'}")
    except Exception as e:
        print(f"[LOG] Unit {uid:02d} summary parse error: {e}")


def _safe_float(s):
    try: return float(s.strip())
    except: return 0.0

def _safe_int(s):
    try: return int(s.strip())
    except: return 0


def _repopulate_alt_history(uid, csv_text):
    """
    Parse alt_tared_ft and t_ms from sensor log CSV and repopulate
    the unit's alt_history for the altitude sparkline.
    Columns (ICD v1.4): t_ms, flight, flight_t_s, state, throw_height_ft,
                         alt_ft, alt_tared_ft, ...
    """
    try:
        lines = [l.strip() for l in csv_text.splitlines() if l.strip()]
        if len(lines) < 2:
            return
        # Find column indices from header
        hdr = [c.strip() for c in lines[0].split(',')]
        try:
            t_idx   = hdr.index('t_ms')
            alt_idx = hdr.index('alt_tared_ft')
        except ValueError:
            print(f"[LOG] Unit {uid:02d} sensor log missing t_ms/alt_tared_ft columns")
            return
        history = []
        for line in lines[1:]:
            parts = line.split(',')
            if len(parts) <= max(t_idx, alt_idx):
                continue
            try:
                t_s  = float(parts[t_idx]) / 1000.0   # ms → s
                alt  = float(parts[alt_idx])
                history.append([round(t_s, 1), round(alt, 1)])
            except (ValueError, IndexError):
                continue
        if uid in units and history:
            units[uid]['alt_history'] = history
            print(f"[LOG] Unit {uid:02d} alt_history repopulated: {len(history)} samples")
    except Exception as e:
        print(f"[LOG] Unit {uid:02d} alt_history error: {e}")


# Snapshot of round/group at time of window close — used by rescore
_summary_round_idx = None
_summary_group_idx = None
_summary_task      = None   # task at time of window close — may differ from active_task by rescore time


def _register_pending_summaries(active_units):
    """Called at WIN_CLOSED. Registers active UIDs as awaiting summary delivery."""
    global _pending_summaries, _summary_window_id, _summary_timeout_t
    global _summary_round_idx, _summary_group_idx, _summary_task
    uids = [u['id'] for u in active_units if not u.get('stale')]
    if not uids:
        return
    _summary_window_id = _window_id
    _summary_round_idx = _c_round_idx   # snapshot before auto-advance
    _summary_group_idx = _c_group_idx
    _summary_task      = active_task    # snapshot task — active_task changes with next round
    _pending_summaries[_window_id] = set(uids)
    _summary_timeout_t = time.time() + SUMMARY_TIMEOUT_S
    print(f"[SCORE] Waiting for summaries from {len(uids)} units "
          f"(window_id={_window_id}, timeout={SUMMARY_TIMEOUT_S}s)")


def _check_summary_complete(window_id):
    """
    Called after each summary is parsed. If all units have reported
    (or timeout has passed), re-score and re-normalise the group.
    """
    pending = _pending_summaries.get(window_id)
    if pending is None:
        return
    timed_out = (_summary_timeout_t is not None and
                 time.time() > _summary_timeout_t)
    if pending and not timed_out:
        print(f"[SCORE] Still waiting for {len(pending)} summary/summaries: {pending}")
        return
    # All in (or timed out) — re-score
    print(f"[SCORE] All summaries received for window_id={window_id} — rescoring")
    _do_rescore_from_summaries()
    del _pending_summaries[window_id]


def _do_rescore_from_summaries():
    """
    Re-score all active units from their downloaded summaries, then re-normalise.
    Uses the round/group snapshot taken at WIN_CLOSED, since _c_round_idx will
    have already advanced to the next group by the time summaries arrive.
    """
    if _summary_round_idx is None or _summary_group_idx is None:
        print("[SCORE] Rescore skipped — no round/group snapshot")
        return
    active = [u for u in units.values() if not u.get('stale')]
    rescored = []
    for u in active:
        uid = u['id']
        ls  = _log_status.get(uid, {})
        flights = ls.get('summary_flights')
        if not flights:
            print(f"[SCORE] Unit {uid:02d} — no summary, keeping UDP score")
            rescored.append(u)
            continue
        # Replace task_flights with summary data
        new_flights = []
        for f in flights:
            new_flights.append({
                'dur':    f['duration_s'],
                'lh_ft':  f['launch_height_ft'],
                'dq':     False,
                'capped_dur': f['duration_s'],
                'peak_alt_ft': 0.0,
                'secsft_score': f['secsft_score'],
                'joed_score':   f['joed_score'],
                'from_summary': True,
            })
        u['task_flights'] = new_flights
        # task_raw_s = sum of secsft scores (matches our LL/secs-ft scoring)
        totals = ls.get('summary_totals')
        u['task_raw_s'] = totals['sum_secsft'] if totals else sum(
            f['secsft_score'] for f in flights)
        u['flight_count'] = len(new_flights)
        print(f"[SCORE] Unit {uid:02d} rescored from summary: "
              f"{len(new_flights)} flights, raw={u['task_raw_s']:.1f}s")
        rescored.append(u)
    # Re-normalise using the snapshotted task — active_task may already be
    # a different task (next round) by the time summaries arrive
    normalise_scores(rescored, _summary_task or active_task)
    for u in rescored:
        units[u['id']]['normalised_score'] = u['normalised_score']
    # Save directly to the snapshotted round/group (not current _c_round_idx
    # which has already advanced to the next group by now)
    _save_rescore_to_group(_summary_round_idx, _summary_group_idx, rescored)
    print(f"[SCORE] Rescore complete — R{(_summary_round_idx or 0)+1} G{(_summary_group_idx or 0)+1} task={_summary_task} — contest saved")


def _rescore_from_picked_file(round_idx, group_idx):
    """
    Open a native Windows multi-select file picker dialog.
    CD can select one summary CSV per pilot in the group (Ctrl/Shift-click).
    Each file's uid is inferred from its filename (summary_UUU_NNN.csv).
    All selected files are parsed and the group is rescored and renormalised.
    Returns (ok, message).
    """
    try:
        rnd = contest_rounds[round_idx]
        grp = rnd["groups"][group_idx]
    except (IndexError, KeyError):
        return False, f"Round {round_idx+1} Group {group_idx+1} not found"

    pilot_names = set(grp.get("pilots", []))
    n_pilots = len(pilot_names)

    # Build uid↔name maps from every available source
    name_to_uid = {name: uid for uid, name in pilots.items() if name in pilot_names}
    for pname, res in grp.get("results", {}).items():
        if pname in pilot_names and pname not in name_to_uid and res.get("unit_id"):
            name_to_uid[pname] = res["unit_id"]
    uid_to_name = {uid: name for name, uid in name_to_uid.items()}

    import queue as _queue
    result_q = _queue.Queue()

    def _pick():
        try:
            import tkinter as _tk
            from tkinter import filedialog as _fd
            root = _tk.Tk()
            root.withdraw()
            root.attributes("-topmost", True)
            initial = os.path.abspath(LOGS_DIR)
            title = (f"R{round_idx+1} G{group_idx+1} — select summary CSV "
                     f"for each pilot ({n_pilots} pilot{'s' if n_pilots!=1 else ''})")
            paths = _fd.askopenfilenames(
                title=title,
                initialdir=initial,
                filetypes=[('Summary CSV files', 'summary_*.csv'),
                           ('All CSV files', '*.csv')],
            )
            root.destroy()
            # askopenfilenames returns a tuple; join with | for transport
            result_q.put('|'.join(paths) if paths else "")
        except Exception as e:
            result_q.put(f"ERROR:{e}")

    from threading import Thread
    t = Thread(target=_pick, daemon=True)
    t.start()
    t.join(timeout=120)   # 2 min for CD to select multiple files
    try:
        raw = result_q.get_nowait()
    except Exception:
        return False, "File picker timed out or failed"

    if raw.startswith("ERROR:"):
        return False, f"File picker error: {raw[6:]}"
    if not raw:
        return False, "No files selected"

    paths = [p for p in raw.split('|') if p]
    if not paths:
        return False, "No files selected"

    task = rnd.get("task", active_task)
    rescored = []
    messages = []

    for path in paths:
        fname = os.path.basename(path)
        # Infer uid from filename: summary_UUU_NNN.csv
        file_uid = None
        try:
            file_uid = int(fname.replace(".csv","").split("_")[1])
        except Exception:
            pass

        # Resolve pilot name
        name = uid_to_name.get(file_uid)
        if not name:
            # Not a known uid — assign to first unmatched pilot
            matched = {u["id"] for u in rescored}
            for pname, puid in name_to_uid.items():
                if puid not in matched:
                    name, file_uid = pname, puid
                    break
        if not name:
            name = next(iter(pilot_names - {u.get("pilot_name") for u in rescored}),
                        f"Unit {file_uid or '?'}")
        uid = file_uid or 0

        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                csv_text = f.read()
        except Exception as e:
            messages.append(f"{fname}: read error — {e}")
            continue

        _log_status.setdefault(uid, {})["status"] = "complete"
        _parse_summary(uid, csv_text)
        ls = _log_status.get(uid, {})
        flights = ls.get("summary_flights", [])
        totals  = ls.get("summary_totals")

        if not flights:
            messages.append(f"{fname}: no flights found")
            continue

        task_flights = [{
            "dur":          f["duration_s"],
            "lh_ft":        f["launch_height_ft"],
            "dq":           False,
            "capped_dur":   f["duration_s"],
            "peak_alt_ft":  0.0,
            "secsft_score": f["secsft_score"],
            "joed_score":   f["joed_score"],
            "from_summary": True,
        } for f in flights]
        raw_score = (totals["sum_secsft"] if totals
                     else sum(f["secsft_score"] for f in flights))

        u = units.get(uid)
        if u:
            u["task_flights"] = task_flights
            u["task_raw_s"]   = raw_score
            u["flight_count"] = len(task_flights)
        else:
            u = {"id": uid, "pilot_name": name,
                 "task_flights": task_flights, "task_raw_s": raw_score,
                 "flight_count": len(task_flights), "normalised_score": 0.0,
                 "stale": False}

        rescored.append(u)
        messages.append(f"{name} ({fname}): {len(flights)} flt, {raw_score:.1f}s")

    if not rescored:
        return False, "No valid summaries loaded: " + "; ".join(messages)

    # Renormalise all loaded pilots together
    normalise_scores(rescored, task)
    for u in rescored:
        if u["id"] in units:
            units[u["id"]]["normalised_score"] = u["normalised_score"]
    _save_rescore_to_group(round_idx, group_idx, rescored)
    msg = f"R{round_idx+1} G{group_idx+1} rescored ({len(rescored)}/{n_pilots} pilots) — " + "; ".join(messages)
    print(f"[SCORE] {msg}")
    return True, msg


def _rescore_round_from_logs(round_idx, group_idx):
    """
    Manually rescore a completed round/group by scanning the logs/ folder
    for summary CSV files matching pilots in that group.
    Works from the group's saved results — does not require live unit objects,
    so it works correctly after server restart or mid-contest.
    Returns (ok, message).
    """
    import glob
    try:
        rnd = contest_rounds[round_idx]
        grp = rnd["groups"][group_idx]
    except (IndexError, KeyError):
        return False, f"Round {round_idx+1} Group {group_idx+1} not found"

    task = rnd.get("task", active_task)
    pilot_names = set(grp.get("pilots", []))

    # Build pilot→uid map: prefer live pilots dict, then scan summary filenames
    # This allows rescoring after restart when pilots dict may be incomplete
    name_to_uid = {name: uid for uid, name in pilots.items() if name in pilot_names}

    # Supplement from saved group results — unit_id is stored there
    for pname, res in grp.get('results', {}).items():
        if pname in pilot_names and pname not in name_to_uid and res.get('unit_id'):
            name_to_uid[pname] = res['unit_id']

    # For any pilot not found yet, try live units dict
    missing = pilot_names - set(name_to_uid.keys())
    if missing:
        for uid, name in list(units.items()) + list(pilots.items()):
            if isinstance(uid, int) and isinstance(name, str) and name in missing:
                name_to_uid[name] = uid
                missing.discard(name)

    if not name_to_uid:
        # Last resort: scan summary files for any uid and use them all
        all_summaries = sorted(glob.glob(os.path.join(LOGS_DIR, "summary_*_*.csv")))
        seen_uids = set()
        for f in all_summaries:
            try:
                uid = int(os.path.basename(f).split("_")[1])
                seen_uids.add(uid)
            except Exception:
                pass
        if not seen_uids:
            return False, "No pilots mapped to unit IDs and no summary files found"
        # Assign uids to pilots in order — best effort
        for name, uid in zip(sorted(pilot_names), sorted(seen_uids)):
            name_to_uid[name] = uid

    rescored = []
    messages = []
    for name in sorted(pilot_names):
        uid = name_to_uid.get(name)
        if uid is None:
            messages.append(f"{name}: no unit ID found — skipped")
            continue

        # Find summary files for this unit, pick most recent
        pattern = os.path.join(LOGS_DIR, f"summary_{uid:03d}_*.csv")
        files = sorted(glob.glob(pattern))
        if not files:
            messages.append(f"Unit {uid:02d} ({name}): no summary files in logs/")
            continue

        # Pick the right summary file:
        # 1. Use window_num stored in group results (persisted across restarts)
        # 2. Fall back to window_num from live log_status
        # 3. Last resort: most recent file (may be wrong after restart)
        chosen = files[-1]
        saved_result = grp.get('results', {}).get(name, {})
        wnum = (saved_result.get('window_num')
                or _log_status.get(uid, {}).get('window_num'))
        if wnum:
            candidate = os.path.join(LOGS_DIR,
                f"summary_{uid:03d}_{int(wnum):03d}.csv")
            if os.path.exists(candidate):
                chosen = candidate
            else:
                print(f"[SCORE] summary_{uid:03d}_{int(wnum):03d}.csv not found, using latest")

        try:
            with open(chosen) as f:
                csv_text = f.read()
        except Exception as e:
            messages.append(f"Unit {uid:02d} ({name}): read error — {e}")
            continue

        # Parse into a temporary log_status slot
        _log_status.setdefault(uid, {})["status"] = "complete"
        _parse_summary(uid, csv_text)
        ls = _log_status[uid]
        flights = ls.get("summary_flights", [])
        totals  = ls.get("summary_totals")

        if not flights:
            messages.append(f"Unit {uid:02d} ({name}): summary has no flights — {os.path.basename(chosen)}")
            continue

        # Build a synthetic unit dict — works even if unit not live
        task_flights = [{
            "dur":          f["duration_s"],
            "lh_ft":        f["launch_height_ft"],
            "dq":           False,
            "capped_dur":   f["duration_s"],
            "peak_alt_ft":  0.0,
            "secsft_score": f["secsft_score"],
            "joed_score":   f["joed_score"],
            "from_summary": True,
        } for f in flights]
        raw = totals["sum_secsft"] if totals else sum(f["secsft_score"] for f in flights)

        # Use live unit if available, otherwise create a stub for normalisation
        u = units.get(uid)
        if u:
            u["task_flights"] = task_flights
            u["task_raw_s"]   = raw
            u["flight_count"] = len(task_flights)
        else:
            u = {"id": uid, "pilot_name": name,
                 "task_flights": task_flights, "task_raw_s": raw,
                 "flight_count": len(task_flights), "normalised_score": 0.0,
                 "stale": False}

        rescored.append(u)
        messages.append(f"Unit {uid:02d} ({name}): {len(flights)} flights, secsft={raw:.1f}s from {os.path.basename(chosen)}")

    if not rescored:
        return False, "No summaries loaded: " + "; ".join(messages)

    normalise_scores(rescored, task)
    # Update live units if present
    for u in rescored:
        if u["id"] in units:
            units[u["id"]]["normalised_score"] = u["normalised_score"]
    _save_rescore_to_group(round_idx, group_idx, rescored)
    msg = f"Rescored R{round_idx+1} G{group_idx+1} ({task}) — " + "; ".join(messages)
    print(f"[SCORE] {msg}")
    return True, msg


def _save_rescore_to_group(round_idx, group_idx, rescored_units):
    """
    Save rescore results directly to the specified round/group,
    bypassing contest_save_group_results() which uses _c_round_idx
    (already advanced by the time summaries arrive).
    """
    if round_idx is None or group_idx is None:
        return
    try:
        grp = contest_rounds[round_idx]['groups'][group_idx]
    except (IndexError, KeyError):
        print(f"[SCORE] Rescore save failed — R{round_idx+1} G{group_idx+1} not found")
        return
    for u in rescored_units:
        # Try live pilots dict first, then the name stored on the stub unit
        name = pilots.get(u['id']) or u.get('pilot_name')
        if not name:
            continue
        grp['results'][name] = {
            'task_raw_s': round(u['task_raw_s'], 2),
            'normalised': round(u.get('normalised_score') or 0.0, 1),
            'unit_id':    u['id'],
            'window_num': _log_status.get(u['id'], {}).get('window_num'),
            'flights': [
                {'dur':         round(f['dur'], 1),
                 'lh_ft':       round(f['lh_ft'], 1),
                 'capped_dur':  round(f.get('capped_dur', f['dur']), 1),
                 'peak_alt_ft': round(f.get('peak_alt_ft', 0.0), 1),
                 'dq':          bool(f.get('dq', False)),
                 'from_summary':bool(f.get('from_summary', False))}
                for f in u.get('task_flights', [])
            ],
        }
    # Re-normalise within group (best = 1000)
    best = max((r['task_raw_s'] for r in grp['results'].values()), default=0.0)
    for res in grp['results'].values():
        res['normalised'] = round(res['task_raw_s'] / best * 1000, 1) if best > 0 else 0.0
    _recalc_standings()
    save_contest()
    print(f"[SCORE] Rescore saved → R{round_idx+1} G{group_idx+1}: "
          f"{list(grp['results'].keys())}")


# ─────────────────────────────────────────────
#  HTTP Handler
# ─────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        p = urlparse(self.path)
        if p.path == '/':
            self._send(200, 'text/html', HTML.encode())
        elif p.path == '/state':
            self._send(200, 'application/json', build_state_json().encode())
        elif p.path in ('/pilots', '/pilots/'):
            self._send(200, 'text/html', PILOTS_HTML.encode())
        elif p.path in ('/standings', '/standings/'):
            self._send(200, 'text/html', STANDINGS_HTML.encode())
        elif p.path in ('/draws', '/draws/'):
            self._send(200, 'text/html', DRAWS_HTML.encode())
        elif p.path == '/api/logs':
            # Log retrieval status for all units
            self._send(200, 'application/json',
                       json.dumps({'logs': _log_status,
                                   'window_id': _window_id}).encode())

        elif p.path.startswith('/api/log/cancel/'):
            # Cancel log retrieval for a unit — record DNF, clear retry queue
            try:
                uid_c = int(p.path.split('/')[-1])
            except ValueError:
                self._send(400,'application/json',b'{"error":"bad uid"}')
                return
            # Remove from retry queue
            if uid_c in _log_retry_queue:
                del _log_retry_queue[uid_c]
                _save_retry_queue()
            # Mark log status as DNF
            _log_status[uid_c] = {
                'status': 'dnf',
                'window_id': _window_id,
                'window_num': _log_status.get(uid_c, {}).get('window_num'),
                'file': None, 'size': 0,
            }
            # Record DNF flight on unit if it's active
            if uid_c in units:
                u = units[uid_c]
                u['task_flights'].append({
                    'dur': 0.0, 'lh_ft': 0.0, 'dq': True,
                    'capped_dur': 0.0, 'peak_alt_ft': 0.0,
                    'dnf': True,
                })
                result = score_task(active_task, u['task_flights'], task_cfg_ovr)
                u['task_raw_s']    = result['raw_s']
                u['task_detail']   = result['detail']
                u['task_progress'] = result['progress']
                u['flight_count']  = len(u['task_flights'])
                # Re-normalise the group
                active_units = [u2 for u2 in units.values() if not u2.get('stale')]
                normalise_scores(active_units, active_task)
                for au in active_units:
                    units[au['id']]['normalised_score'] = au['normalised_score']
                # Remove from pending summaries so group can rescore
                if _summary_window_id and _summary_window_id in _pending_summaries:
                    _pending_summaries[_summary_window_id].discard(uid_c)
                    _check_summary_complete(_summary_window_id)
                print(f'[LOG] Unit {uid_c:02d} DNF recorded — retry queue cleared')
                contest_save_group_results()
            self._send(200,'application/json',b'{"ok":true}')

        elif p.path.startswith('/api/log/') or p.path.startswith('/api/summary/'):
            # Serve a retrieved CSV file
            # /api/log/<uid>/<window_num>
            # /api/summary/<uid>/<window_num>
            parts = p.path.split('/')
            is_summary = p.path.startswith('/api/summary/')
            if len(parts) == 5:
                try:
                    uid_p = int(parts[3])
                    wnum  = int(parts[4])
                    prefix = 'summary' if is_summary else 'window'
                    fpath = os.path.join(LOGS_DIR,
                            f'{prefix}_{uid_p:03d}_{wnum:03d}.csv')
                    if os.path.exists(fpath):
                        with open(fpath,'rb') as ff:
                            self._send(200,'text/csv',ff.read())
                    else:
                        self._send(404,'text/plain',b'File not found')
                except ValueError:
                    self._send(400,'text/plain',b'Invalid path')
            else:
                self._send(400,'text/plain',b'Bad request')

        elif p.path == '/contest_data':
            # Full contest rounds with pilot assignments for /draws page
            self._send(200, 'application/json',
                       json.dumps({'rounds': contest_rounds}).encode())
        elif p.path in ('/gps_state', '/imu_state'):
            # Sensor data — GPS fixes and flight metrics
            self._send(200, 'application/json',
                       json.dumps(build_sensor_state_json()).encode())
        else:
            self._send(404, 'text/plain', b'Not found')

    def do_POST(self):
        global active_task, task_cfg_ovr, active_window_s, window_start_time
        global prep_start_time, _landing_win_zeroed
        global paused, pause_start_time, total_paused_s
        length = int(self.headers.get('Content-Length',0))
        body   = self.rfile.read(length)
        p      = urlparse(self.path)
        try:    data = json.loads(body) if body else {}
        except: data = {}

        if p.path == '/api/pilots':
            # Assign pilot names to unit numbers for this round
            new_pilots = data.get('pilots',{})
            pilots.clear()
            active_group_uids.clear()
            for k,v in new_pilots.items():
                try:
                    uid = int(k)
                    if 1<=uid<=20 and isinstance(v,str):
                        pilots[uid]=v.strip()
                        active_group_uids.add(uid)
                except: pass
            print(f"[SERVER] Active group: {pilots}")
            self._send(200,'application/json',b'{"ok":true}')

        elif p.path == '/api/roster':
            # Save/update the master roster list
            action = data.get('action', 'set')
            if action == 'set':
                new_roster = data.get('roster', [])
                roster.clear()
                roster.extend([str(n).strip() for n in new_roster if str(n).strip()])
                save_config()
                print(f"[SERVER] Roster updated: {len(roster)} pilots")
                self._send(200,'application/json',b'{"ok":true}')
            elif action == 'add':
                name = str(data.get('name','')).strip()
                if name and name not in roster:
                    roster.append(name)
                    save_config()
                self._send(200,'application/json',b'{"ok":true}')
            elif action == 'remove':
                name = str(data.get('name','')).strip()
                if name in roster:
                    roster.remove(name)
                    save_config()
                self._send(200,'application/json',b'{"ok":true}')
            else:
                self._send(400,'application/json',b'{"error":"unknown action"}')

        elif p.path == '/api/checkin':
            # Toggle or set check-in status for a pilot
            # Expects: {"name": "Brent Lytle", "present": true}
            #      or: {"action": "all"}  / {"action": "none"}
            action = data.get('action')
            if action == 'all':
                checkins.update({n: checkins.get(n) for n in roster})
                print(f"[SERVER] Check-in: ALL ({len(checkins)} pilots)")
            elif action == 'none':
                checkins.clear()
                print("[SERVER] Check-in: cleared")
            else:
                name    = str(data.get('name','')).strip()
                present = bool(data.get('present', True))
                uid_val = data.get('unit')
                uid_int = int(uid_val) if uid_val not in (None,'') else None
                if name:
                    if present:
                        checkins[name] = uid_int if uid_int is not None else checkins.get(name)
                    else:
                        checkins.pop(name, None)
                    print(f"[SERVER] Check-in: {name} → {'IN' if present else 'OUT'} "
                          f"uid={checkins.get(name)}  "
                          f"({len(checkins)}/{len(roster)} present)")
            save_contest()
            self._send(200,'application/json',
                       json.dumps({'ok':True,'count':len(checkins)}).encode())

        elif p.path == '/api/checkin/unit':
            # Set/clear pre-assigned unit for a checked-in pilot
            # {"name":"Brent Lytle", "unit":177}  or {"unit":null} to clear
            name    = str(data.get('name','')).strip()
            uid_val = data.get('unit')
            uid_int = int(uid_val) if uid_val not in (None,'') else None
            if name and name in checkins:
                checkins[name] = uid_int
                print(f'[SERVER] Unit pre-assign: {name} → {uid_int}')
                save_contest()
            self._send(200,'application/json',b'{"ok":true}')

        elif p.path == '/api/roster/draw':
            # Random draw from checked-in pilots only (minus excluded)
            # Expects: {"count": 8, "exclude": ["Alice"]}
            count   = int(data.get('count', min(len(checkins) or len(roster), 8)))
            exclude = set(data.get('exclude', []))
            # Pool = checked-in pilots not excluded
            # If nobody checked in yet, fall back to full roster (demo mode)
            base    = checkins if checkins else {n:None for n in roster}
            pool    = [n for n in roster if n in base and n not in exclude]
            import random as _rnd
            count   = min(count, len(pool))
            if count == 0:
                self._send(200,'application/json',
                           json.dumps({'ok':True,'assignment':{},'pool_empty':True}).encode())
                return
            drawn      = _rnd.sample(pool, count)
            assignment = {str(i+1): name for i, name in enumerate(drawn)}
            print(f"[SERVER] Draw {count} from {len(pool)} available: "
                  f"{list(assignment.values())[:3]}…")
            self._send(200,'application/json',
                       json.dumps({'ok':True,'assignment':assignment}).encode())

        elif p.path == '/api/task':
            new_task = data.get('task', active_task)
            if new_task in TASKS:
                active_task       = new_task
                task_cfg_ovr      = data.get('cfg',{})
                active_window_s   = int(task_cfg_ovr.get('window',
                                        TASKS[active_task].get('window',0)))
                window_start_time = None
                _landing_win_zeroed = False
                # Reset Task C state
                taskc_state['phase']       = TASKC_READY
                taskc_state['num_attempts']= task_cfg_ovr.get('numFlights',3)
                taskc_state['attempt']     = 0
                taskc_state['phase_start'] = None
                _reset_scores()
                print(f"[SERVER] Task={active_task}  window={active_window_s}s")
                # Announce new task
                _ti = TASKS.get(active_task,{})
                audio_task_set(_ti.get("name",""), _ti.get("desc",""))
                self._send(200,'application/json',b'{"ok":true}')
            else:
                self._send(400,'application/json',b'{"error":"unknown task"}')

        elif p.path == '/api/window/start':
            # Both Task C and standard tasks go through the prep sequence.
            # Task C auto-starts its attempt machine when prep completes.
            prep_start_time     = time.time()
            window_start_time   = None
            _landing_win_zeroed = False
            paused              = False
            pause_start_time    = None
            total_paused_s      = 0.0
            if active_task == 'C':
                taskc_state['phase']       = TASKC_READY
                taskc_state['phase_start'] = None
            print(f"[SERVER] Prep started  task={active_task}  "
                  f"prep={round_settings['prep_s']}s  "
                  f"testing={round_settings['testing_s']}s  "
                  f"nofly={round_settings['nofly_s']}s")
            self._send(200,'application/json',b'{"ok":true}')

        elif p.path == '/api/window/skip_prep':
            prep_start_time     = None
            window_start_time   = time.time()
            _landing_win_zeroed = False
            _window_scored      = False
            paused              = False
            pause_start_time    = None
            total_paused_s      = 0.0
            print("[SERVER] Prep skipped — window opened immediately")
            _broadcast_window_start(int(active_window_s))
            self._send(200,'application/json',b'{"ok":true}')

        elif p.path == '/api/window/pause':
            # Only pauseable during prep sequence (PREP/TESTING/NO_FLY)
            # NOT once the working window is running
            _pauseable = (prep_start_time is not None and window_start_time is None)
            if _pauseable and not paused:
                paused           = True
                pause_start_time = time.time()
                print("[SERVER] ⏸  PAUSED — Mike Smith rule invoked")
                audio_mike_smith()
            elif not _pauseable:
                print("[SERVER] Pause rejected — not in prep sequence")
            self._send(200,'application/json',b'{"ok":true}')

        elif p.path == '/api/window/resume':
            if paused and pause_start_time is not None:
                total_paused_s += time.time() - pause_start_time
            paused           = False
            pause_start_time = None
            print(f"[SERVER] ▶  RESUMED  (total paused: {total_paused_s:.1f}s)")
            audio_resume()
            self._send(200,'application/json',b'{"ok":true}')

        elif p.path == '/api/window/stop':
            window_start_time   = None
            prep_start_time     = None
            _landing_win_zeroed = False
            paused              = False
            pause_start_time    = None
            total_paused_s      = 0.0
            taskc_state['phase']       = TASKC_READY
            taskc_state['phase_start'] = None
            print("[SERVER] Window stopped/reset")
            self._send(200,'application/json',b'{"ok":true}')

        elif p.path == '/api/groups':
            # Manage pre-loaded groups list
            # {"action":"add",    "group":{"name":"Group 1","pilots":{"1":"Alice",...}}}
            # {"action":"remove", "index": 0}
            # {"action":"reorder","from_idx":0,"to_idx":2}
            # {"action":"clear"}
            # {"action":"auto",   "count":8}  — auto-build all groups from checkins
            action = data.get('action','add')

            if action == 'add':
                g = data.get('group',{})
                if g.get('pilots'):
                    groups.append({'name': g.get('name',f"Group {len(groups)+1}"),
                                   'pilots': {str(k):str(v) for k,v in g['pilots'].items()}})
                    print(f"[SERVER] Group added: {groups[-1]['name']}")
                self._send(200,'application/json',b'{"ok":true}')

            elif action == 'remove':
                idx = int(data.get('index',0))
                if 0 <= idx < len(groups):
                    removed = groups.pop(idx)
                    print(f"[SERVER] Group removed: {removed['name']}")
                self._send(200,'application/json',b'{"ok":true}')

            elif action == 'reorder':
                fi = int(data.get('from_idx',0))
                ti = int(data.get('to_idx',0))
                if 0<=fi<len(groups) and 0<=ti<len(groups) and fi!=ti:
                    g = groups.pop(fi)
                    groups.insert(ti, g)
                self._send(200,'application/json',b'{"ok":true}')

            elif action == 'clear':
                groups.clear()
                print("[SERVER] Groups cleared")
                self._send(200,'application/json',b'{"ok":true}')

            elif action == 'auto':
                # Auto-build groups with even sizing.
                # max_size is the upper limit; groups differ by at most 1.
                # Algorithm: n_groups = ceil(total / max_size)
                #   base = total // n_groups
                #   remainder = total % n_groups
                #   first 'remainder' groups get base+1, rest get base
                max_size = int(data.get('count', 8))
                pool     = [n for n in roster
                            if n in (checkins if checkins else set(roster))]
                random.shuffle(pool)
                total    = len(pool)
                if total == 0:
                    self._send(200,'application/json',
                               json.dumps({'ok':True,'count':0}).encode())
                    return
                import math as _math
                n_groups  = _math.ceil(total / max_size)
                base      = total // n_groups
                remainder = total % n_groups  # first N groups get base+1
                groups.clear()
                i = 0
                for g_num in range(1, n_groups + 1):
                    size  = base + (1 if g_num <= remainder else 0)
                    chunk = pool[i:i+size]
                    g_pilots = {str(j+1): name for j,name in enumerate(chunk)}
                    groups.append({'name': f"Group {g_num}", 'pilots': g_pilots})
                    i += size
                sizes_str = '+'.join(str(len(g['pilots'])) for g in groups)
                print(f"[SERVER] Auto-built {len(groups)} groups "
                      f"(max {max_size}/group, {total} pilots: {sizes_str})")
                self._send(200,'application/json',
                           json.dumps({'ok':True,'count':len(groups)}).encode())
            else:
                self._send(400,'application/json',b'{"error":"unknown action"}')

        elif p.path == '/api/groups/edit_uid':
            # Change a pilot's unit number within a group
            # {"group_idx":0, "old_uid":2, "new_uid":5, "name":"Alice"}
            g_idx   = int(data.get('group_idx', 0))
            old_uid = str(data.get('old_uid', 0))
            new_uid = str(int(data.get('new_uid', 0)))
            name    = str(data.get('name', '')).strip()
            if 0 <= g_idx < len(groups):
                g = groups[g_idx]
                # Remove old entry, add new
                if old_uid in g['pilots'] and g['pilots'][old_uid] == name:
                    del g['pilots'][old_uid]
                    g['pilots'][new_uid] = name
                    print(f"[SERVER] Group {g_idx+1}: {name} "
                          f"unit {old_uid} → {new_uid}")
                    # If this group is currently active, update pilots live
                    if int(old_uid) in active_group_uids:
                        active_group_uids.discard(int(old_uid))
                        active_group_uids.add(int(new_uid))
                        if int(old_uid) in pilots:
                            del pilots[int(old_uid)]
                        pilots[int(new_uid)] = name
            self._send(200,'application/json',b'{"ok":true}')

        elif p.path == '/api/groups/activate':
            # Activate a group: assign pilots + set active_group_uids
            # {"index": 0}  — activates groups[0]
            idx = int(data.get('index', 0))
            if 0 <= idx < len(groups):
                g = groups[idx]
                pilots.clear()
                active_group_uids.clear()
                for k,v in g['pilots'].items():
                    try:
                        uid = int(k)
                        if 1<=uid<=20:
                            pilots[uid]   = str(v).strip()
                            active_group_uids.add(uid)
                    except: pass
                print(f"[SERVER] Activated: {g['name']}  pilots={pilots}")
                self._send(200,'application/json',b'{"ok":true}')
            else:
                self._send(400,'application/json',b'{"error":"invalid index"}')

        elif p.path == '/api/round_settings':
            # Only testing_s is configurable (1–5 min in 60s steps)
            rs = data.get('round_settings', {})
            if 'testing_s' in rs:
                t = int(rs['testing_s'])
                round_settings['testing_s'] = max(60, min(300, t))
            print(f"[SERVER] Round settings: {round_settings}")
            self._send(200,'application/json',b'{"ok":true}')

        elif p.path == '/api/contest/archive':
            # Archive current contest and start fresh.
            # {"confirm": true}  — required to prevent accidents
            # {"confirm": true, "force": true}  — allow mid-contest archive
            if not data.get('confirm'):
                self._send(400,'application/json',
                           b'{"error":"confirm required"}')
            else:
                # Check if a round is currently active
                is_active = (_c_round_idx is not None)
                if is_active and not data.get('force'):
                    # Return warning — client must re-confirm with force=true
                    self._send(409,'application/json',
                               json.dumps({
                                   'warning': True,
                                   'message': (f"Round {(_c_round_idx or 0)+1} is "
                                               f"currently active. Archive anyway?"),
                               }).encode())
                else:
                    ok, path, err = archive_contest()
                    if ok:
                        self._send(200,'application/json',
                                   json.dumps({'ok':True,
                                               'archived_as': os.path.basename(path)
                                              }).encode())
                    else:
                        self._send(500,'application/json',
                                   json.dumps({'error': err}).encode())

        elif p.path == '/api/contest/list':
            # List archived contests
            files = list_archived_contests()
            self._send(200,'application/json',
                       json.dumps({'files': files}).encode())

        elif p.path == '/api/contest/drop_mode':
            # Set the drop mode: {"mode": "auto"|"force"|"none"}
            mode = str(data.get('mode', 'auto'))
            if mode in ('auto', 'force', 'none'):
                contest['drop_mode'] = mode
                _recalc_standings()
                save_contest()
                print(f"[CONTEST] Drop mode set to: {mode}")
                self._send(200,'application/json',
                           json.dumps({'ok':True,'mode':mode}).encode())
            else:
                self._send(400,'application/json',b'{"error":"invalid mode"}')

        elif p.path == '/api/contest':
            # Set/update contest metadata and round plan
            # {"contest":{...}, "rounds":[{"task":"G"}, ...]}
            if 'contest' in data:
                contest.update({k:v for k,v in data['contest'].items()
                                 if k in ('name','date','num_units','num_rounds')})
            if 'rounds' in data:
                tasks = [r.get('task','G') for r in data['rounds']]
                contest_build_rounds(len(tasks), tasks)
            else:
                save_contest()
            print(f"[CONTEST] Updated: {contest}")
            self._send(200,'application/json',b'{"ok":true}')

        elif p.path == '/api/contest/draw':
            # Draw all groups for all planned rounds
            # {"group_size": 8}
            gs = int(data.get('group_size', contest.get('num_units', 8)))
            for i, rnd in enumerate(contest_rounds):
                if rnd['status'] == 'planned':
                    contest_draw_groups(i, gs)
            # Count pilots and groups across all drawn rounds
            total_pilots = sum(
                len(g['pilots'])
                for rnd in contest_rounds
                for g in rnd.get('groups', [])
            )
            total_groups = sum(
                len(rnd.get('groups', []))
                for rnd in contest_rounds
            )
            self._send(200,'application/json',
                       json.dumps({'ok':True,'rounds':len(contest_rounds),
                                   'pilots':total_pilots,
                                   'groups':total_groups}).encode())

        elif p.path == '/api/contest/next_group':
            # Skip to the next group — delegates to module-level function
            # to avoid global declaration inside do_POST
            _contest_next_group()
            self._send(200,'application/json',b'{"ok":true}')

        elif p.path.startswith('/api/rescore/pick/'):
            # File-picker rescore: POST /api/rescore/pick/<ri>/<gi>
            # Opens a native Windows file dialog on the server machine.
            parts = p.path.strip('/').split('/')
            try:
                ri, gi = int(parts[3]), int(parts[4])
            except (IndexError, ValueError):
                self._send(400, 'application/json', b'{"error":"bad ri/gi"}')
                return
            ok, msg = _rescore_from_picked_file(ri, gi)
            self._send(200, 'application/json',
                       json.dumps({'ok': ok, 'message': msg}).encode())

        elif p.path.startswith('/api/rescore/'):
            # Auto rescore from logs: POST /api/rescore/<ri>/<gi>
            parts = p.path.strip('/').split('/')
            try:
                ri, gi = int(parts[2]), int(parts[3])
            except (IndexError, ValueError):
                self._send(400, 'application/json', b'{"error":"bad ri/gi"}')
                return
            ok, msg = _rescore_round_from_logs(ri, gi)
            self._send(200, 'application/json',
                       json.dumps({'ok': ok, 'message': msg}).encode())

        elif p.path == '/api/contest/start':
            # Start the contest from the first planned round
            # Finds the first non-complete round and activates its first group
            started = False
            for i, rnd in enumerate(contest_rounds):
                if rnd['status'] != 'complete':
                    if not rnd['groups']:
                        gs = int(contest.get('num_units', 8))
                        contest_draw_groups(i, gs)
                    contest_activate_group(i, 0, auto=False)
                    started = True
                    break
            if not started:
                self._send(400,'application/json',
                           b'{"error":"no planned rounds"}')
            else:
                self._send(200,'application/json',b'{"ok":true}')

        elif p.path == '/api/contest/activate':
            # Manually activate a specific round/group
            # {"round_idx":0, "group_idx":0}
            ri = int(data.get('round_idx', 0))
            gi = int(data.get('group_idx', 0))
            if 0<=ri<len(contest_rounds) and 0<=gi<len(contest_rounds[ri]['groups']):
                contest_activate_group(ri, gi, auto=False)
                self._send(200,'application/json',b'{"ok":true}')
            else:
                self._send(400,'application/json',b'{"error":"invalid index"}')

        elif p.path == '/api/contest/edit_uid':
            # Edit a pilot's unit assignment in contest_rounds
            # {"round_idx":0,"group_idx":0,"old_uid":2,"new_uid":5,"name":"Alice"}
            ri      = int(data.get('round_idx', 0))
            gi      = int(data.get('group_idx', 0))
            old_uid = str(int(data.get('old_uid', 0)))
            new_uid = str(int(data.get('new_uid', 0)))
            name    = str(data.get('name', '')).strip()
            ok = False
            if (0 <= ri < len(contest_rounds) and
                    0 <= gi < len(contest_rounds[ri]['groups'])):
                grp = contest_rounds[ri]['groups'][gi]
                already_taken = (new_uid in grp['pilots'] and
                                 grp['pilots'][new_uid] != name)
                if already_taken:
                    # Collision — another pilot already has this unit
                    taken_by = grp['pilots'][new_uid]
                    self._send(409,'application/json',
                               json.dumps({'ok':False,
                                 'error':'collision',
                                 'taken_by':taken_by}).encode())
                    return
                if old_uid in grp['pilots'] and grp['pilots'][old_uid] == name:
                    del grp['pilots'][old_uid]
                    grp['pilots'][new_uid] = name
                    ok = True
                    # If this group is currently active, update live assignment
                    if (_c_round_idx == ri and _c_group_idx == gi):
                        active_group_uids.discard(int(old_uid))
                        active_group_uids.add(int(new_uid))
                        if int(old_uid) in pilots:
                            del pilots[int(old_uid)]
                        pilots[int(new_uid)] = name
                    save_contest()
                    print(f"[CONTEST] R{ri+1} G{gi+1}: {name} "
                          f"unit {old_uid} → {new_uid}")
            self._send(200,'application/json',
                       json.dumps({'ok':ok}).encode())

        elif p.path == '/api/contest/mark_group_complete':
            # Mark a specific group complete with empty results (skip/forfeit)
            # {"round_idx":0, "group_idx":0}
            ri = int(data.get('round_idx', 0))
            gi = int(data.get('group_idx', 0))
            if 0<=ri<len(contest_rounds) and 0<=gi<len(contest_rounds[ri]['groups']):
                grp = contest_rounds[ri]['groups'][gi]
                grp['status']  = 'complete'
                grp['results'] = {}
                # Check if all groups now complete → close out the round
                rnd = contest_rounds[ri]
                if all(g['status']=='complete' for g in rnd['groups']):
                    _contest_complete_round(ri)
                else:
                    save_contest()
                print(f"[CONTEST] Marked R{ri+1} G{gi+1} complete (skipped)")
                self._send(200,'application/json',b'{"ok":true}')
            else:
                self._send(400,'application/json',b'{"error":"invalid index"}')

        elif p.path == '/api/reset':
            window_start_time   = None
            prep_start_time     = None
            _landing_win_zeroed = False
            paused              = False
            pause_start_time    = None
            total_paused_s      = 0.0
            taskc_state['phase']       = TASKC_READY
            taskc_state['phase_start'] = None
            _reset_scores()
            print("[SERVER] Round reset")
            self._send(200,'application/json',b'{"ok":true}')

        # ── F3XVault integration endpoints ────────────────────────────

        elif p.path == '/api/vault/pull':
            # Pull event data from F3XVault and run pilot name matching.
            # Optional body: {"event_id": 1234}  — overrides config event_id.
            # Returns match report so the UI can show REVIEW/UNMATCHED pilots.
            global _vault_pull_data, _vault_pilot_map, _vault_match_report
            if not _VAULT_AVAILABLE:
                self._send(503,'application/json',
                           b'{"ok":false,"error":"f3xvault module not installed"}')
                return
            override_eid = data.get('event_id')
            pull = vault_pull_contest(override_eid)
            if not pull['ok']:
                self._send(500,'application/json',
                           json.dumps({'ok':False,'error':pull['error']}).encode())
                return
            _vault_pull_data = pull
            # Run name matching against current roster
            overrides = config.get('f3xvault', {}).get('pilot_overrides', {})
            vault_raw_pilots = pull['raw'].get('pilots', [])
            _vault_pilot_map, _vault_match_report = build_pilot_map_with_matching(
                roster, vault_raw_pilots, overrides, verbose=True
            )
            # Build response for UI
            matched  = [{'name': m.roster_name, 'vault_name': m.vault_name,
                         'vault_id': m.vault_id, 'bib': m.vault_bib,
                         'method': m.method, 'score': round(m.score, 2)}
                        for m in _vault_match_report.matched]
            review   = [{'name': m.roster_name, 'vault_name': m.vault_name,
                         'vault_id': m.vault_id, 'score': round(m.score, 2),
                         'method': m.method}
                        for m in _vault_match_report.review]
            unmatched= [{'name': u.roster_name, 'closest': u.best_vault,
                         'score': round(u.best_score, 2)}
                        for u in _vault_match_report.unmatched]
            self._send(200,'application/json', json.dumps({
                'ok':        True,
                'event_id':  pull['event_id'],
                'event_name':pull['event_name'],
                'start_date':pull['start_date'],
                'total_rounds': pull['total_rounds'],
                'round_tasks':   {str(k): v for k, v in pull['round_tasks'].items()},
                'round_sub_counts': {str(k): v for k, v in pull['round_sub_counts'].items()},
                'matched':   matched,
                'review':    review,
                'unmatched': unmatched,
                'skipped':   _vault_match_report.skipped,
                'all_ok':    _vault_match_report.all_ok,
            }).encode())

        elif p.path == '/api/vault/push_round':
            # Push a completed round's scores to F3XVault.
            # Body: {"round_idx": 0, "group_idx": 0}
            # round_idx/group_idx index into contest_rounds.
            # Requires a prior successful /api/vault/pull.
            if not _VAULT_AVAILABLE:
                self._send(503,'application/json',
                           b'{"ok":false,"error":"f3xvault module not installed"}')
                return
            if not _vault_pilot_map:
                self._send(400,'application/json',
                           b'{"ok":false,"error":"No vault pull data - run Pull first"}')
                return
            ri = int(data.get('round_idx', 0))
            gi = int(data.get('group_idx', 0))
            try:
                rnd = contest_rounds[ri]
                grp = rnd['groups'][gi]
            except (IndexError, KeyError):
                self._send(400,'application/json',
                           b'{"ok":false,"error":"Invalid round/group index"}')
                return
            if grp.get('status') != 'complete':
                self._send(400,'application/json',
                           b'{"ok":false,"error":"Group not yet complete"}')
                return
            task_id     = rnd.get('task', 'LL')
            round_num   = rnd.get('round_num', ri + 1)
            sub_counts  = _vault_pull_data.get('round_sub_counts', {})
            # Build pilot_scores list from saved group results
            pilot_scores = []
            for pname, res in grp.get('results', {}).items():
                uid = res.get('unit_id')
                u   = units.get(uid, {})
                pilot_scores.append({
                    'name':           pname,
                    'group':          res.get('group', ''),
                    'order':          res.get('order', 0),
                    'task_flights':   res.get('flights', []),
                    'scored_flights': u.get('scored_flights', []),
                })
            def _do_push():
                result = vault_post_round(
                    round_number     = round_num,
                    task_id          = task_id,
                    pilot_scores     = pilot_scores,
                    vault_pilot_map  = _vault_pilot_map,
                    round_sub_counts = sub_counts,
                    mark_scored      = True,
                )
                print(f"[VAULT] Push R{round_num} G{gi+1}: "
                      f"{len(result['posted'])} OK, {len(result['failed'])} failed")
            Thread(target=_do_push, daemon=True).start()
            self._send(200,'application/json', json.dumps({
                'ok':      True,
                'message': f"Push started for R{round_num} G{gi+1} "
                           f"({len(pilot_scores)} pilots)",
            }).encode())

        elif p.path == '/api/vault/push_all':
            # Push ALL completed rounds/groups to F3XVault in one shot.
            # Runs in a background thread — returns immediately.
            # Body: {} (uses stored pull data and all complete groups)
            if not _VAULT_AVAILABLE:
                self._send(503,'application/json',
                           b'{"ok":false,"error":"f3xvault module not installed"}')
                return
            if not _vault_pilot_map:
                self._send(400,'application/json',
                           b'{"ok":false,"error":"No vault pull data - run Pull first"}')
                return
            sub_counts = _vault_pull_data.get('round_sub_counts', {})
            def _push_all():
                total_ok = 0; total_fail = 0
                for ri, rnd in enumerate(contest_rounds):
                    task_id   = rnd.get('task', 'LL')
                    round_num = rnd.get('round_num', ri + 1)
                    for gi, grp in enumerate(rnd.get('groups', [])):
                        if grp.get('status') != 'complete':
                            continue
                        pilot_scores = []
                        for pname, res in grp.get('results', {}).items():
                            uid = res.get('unit_id')
                            u   = units.get(uid, {})
                            pilot_scores.append({
                                'name':           pname,
                                'group':          res.get('group', ''),
                                'order':          res.get('order', 0),
                                'task_flights':   res.get('flights', []),
                                'scored_flights': u.get('scored_flights', []),
                            })
                        if not pilot_scores:
                            continue
                        result = vault_post_round(
                            round_number     = round_num,
                            task_id          = task_id,
                            pilot_scores     = pilot_scores,
                            vault_pilot_map  = _vault_pilot_map,
                            round_sub_counts = sub_counts,
                            mark_scored      = True,
                        )
                        total_ok   += len(result['posted'])
                        total_fail += len(result['failed'])
                print(f"[VAULT] Push all complete: {total_ok} OK, {total_fail} failed")
            Thread(target=_push_all, daemon=True).start()
            n_complete = sum(
                1 for rnd in contest_rounds
                for grp in rnd.get('groups', [])
                if grp.get('status') == 'complete'
            )
            self._send(200,'application/json', json.dumps({
                'ok':     True,
                'message': f"Push all started — {n_complete} completed groups",
            }).encode())

        else:
            self._send(404,'text/plain',b'Not found')

    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin','*')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args): pass


# ─────────────────────────────────────────────
#  Contest Engine
#  Multi-round contest management, persistence,
#  cross-group normalisation, standings, and
#  fully automatic group/round advance.
# ─────────────────────────────────────────────

CONTEST_FILE  = 'contest.json'
CONTESTS_DIR  = 'contests'   # archived contest files live here

FAI_DROP_TABLE = [(5, 1), (9, 2)]   # (min_rounds_complete, drops)

def _drop_count(n_complete):
    drops = 0
    for thresh, count in FAI_DROP_TABLE:
        if n_complete >= thresh:
            drops = count
    return drops

# ── Contest state ─────────────────────────────
contest = {
    'name':       'F3K Contest',
    'date':       '',
    'num_units':  8,
    'num_rounds': 6,
    'drop_mode':  'auto',   # 'auto' (FAI) | 'force' | 'none'
}
contest_rounds    = []   # list of round dicts
contest_standings = {}   # pilot → {rounds, total, dropped}

# Active round/group pointers
_c_round_idx       = None   # index into contest_rounds (None = not running)
_c_group_idx       = None   # index into current round's groups
_c_group_advanced  = False  # guard: fire auto-advance only once per window close


def load_contest():
    global contest, contest_rounds, contest_standings, checkins
    if not os.path.exists(CONTEST_FILE):
        return
    try:
        with open(CONTEST_FILE) as f:
            data = json.load(f)
        contest           = data.get('contest', contest)
        contest_rounds    = data.get('rounds', [])
        contest_standings = data.get('standings', {})
        saved_ci = data.get('checkins', {})
        if saved_ci:
            checkins.clear()
            checkins.update({k: v for k, v in saved_ci.items()})
            print(f"[CONTEST] Restored {len(checkins)} check-ins")
        print(f"[CONTEST] Loaded  rounds={len(contest_rounds)}  "
              f"name={contest['name']}")
    except Exception as e:
        print(f"[CONTEST] Load error: {e}")


def save_contest():
    try:
        with open(CONTEST_FILE, 'w') as f:
            json.dump({'contest':contest,'rounds':contest_rounds,
                       'standings':contest_standings,
                       'checkins':checkins}, f, indent=2)
    except Exception as e:
        print(f"[CONTEST] Save error: {e}")


def archive_contest():
    """
    Archive the current contest.json into contests/<name>_<date>[_N].json
    then reset all contest state to a fresh blank contest.
    Returns (ok, archive_path, error_msg).
    """
    global contest, contest_rounds, contest_standings
    global _c_round_idx, _c_group_idx, _c_group_advanced
    global active_task, task_cfg_ovr, active_window_s
    global window_start_time, prep_start_time, _landing_win_zeroed
    global paused, pause_start_time, total_paused_s

    # Ensure contests/ directory exists
    os.makedirs(CONTESTS_DIR, exist_ok=True)

    # Build archive filename from contest name and date
    raw_name = contest.get('name', 'F3K Contest')
    raw_date = contest.get('date', '') or time.strftime('%Y-%m-%d')
    safe_name = ''.join(c if c.isalnum() or c in '-_' else '_'
                        for c in raw_name.replace(' ', '_'))
    safe_date = raw_date.replace('/', '-').replace(' ', '')
    base = os.path.join(CONTESTS_DIR, f"contest_{safe_name}_{safe_date}")
    archive_path = base + '.json'

    # Avoid overwriting an existing archive — append counter
    counter = 2
    while os.path.exists(archive_path):
        archive_path = f"{base}_{counter}.json"
        counter += 1

    # Copy current contest.json to archive path
    try:
        # Always write current in-memory state first so archive is up to date
        with open(CONTEST_FILE, 'w') as f:
            json.dump({'contest': contest, 'rounds': contest_rounds,
                       'standings': contest_standings}, f, indent=2)
        import shutil
        shutil.copy2(CONTEST_FILE, archive_path)
    except Exception as e:
        return False, None, str(e)

    # Reset all contest state
    contest = {
        'name':       'F3K Contest',
        'date':       time.strftime('%Y-%m-%d'),
        'num_units':  contest.get('num_units', 8),   # preserve unit count
        'num_rounds': 6,
        'drop_mode':  'auto',
    }
    contest_rounds    = []
    contest_standings = {}
    _c_round_idx      = None
    _c_group_idx      = None
    _c_group_advanced = False

    # Reset active window state
    window_start_time   = None
    prep_start_time     = None
    _landing_win_zeroed = False
    paused              = False
    pause_start_time    = None
    total_paused_s      = 0.0

    # Write fresh contest.json
    save_contest()
    print(f"[CONTEST] Archived → {archive_path}")
    print(f"[CONTEST] Fresh contest.json created")
    return True, archive_path, None


def list_archived_contests():
    """Return sorted list of archived contest filenames."""
    if not os.path.isdir(CONTESTS_DIR):
        return []
    files = [f for f in os.listdir(CONTESTS_DIR)
             if f.endswith('.json') and f.startswith('contest_')]
    return sorted(files, reverse=True)   # newest first


def contest_build_rounds(n_rounds, task_list):
    """
    (Re)build the round list with given tasks.
    Preserves complete rounds; replaces planned/active ones.
    """
    global contest_rounds
    new_rounds = []
    for i in range(n_rounds):
        task = task_list[i] if i < len(task_list) else 'G'
        if i < len(contest_rounds) and contest_rounds[i]['status'] == 'complete':
            r = dict(contest_rounds[i])
            r['task'] = task
            new_rounds.append(r)
        else:
            new_rounds.append({
                'round_num': i + 1,
                'task': task, 'task_cfg': {},
                'status': 'planned', 'groups': [],
            })
    contest_rounds = new_rounds
    save_contest()


def contest_draw_groups(round_idx, group_size):
    """
    Draw all groups for a round from checked-in pilots.
    Applies even-sizing and unit-continuity optimisation.
    """
    import math as _math
    pool = [n for n in roster
            if n in (checkins if checkins else set(roster))]
    random.shuffle(pool)
    total = len(pool)
    if total == 0:
        return

    n_grp     = _math.ceil(total / group_size)
    base      = total // n_grp
    remainder = total % n_grp

    # Prefer same unit as last round for each pilot
    prev_map = {}
    if round_idx > 0 and round_idx <= len(contest_rounds):
        for g in contest_rounds[round_idx-1].get('groups', []):
            for uid_s, name in g['pilots'].items():
                prev_map[name] = int(uid_s)
    # Pre-assigned units (pilot owns their unit) override continuity
    for n in pool:
        if n in checkins and checkins[n] is not None:
            prev_map[n] = checkins[n]

    rnd = contest_rounds[round_idx]
    rnd['groups'] = []
    # All pre-assigned units across the entire pool — auto-assign must never use these
    all_preassigned = {checkins[n] for n in pool if n in checkins and checkins[n] is not None}
    i = 0
    for gn in range(1, n_grp + 1):
        size  = base + (1 if gn <= remainder else 0)
        chunk = pool[i:i+size]; i += size
        used  = set()
        asgn  = {}
        # Pass 1: assign pre-assigned and preferred units
        for name in chunk:
            p = prev_map.get(name)
            if p and p not in used:
                asgn[name] = p; used.add(p)
        # Pass 2: auto-assign remaining pilots, skipping ALL pre-assigned units
        uid = 1
        for name in chunk:
            if name not in asgn:
                while uid in used or uid in all_preassigned: uid += 1
                asgn[name] = uid; used.add(uid); uid += 1
        rnd['groups'].append({
            'group_num': gn,
            'pilots':    {str(v): k for k, v in asgn.items()},
            'status':    'planned',
            'results':   {},
        })
    rnd['status'] = 'planned'
    save_contest()
    print(f"[CONTEST] Drew R{round_idx+1}: {n_grp} groups from {total} pilots")


def contest_activate_group(round_idx, group_idx, auto=False):
    """
    Activate a specific group: assign pilots, reset scores,
    set task, start prep sequence automatically.
    """
    global _c_round_idx, _c_group_idx, _c_group_advanced
    global active_task, task_cfg_ovr, active_window_s
    global window_start_time, prep_start_time, _landing_win_zeroed
    global paused, pause_start_time, total_paused_s

    if round_idx >= len(contest_rounds):
        return
    rnd = contest_rounds[round_idx]
    grp = rnd['groups'][group_idx]

    # Set task
    active_task     = rnd['task']
    task_cfg_ovr    = rnd.get('task_cfg', {})
    active_window_s = int(task_cfg_ovr.get('window',
                          TASKS.get(active_task, {}).get('window', 0)))

    # Assign pilots
    pilots.clear()
    active_group_uids.clear()
    for uid_s, name in grp['pilots'].items():
        try:
            uid = int(uid_s)
            if 1 <= uid <= 20:
                pilots[uid] = name
                active_group_uids.add(uid)
        except: pass

    # Reset window and scores
    window_start_time   = None
    prep_start_time     = None
    _landing_win_zeroed = False
    paused              = False
    pause_start_time    = None
    total_paused_s      = 0.0
    _c_group_advanced   = False
    _reset_scores()

    # Task C reset
    if active_task == 'C':
        taskc_state.update({
            'phase': TASKC_READY,
            'num_attempts': task_cfg_ovr.get('numFlights', 3),
            'attempt': 0, 'phase_start': None,
        })

    grp['status'] = 'active'
    _c_round_idx  = round_idx
    _c_group_idx  = group_idx

    n_grps  = len(rnd['groups'])
    n_rnds  = len(contest_rounds)
    print(f"[CONTEST] {'AUTO ' if auto else ''}Activated "
          f"R{round_idx+1}/{n_rnds} G{group_idx+1}/{n_grps}  "
          f"task={active_task}  pilots={list(pilots.values())}")

    # Kick off prep sequence immediately
    prep_start_time = time.time()

    # Audio: call to line + task announcement
    audio_prep_start(list(pilots.values()))
    ti = TASKS.get(active_task, {})
    audio_task_set(ti.get('name',''), ti.get('desc',''))

    # If not the very first group, announce which group/round this is
    if auto:
        n_grps = len(rnd['groups'])
        if group_idx == 0:
            _q({'type':'tts',
                'text': f"Round {round_idx+1} — {ti.get('name','')}. "
                        f"Group 1 of {n_grps}."})
        else:
            _q({'type':'tts',
                'text': f"Group {group_idx+1} of {n_grps}."})


def contest_save_group_results():
    """
    Save current active group's flight results from live unit data,
    normalise within the group (best in group = 1000), and immediately
    write contest.json so results are safe across restarts.
    """
    if _c_round_idx is None or _c_group_idx is None:
        return
    grp = contest_rounds[_c_round_idx]['groups'][_c_group_idx]
    for uid, name in pilots.items():
        u = units.get(uid)
        if not u: continue
        grp['results'][name] = {
            'task_raw_s': round(u['task_raw_s'], 2),
            'normalised': None,   # filled below
            'flights': [
                {'dur':        round(f['dur'],   1),
                 'lh_ft':      round(f['lh_ft'], 1),
                 'capped_dur': round(f.get('capped_dur', f['dur']), 1),
                 'peak_alt_ft':round(f.get('peak_alt_ft', 0.0), 1),
                 'dq':         bool(f.get('dq', False))}
                for f in u.get('task_flights', [])
            ],
        }
    grp['status'] = 'complete'

    # Normalise within this group immediately — best raw in group = 1000
    best = max((r['task_raw_s'] for r in grp['results'].values()), default=0.0)
    for res in grp['results'].values():
        res['normalised'] = round(res['task_raw_s'] / best * 1000, 1) if best > 0 else 0.0

    # Update standings with what we have so far, then persist to disk
    _recalc_standings()
    save_contest()

    print(f"[CONTEST] Saved+normalised R{_c_round_idx+1} "
          f"G{_c_group_idx+1}: {list(grp['results'].keys())}")


def contest_auto_advance():
    """
    Called once when WIN_CLOSED fires (guarded by _c_group_advanced).
    Saves results then advances to next group or next round automatically.
    """
    global _c_group_advanced, _c_round_idx, _c_group_idx

    if _c_round_idx is None:
        return   # no active contest round
    if _c_group_advanced:
        return   # already fired this window

    _c_group_advanced = True

    contest_save_group_results()

    rnd     = contest_rounds[_c_round_idx]
    n_grps  = len(rnd['groups'])
    n_rnds  = len(contest_rounds)
    next_gi = _c_group_idx + 1

    if next_gi < n_grps:
        # More groups in this round → activate next
        contest_activate_group(_c_round_idx, next_gi, auto=True)

    else:
        # Round complete → cross-group normalise + standings
        _contest_complete_round(_c_round_idx)

        next_ri = _c_round_idx + 1
        if next_ri < n_rnds:
            # Draw groups for next round if not already drawn
            rnd_next = contest_rounds[next_ri]
            if not rnd_next['groups']:
                gs = contest.get('num_units', 8)
                contest_draw_groups(next_ri, gs)
            # Activate first group of next round
            contest_activate_group(next_ri, 0, auto=True)
        else:
            # All rounds complete
            _c_round_idx = None
            _c_group_idx = None
            audio_round_complete()
            _q({'type':'tts',
                'text': "Contest complete. All rounds have been flown. "
                        "Thank you for flying."})
            save_contest()


def _contest_complete_round(round_idx):
    """
    Mark round complete and update standings.
    Groups are already individually normalised when saved —
    no cross-group normalisation applied.
    """
    rnd = contest_rounds[round_idx]
    rnd['status'] = 'complete'
    _recalc_standings()
    save_contest()

    n_done  = sum(1 for r in contest_rounds if r['status']=='complete')
    n_total = len(contest_rounds)
    print(f"[CONTEST] Round {round_idx+1} complete  ({n_done}/{n_total})")


def _recalc_standings():
    global contest_standings
    all_pilots = set()
    for rnd in contest_rounds:
        for g in rnd.get('groups', []):
            all_pilots.update(g['results'].keys())

    n_done    = sum(1 for r in contest_rounds if r['status']=='complete')
    drop_mode = contest.get('drop_mode', 'auto')
    if drop_mode == 'none':
        drops = 0
    elif drop_mode == 'force':
        drops = 1          # CD-forced: always drop 1 worst regardless of round count
    else:
        drops = _drop_count(n_done)   # FAI auto rule

    standings = {}
    for name in all_pilots:
        scores = []
        for rnd in contest_rounds:
            if rnd['status'] != 'complete':
                scores.append(None); continue
            score = None
            for g in rnd['groups']:
                if name in g['results']:
                    score = g['results'][name]['normalised']
                    break
            scores.append(score)

        valid   = [(i, s) for i, s in enumerate(scores) if s is not None]
        dropped = []
        if drops > 0 and len(valid) > drops:
            worst   = sorted(valid, key=lambda x: x[1])[:drops]
            dropped = [i for i, _ in worst]

        total = sum(s for i, s in valid if i not in dropped)
        standings[name] = {
            'rounds':  scores,
            'total':   round(total, 1),
            'dropped': dropped,
        }
    contest_standings = standings


def contest_state_dict():
    """Compact contest state for /state JSON response."""
    return {
        'contest':    {**contest},   # includes drop_mode
        'rounds':     [{
            'round_num': r['round_num'],
            'task':      r['task'],
            'status':    r['status'],
            'n_groups':  len(r['groups']),
            'n_complete':sum(1 for g in r['groups'] if g['status']=='complete'),
        } for r in contest_rounds],
        'standings':  contest_standings,
        'active_round': _c_round_idx,
        'active_group': _c_group_idx,
    }


def _reset_scores():
    for u in units.values():
        u['task_flights']=[]; u['task_raw_s']=0.0
        u['task_detail']=''; u['task_progress']=''
        u['normalised_score']=None; u['flight_count']=0
        u['best_raw_s']=0.0; u['best_adjusted_s']=None; u['best_task_raw_s']=0.0
        u['alt_history']=[]

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """HTTP server that handles each request in a separate thread.
    Prevents single slow request from blocking all other clients.
    Essential on Windows where concurrent browser requests would
    otherwise queue behind each other and appear to hang.
    """
    daemon_threads = True   # threads die with the server

def run_http(port):
    ThreadedHTTPServer(('0.0.0.0', port), Handler).serve_forever()



HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>F3K Scoring</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@300;400;600;700;900&family=Roboto+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#ffffff;--surf:#ffffff;--bdr:#bbc4d0;
  --cyan:#1565c0;--warn:#e65100;--danger:#c62828;
  --good:#2e7d32;--pen:#6a1b9a;--purple:#4527a0;
  --text:#0d1117;--muted:#3d5166;
  --mono:'Roboto Mono',monospace;--disp:'Barlow Condensed',sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;overflow:hidden}
body{background:var(--bg);color:var(--text);font-family:var(--disp);font-size:17px;
  display:flex;flex-direction:column}

/* ── Header ── */
header{display:flex;align-items:center;justify-content:space-between;
  padding:12px 24px;border-bottom:2px solid var(--bdr);
  background:var(--surf);position:sticky;top:0;z-index:100;gap:12px;flex-wrap:wrap;flex-shrink:0;box-shadow:0 2px 6px rgba(0,0,0,.08)}
.logo{display:flex;align-items:baseline;gap:8px}
.logo-f3k{font-size:2rem;font-weight:900;letter-spacing:.08em;color:var(--cyan)}
.logo-sub{font-size:.75rem;font-weight:800;color:var(--muted);letter-spacing:.18em;text-transform:uppercase}
.hdr-r{display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.pill{font-family:var(--mono);font-size:.82rem;font-weight:800;color:var(--text)}
.pill span{color:var(--cyan);font-weight:800}
.spec-links{display:flex;align-items:center;gap:6px;margin-left:6px;
  padding-left:10px;border-left:1.5px solid var(--bdr)}
.spec-link{font-family:var(--mono);font-size:.68rem;color:var(--muted);
  text-decoration:none;padding:2px 6px;border:1.5px solid var(--bdr);
  border-radius:2px;letter-spacing:.08em;transition:all .15s}
.spec-link:hover{color:var(--cyan);border-color:rgba(0,229,255,.4)}
#cdot{width:7px;height:7px;border-radius:50%;background:var(--danger);
  display:inline-block;margin-right:5px;transition:background .3s}
#cdot.live{background:var(--good)}

/* ── Tab bar ── */
.tabs{display:flex;border-bottom:2px solid var(--bdr);background:var(--surf);padding:0 24px;position:sticky;top:0;z-index:99;flex-shrink:0}
.tab{padding:11px 20px;font-family:var(--disp);font-size:.9rem;font-weight:700;
  letter-spacing:.1em;text-transform:uppercase;color:var(--muted);
  cursor:pointer;border-bottom:2px solid transparent;transition:color .2s,border-color .2s}
.tab:hover{color:var(--text)}
.tab-sep{width:1px;background:var(--bdr);margin:6px 4px;flex-shrink:0}
.tab.active{color:var(--cyan);border-bottom-color:var(--cyan);border-bottom-width:3px}

/* ── Panel ── */
.panel{display:none;padding:20px 24px}
.panel.active{display:block;overflow-y:auto;flex:1;min-height:0;width:100%}

/* ── Section label ── */
.sec{font-size:.72rem;font-weight:800;letter-spacing:.20em;color:var(--text);text-transform:uppercase;
  color:var(--muted);margin-bottom:10px;display:flex;align-items:center;gap:10px}
.sec::after{content:'';flex:1;height:2px;background:var(--bdr)}

/* ── Banners ── */
.banner{flex-shrink:0;background:rgba(255,107,53,.07);border-bottom:2px solid var(--warn);
  padding:6px 24px;font-family:var(--mono);font-size:.7rem;color:var(--pen);
  letter-spacing:.07em;display:flex;gap:28px;flex-wrap:wrap}

/* ── Window bar ── */
.window-bar{display:flex;align-items:center;gap:16px;background:#fff;
  padding:10px 24px;border-bottom:2px solid var(--bdr);
  background:#ffffff;flex-wrap:wrap;position:sticky;top:0;z-index:98;flex-shrink:0}
.win-task{font-family:var(--disp);font-size:1.25rem;font-weight:800;
  color:var(--purple);letter-spacing:.05em}
.win-desc{font-family:var(--mono);font-size:.7rem;color:var(--muted)}
.win-spacer{flex:1}
.win-rg{font-family:var(--disp);font-size:1rem;font-weight:800;
  color:var(--warn);white-space:nowrap}
.win-timer-wrap{display:flex;align-items:center;gap:10px}
.win-label{font-family:var(--mono);font-size:.65rem;color:var(--muted);
  letter-spacing:.15em;text-transform:uppercase}
.win-timer{font-family:var(--mono);font-size:1.9rem;font-weight:700;
  color:var(--cyan);letter-spacing:.05em;min-width:80px;text-align:right;
  transition:color .3s}
.win-timer.warn{color:var(--warn)}
.win-timer.crit{color:var(--danger)}
.win-timer.done{color:var(--muted)}
.win-timer.none{color:var(--muted);font-size:.85rem}
.win-progress{width:180px;height:6px;background:#d0d7e2;border-radius:3px;overflow:hidden}
.win-fill{height:100%;border-radius:3px;background:var(--cyan);
  transition:width .2s,background .3s}
.win-fill.warn{background:var(--warn)}
.win-fill.crit{background:var(--danger)}
.win-btn{font-family:var(--mono);font-size:.75rem;font-weight:700;
  letter-spacing:.1em;text-transform:uppercase;padding:5px 12px;
  border:1.5px solid var(--bdr);border-radius:3px;cursor:pointer;
  background:transparent;color:var(--text);transition:border-color .2s,color .2s}
.win-btn:hover{border-color:var(--cyan);color:var(--cyan)}
.win-btn.active{border-color:var(--good);color:var(--good)}
.win-btn.stop{border-color:var(--danger);color:var(--danger)}
.win-btn.skip{border-color:var(--muted);color:var(--muted);font-size:.62rem}
.win-btn.pause{border-color:var(--warn);color:var(--warn)}
.win-btn.resume{border-color:var(--good);color:var(--good)}
.win-timer.paused{color:var(--warn);animation:blink 1s step-end infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.3}}
.paused-banner{background:rgba(255,179,0,.12);border:2px solid #e65100;
  border-radius:3px;padding:5px 12px;font-family:var(--mono);font-size:.7rem;
  color:var(--warn);letter-spacing:.08em;display:none;margin-left:12px}
/* Round settings boxes */
.rs-row{display:flex;align-items:center;gap:10px;margin-bottom:8px}
.rs-label{font-size:.88rem;font-weight:700;color:var(--text);letter-spacing:.1em;flex:1;min-width:120px}
.rs-input{background:#ffffff;border:2px solid var(--bdr);color:var(--text);
  padding:8px 10px;border-radius:4px;font-family:var(--mono);font-size:.82rem;width:70px}
.rs-unit{font-family:var(--mono);font-size:.7rem;color:var(--muted)}
.rs-note{font-size:.68rem;color:var(--muted);font-style:italic;margin-top:4px}
.rs-warn{font-size:.68rem;color:var(--danger);margin-top:4px;display:none}

/* ── Unit cards ── */
#grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));
  gap:11px;margin-bottom:28px}
.card{background:var(--surf);border:1.5px solid var(--bdr);border-radius:4px;
  padding:13px 15px;position:relative;overflow:hidden;transition:border-color .2s,background .2s}
.card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:var(--muted);transition:background .2s}
.s-launch{background:#fff3e0;border-color:var(--warn)}
.s-launch::before{background:var(--warn)}
.s-flight{background:#e3f0fb;border-color:var(--cyan);animation:pulse 2s ease-in-out infinite}
.s-flight::before{background:var(--cyan)}
.s-landed{background:#e8f5e9;border-color:var(--good)}
.s-landed::before{background:var(--good)}
.stale{opacity:.35}

/* ── Card header: unit + pilot on one line ── */
.ctop{display:flex;justify-content:space-between;align-items:center;margin-bottom:9px}
.card-id-row{display:flex;align-items:baseline;gap:12px;flex:1;min-width:0}
.uid{font-size:2.6rem;font-weight:900;line-height:1;flex-shrink:0;color:var(--text)}
.s-launch .uid{color:var(--warn)}.s-flight .uid{color:var(--cyan)}.s-landed .uid{color:var(--good)}
.pilot-name{font-size:1.9rem;font-weight:900;line-height:1;
  color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
  letter-spacing:-.01em}
.badge{font-family:var(--mono);font-size:.56rem;letter-spacing:.1em;flex-shrink:0;
  padding:3px 6px;border-radius:2px;text-transform:uppercase;
  background:var(--bdr);color:var(--muted)}
.b-LAUNCH\ WIN{background:#ffe0b2;color:#bf360c;font-weight:800}
.b-GROUND{background:#e0e0e0;color:#212121;font-weight:800}
.b-FLIGHT{background:#bbdefb;color:#0d47a1;font-weight:800}
.b-LANDED{background:#c8e6c9;color:#1b5e20;font-weight:800}

.metrics{display:grid;grid-template-columns:1fr 1fr 1fr;gap:7px 5px}
.met{display:flex;flex-direction:column;gap:1px}
.ml{font-size:.65rem;font-weight:800;letter-spacing:.15em;text-transform:uppercase;
  color:var(--muted);margin-bottom:2px}
.mv{font-family:var(--mono);font-size:.88rem;color:var(--text);line-height:1}
.mv.cy{color:var(--cyan)}.mv.gr{color:var(--good)}.mv.or{color:var(--warn)}
.mv.pe{color:var(--pen)}.mv.ng{color:var(--danger)}.mv.pu{color:var(--purple)}
.task-score-row{grid-column:1/-1;border-top:1.5px solid var(--bdr);
  padding-top:6px;margin-top:2px}
.task-score-detail{font-family:var(--mono);font-size:.6rem;color:var(--muted);
  margin-top:3px;line-height:1.4;word-break:break-all}
.lb-hist-btn{font-family:var(--mono);font-size:.65rem;padding:3px 8px;
  border:1.5px solid var(--bdr);border-radius:2px;background:none;
  color:var(--muted);cursor:pointer;transition:all .15s}
.lb-hist-btn:hover{color:var(--cyan);border-color:rgba(0,229,255,.4)}
.lb-hist-btn.active{color:var(--cyan);border-color:var(--cyan);
  background:rgba(0,229,255,.07)}
.bbar{grid-column:1/-1;height:3px;background:var(--bdr);border-radius:2px;
  margin-top:4px;overflow:hidden}
.bfil{height:100%;border-radius:2px;background:var(--good);transition:width .5s}
.bfil.w{background:var(--warn)}.bfil.c{background:var(--danger)}
.flight-list{display:flex;flex-wrap:wrap;gap:4px;padding:5px 0 2px}
.fl-item{font-family:var(--mono);font-size:.75rem;display:inline-flex;
  gap:3px;align-items:center;background:#f8f9fb;
  border:1.5px solid var(--bdr);border-radius:3px;padding:2px 5px}
.fl-item.fl-dq{opacity:.4}
.fl-n{color:var(--muted)}
.fl-raw{color:var(--text)}
.fl-adj{color:var(--good)}
.log-badge{font-family:var(--mono);font-size:.65rem;padding:3px 6px;
  border-radius:2px;cursor:default;white-space:nowrap}
.log-badge.none{color:var(--muted);border:1.5px solid var(--bdr)}
.log-badge.announced{color:var(--warn);border:2px solid #e65100;animation:pulse .9s ease-in-out infinite alternate}
.log-badge.fetching{color:var(--cyan);border:2px solid #1565c0;animation:pulse .9s ease-in-out infinite alternate}
.log-badge.complete{color:var(--good);border:2px solid #2e7d32}
.log-badge.error{color:var(--danger);border:2px solid #c62828}
.log-badge.queued{color:var(--warn);border:2px solid #e65100;animation:pulse .9s ease-in-out infinite alternate}
.log-badge.dnf{color:var(--danger);border:2px solid #c62828}
.log-cancel{font-family:var(--mono);font-size:.58rem;padding:2px 5px;border-radius:2px;
  background:none;border:1.5px solid var(--bdr);color:var(--muted);cursor:pointer;
  line-height:1;margin-left:3px}
.log-cancel:hover{border-color:var(--danger);color:var(--danger)}
.dbg-toggle{width:100%;background:none;border:none;border-top:1.5px solid var(--bdr);
  color:var(--muted);font-family:var(--mono);font-size:.6rem;letter-spacing:.12em;
  padding:4px 10px;text-align:left;cursor:pointer;display:flex;align-items:center;gap:6px}
.dbg-toggle:hover{color:var(--text)}
.dbg-toggle .dbg-arrow{transition:transform .2s;display:inline-block}
.dbg-toggle.open .dbg-arrow{transform:rotate(90deg)}
.dbg-panel{display:none;padding:8px 12px 10px;border-top:2px solid var(--bdr);
  background:#f0f4f8;grid-template-columns:1fr 1fr 1fr;gap:3px 8px}
.dbg-panel.open{display:grid}
.dbg-row{display:flex;flex-direction:column;gap:1px}
.dbg-label{font-family:var(--mono);font-size:.55rem;color:var(--muted);letter-spacing:.08em;text-transform:uppercase}
.dbg-val{font-family:var(--mono);font-size:.75rem;color:var(--text)}
.dbg-val.good{color:var(--good)}
.dbg-val.warn{color:var(--warn)}
.dbg-val.bad{color:var(--danger)}
.dbg-val.dim{color:var(--muted)}
.spark-wrap{grid-column:1/-1;margin-top:6px;position:relative;height:52px;
  background:#e8edf5;border-radius:3px;overflow:hidden}
.spark-wrap svg{width:100%;height:100%;display:block}
.spark-empty{position:absolute;inset:0;display:flex;align-items:center;
  justify-content:center;font-family:var(--mono);font-size:.6rem;
  color:rgba(255,255,255,.12);letter-spacing:.1em;pointer-events:none}

/* ── Leaderboard ── */
#lb{background:var(--surf);border:1.5px solid var(--bdr);border-radius:4px;
  overflow:hidden;margin-bottom:28px}
.lbh,.lbr{display:grid;
  grid-template-columns:36px 52px 1fr 90px 90px 90px 90px 60px;
  padding:8px 14px;align-items:center;gap:0}
.lbh{background:#dde3ec;border-bottom:2px solid var(--bdr);font-size:.76rem;font-weight:800;
  letter-spacing:.18em;text-transform:uppercase;color:var(--muted)}
.lbr{border-bottom:1.5px solid var(--bdr);transition:background .15s}
.lbr:last-child{border-bottom:none}
.lbr:hover{background:rgba(21,101,192,.05)}
.rk{font-family:var(--mono);font-size:.7rem;color:var(--muted)}
.r1{color:#ffd700;font-weight:700}.r2{color:#c0c0c0;font-weight:700}.r3{color:#cd7f32;font-weight:700}
.lid{font-size:1rem;font-weight:700}
.lname{font-size:1rem;font-weight:800;color:var(--text)}
.lts{font-family:var(--mono);font-size:1rem;color:var(--purple);font-weight:700}
.lts.ng{color:var(--danger)}
.lr{font-family:var(--mono);font-size:.8rem;color:var(--text)}
.lp{font-family:var(--mono);font-size:.8rem;color:var(--pen)}
.ll{font-family:var(--mono);font-size:.78rem;color:var(--muted)}
.lf{font-family:var(--mono);font-size:.72rem;color:var(--muted)}
.nodata{text-align:center;padding:36px;color:var(--muted);
  font-size:.88rem;letter-spacing:.15em}
/* Flight log expansion */
.lbr{cursor:pointer}
.lbr.expanded{background:rgba(0,229,255,.03)}
.flight-log{display:none;grid-column:1/-1;padding:6px 14px 10px;
  border-top:1.5px solid var(--bdr);background:#f8f9fb}
.flight-log.open{display:block}
.fl-table{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:.82rem}
.fl-table th{text-align:left;color:var(--muted);font-weight:800;letter-spacing:.12em;
  padding:3px 8px 5px;border-bottom:1.5px solid var(--bdr)}
.fl-table td{padding:3px 8px;color:var(--text);border-bottom:1.5px solid rgba(30,37,48,.5)}
.fl-table tr:last-child td{border-bottom:none}
.fl-table tr:hover td{background:rgba(255,255,255,.02)}
.fl-dq{color:var(--danger)}
.fl-adj-pos{color:var(--good)}
.fl-adj-neg{color:var(--danger)}
.fl-best-row{background:rgba(0,230,118,.05)}
.fl-best{color:var(--cyan);font-weight:700}

/* ── Settings ── */
.settings-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px}
@media(max-width:700px){.settings-grid{grid-template-columns:1fr}}
.settings-box{background:var(--surf);border:1.5px solid var(--bdr);
  border-radius:4px;padding:18px 20px}
.settings-box h3{font-size:.7rem;font-weight:800;letter-spacing:.2em;
  text-transform:uppercase;color:var(--muted);margin-bottom:14px}
.pilot-row{display:grid;grid-template-columns:40px 1fr;gap:8px;
  align-items:center;margin-bottom:8px}
.pilot-unit{font-family:var(--mono);font-size:.85rem;color:var(--muted);text-align:right}
.pilot-input{background:#f8f9fb;border:1.5px solid var(--bdr);border-radius:3px;
  padding:5px 9px;font-family:var(--disp);font-size:.85rem;color:var(--text);
  width:100%;outline:none;transition:border-color .2s}
.pilot-input:focus{border-color:var(--cyan)}
.task-select-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.task-btn{background:#f8f9fb;border:1.5px solid var(--bdr);border-radius:3px;
  padding:8px 10px;cursor:pointer;transition:border-color .2s,background .2s;
  display:flex;flex-direction:column;gap:2px}
.task-btn:hover{border-color:var(--purple)}
.task-btn.selected{border-color:var(--purple);background:rgba(180,142,255,.08)}
.task-btn-id{font-family:var(--mono);font-size:.75rem;color:var(--purple)}
.task-btn-name{font-size:.82rem;font-weight:800;color:var(--text)}
.task-btn-desc{font-size:.7rem;color:var(--muted)}
.cfg-row{display:flex;align-items:center;gap:10px;margin-top:10px;
  padding:10px;background:#f8f9fb;border:1.5px solid var(--bdr);border-radius:3px}
.cfg-label{font-size:.72rem;color:var(--muted);letter-spacing:.1em;flex:1}
.cfg-select{background:#f8f9fb;border:1.5px solid var(--bdr);
  color:var(--text);padding:4px 8px;border-radius:3px;
  font-family:var(--mono);font-size:.8rem}
.btn{background:var(--cyan);color:#fff;border:none;border-radius:4px;
  padding:10px 22px;font-family:var(--disp);font-size:.85rem;font-weight:700;
  letter-spacing:.1em;text-transform:uppercase;cursor:pointer;
  transition:opacity .2s;margin-top:10px}
.btn:hover{opacity:.85}
.btn.danger{background:var(--danger);color:#fff}
.save-status{font-family:var(--mono);font-size:.72rem;
  color:var(--good);margin-left:10px;opacity:0;transition:opacity .3s}
.save-status.show{opacity:1}

/* ── Roster tab ── */
.roster-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px}
@media(max-width:760px){.roster-grid{grid-template-columns:1fr}}
.pilot-list{list-style:none;margin:0;padding:0;max-height:500px;overflow-y:auto}
.pilot-item{display:grid;grid-template-columns:32px 1fr auto 36px 28px;
  align-items:center;gap:6px;
  padding:5px 8px;border-bottom:1.5px solid var(--bdr);
  font-family:var(--disp);font-size:.88rem;cursor:pointer;
  transition:background .1s}
.pilot-item:hover{background:rgba(255,255,255,.03)}
.pilot-item.checked-in{background:rgba(0,230,118,.04)}
.ci-toggle{width:20px;height:20px;border-radius:50%;border:2px solid var(--bdr);
  display:flex;align-items:center;justify-content:center;
  font-size:.7rem;flex-shrink:0;transition:all .15s;cursor:pointer}
.ci-toggle.on{border-color:var(--good);background:var(--good);color:#000}
.ci-toggle.off{border-color:var(--bdr);color:var(--muted)}
.pname{color:var(--muted);transition:color .15s}
.pilot-item.checked-in .pname{color:var(--text);font-weight:800}
.unit-input{font-family:var(--mono);font-size:.72rem;font-weight:700;
  color:var(--cyan);background:#f8f9fb;border:1.5px solid var(--bdr);
  border-radius:3px;width:34px;text-align:center;padding:1px 3px;
  outline:none;-moz-appearance:textfield}
.unit-input::-webkit-outer-spin-button,.unit-input::-webkit-inner-spin-button{-webkit-appearance:none}
.unit-input:focus{border-color:var(--cyan)}
.unit-badge{font-family:var(--mono);font-size:.68rem;font-weight:700;
  color:var(--cyan);background:#f8f9fb;border:1.5px solid var(--bdr);
  border-radius:3px;padding:1px 6px;align-self:center;justify-self:end;
  margin-right:4px}
.pilot-del{background:none;border:none;color:transparent;cursor:pointer;
  font-size:.85rem;padding:0;line-height:1;transition:color .15s}
.pilot-item:hover .pilot-del{color:var(--muted)}
.pilot-del:hover{color:var(--danger) !important}
.checkin-stats{font-family:var(--mono);font-size:.72rem;color:var(--muted);
  padding:6px 0;display:flex;justify-content:space-between;align-items:center}
.checkin-stats strong{color:var(--good)}
.add-row{display:flex;gap:8px;margin-top:10px}
.add-input{flex:1;background:#f8f9fb;border:1.5px solid var(--bdr);
  border-radius:3px;padding:6px 10px;font-family:var(--disp);
  font-size:.85rem;color:var(--text);outline:none}
.add-input:focus{border-color:var(--cyan)}
.add-btn{background:var(--cyan);color:#000;border:none;border-radius:3px;
  padding:6px 14px;font-family:var(--disp);font-size:.85rem;font-weight:700;
  cursor:pointer;white-space:nowrap}
/* Draw panel */





.draw-result{border:1.5px solid var(--bdr);border-radius:4px;overflow:hidden;
  margin-bottom:12px}
.draw-row{display:grid;grid-template-columns:44px 1fr;
  padding:6px 12px;border-bottom:1.5px solid var(--bdr);align-items:center}
.draw-row:last-child{border-bottom:none}
.draw-unit{font-family:var(--mono);font-size:.9rem;font-weight:700;color:var(--cyan)}
.draw-name{font-size:.9rem;font-weight:800;color:var(--text)}







.contest-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px}
@media(max-width:760px){.contest-grid{grid-template-columns:1fr}}
.round-table{width:100%;border-collapse:collapse;margin-top:12px;font-size:1rem}
.round-table th{font-family:var(--mono);font-size:.78rem;font-weight:800;
  letter-spacing:.1em;color:var(--text);text-align:left;
  padding:4px 10px 6px;border-bottom:1.5px solid var(--bdr)}
.round-table td{padding:10px 10px;border-bottom:2px solid var(--bdr);
  font-size:1rem;font-weight:700}
.round-table tr:last-child td{border-bottom:none}
.round-table tr.active-row td{background:rgba(21,101,192,.08);border-left:4px solid var(--cyan)}
.round-table tr.done-row td{opacity:.8;color:var(--muted)}
.rtask-sel{background:#ffffff;border:2px solid var(--bdr);color:var(--text);
  font-family:var(--mono);font-size:.9rem;padding:2px 6px;border-radius:3px}
.standings-table{width:100%;border-collapse:collapse;margin-top:10px}
.standings-table th{font-family:var(--mono);font-size:.6rem;font-weight:700;
  letter-spacing:.15em;color:var(--muted);text-align:right;padding:4px 8px 6px;
  border-bottom:1.5px solid var(--bdr)}
.standings-table th:nth-child(1),.standings-table th:nth-child(2)
  {text-align:left}
.standings-table td{padding:5px 8px;border-bottom:1.5px solid rgba(33,38,45,.6);
  font-family:var(--mono);font-size:.78rem;text-align:right}
.standings-table td:nth-child(1),.standings-table td:nth-child(2)
  {text-align:left}
.standings-table tr.dropped-col td{color:var(--muted)}
.s-total{color:var(--cyan);font-weight:700}
.s-drop{color:var(--muted);text-decoration:line-through}
.s-active{color:var(--warn)}
.contest-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
.drop-mode-btn{font-size:.68rem;padding:3px 10px;background:none;
  border:1.5px solid var(--bdr);color:var(--muted)}
.drop-mode-btn.active{border-color:var(--cyan);color:var(--cyan);
  background:rgba(0,229,255,.07)}
.drop-mode-btn.active.force{border-color:var(--warn);color:var(--warn);
  background:rgba(255,179,0,.06)}
.drop-mode-btn.active.none{border-color:var(--muted);color:var(--text);
  background:rgba(255,255,255,.04)}
footer{padding:10px 24px;border-top:1.5px solid var(--bdr);
  display:flex;justify-content:space-between;
  font-family:var(--mono);font-size:.66rem;color:var(--muted)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.6}}
</style>
</head>
<body>

<header>
  <div class="logo">
    <div class="logo-f3k">F3K</div>
    <div class="logo-sub" id="sname">Scoring System</div>
  </div>
  <div class="hdr-r">
    <div class="pill"><span id="cdot"></span><span id="clbl">CONNECTING</span></div>
    <div class="pill">TASK <span id="htask">—</span></div>
    <div class="pill">UNITS <span id="ucnt">0</span></div>
    <div class="pill">PKTS <span id="pcnt">0</span></div>
    <div class="pill" id="clk">--:--:--</div>
    <div class="spec-links">
      <a class="spec-link" href="/pilots"   target="_blank">📡 Live</a>
      <a class="spec-link" href="/standings"   target="_blank">🏅 Standings</a>
      <a class="spec-link" href="/draws"    target="_blank">📋 Draws</a>
    </div>
  </div>
</header>

<div class="tabs">
  <div class="tab active" onclick="showTab('dashboard')">Dashboard</div>
  <div class="tab" onclick="showTab('leaderboard')">Leaderboard</div>
  <div class="tab" onclick="showTab('standings')">📊 Standings</div>
  <div class="tab-sep"></div>
  <div class="tab" onclick="showTab('roster')">👥 Roster</div>
  <div class="tab" onclick="showTab('contest')">🏆 Contest</div>
  <div class="tab" onclick="showTab('draws')">📋 Draws</div>
</div>

<!-- ── Penalty banner ── -->
<div class="banner">
  <span>LAUNCH PENALTY</span>
  <span>adjusted = raw − (launch_ft × <span id="kval">?</span>s)</span>
</div>

<!-- ── Window / task bar ── -->
<div class="window-bar">
  <div>
    <div class="win-task" id="win-task-label">—</div>
    <div class="win-desc" id="win-task-desc"></div>
  </div>
  <div class="win-rg" id="win-rg-label" style="display:none"></div>
  <div class="paused-banner" id="paused-banner">⏸  PAUSED — MIKE SMITH RULE</div>
  <div class="win-spacer"></div>
  <div class="win-timer-wrap">
    <div class="win-label" id="win-label">WINDOW</div>
    <div class="win-progress" id="win-prog-wrap" style="display:none">
      <div class="win-fill" id="win-fill"></div>
    </div>
    <div class="win-timer" id="win-timer">—</div>
    <button class="win-btn" id="win-btn" onclick="toggleWindow()">START</button>
    <button class="win-btn pause" id="pause-btn"
      onclick="togglePause()" style="display:none">⏸</button>

  </div>
</div>

<!-- ── Dashboard ── -->
<div id="tab-dashboard" class="panel active">
  <div class="sec" style="margin-top:16px">Live Units</div>
  <div id="grid"><div class="nodata" style="grid-column:1/-1">WAITING FOR FLIGHT UNITS</div></div>
</div>

<!-- ── Leaderboard ── -->
<div id="tab-leaderboard" class="panel">
  <!-- Round/group history selector -->
  <div style="margin-top:12px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
    <span style="font-family:var(--mono);font-size:.65rem;color:var(--muted);
      letter-spacing:.1em">VIEW:</span>
    <button class="btn" id="lb-live-btn" onclick="lbSelectLive()"
      style="font-size:.68rem;padding:3px 10px;background:var(--cyan);color:#000">
      ▶ Live Group</button>
    <div id="lb-hist-btns" style="display:flex;gap:5px;flex-wrap:wrap"></div>
  </div>
  <div class="sec" id="lb-heading" style="margin-top:10px">
    Live Group — Task Score (Normalised)
  </div>
  <div id="lb"><div class="nodata">NO COMPLETED FLIGHTS YET</div></div>
</div>

<!-- ── Roster ── -->
<div id="tab-roster" class="panel">
  <div style="margin-top:16px">
    <div class="roster-grid">

      <!-- Check-in + Master Roster -->
      <div class="settings-box">
        <h3>Check-In — <span id="roster-count">0</span> pilots registered</h3>
        <div class="checkin-stats">
          <span><strong id="checkin-count">0</strong> checked in</span>
          <span>
            <button class="btn secondary" style="padding:3px 10px;margin-top:0;font-size:.7rem"
              onclick="checkInAll()">✓ All In</button>
            &nbsp;
            <button class="btn secondary" style="padding:3px 10px;margin-top:0;font-size:.7rem"
              onclick="checkInNone()">✗ Clear</button>
          </span>
        </div>
        <ul class="pilot-list" id="pilot-list"></ul>
        <div class="add-row" style="margin-top:10px">
          <input class="add-input" id="add-name" placeholder="Add pilot name…"
            onkeydown="if(event.key==='Enter')addPilot()">
          <button class="add-btn" onclick="addPilot()">+ Add</button>
        </div>
        <span class="save-status" id="roster-status" style="margin-top:6px;display:block">Saved!</span>
      </div>

    </div>


  </div>
</div>

<!-- ── Contest ── -->
<div id="tab-contest" class="panel">
  <div style="margin-top:16px">
    <div class="contest-grid">

      <!-- Contest Plan -->
      <div class="settings-box">
        <h3>Contest Plan</h3>
        <div class="rs-row" style="margin-bottom:8px">
          <span class="rs-label">Name</span>
          <input class="rs-input" id="c-name" style="width:160px"
            placeholder="SoCal F3K" value="F3K Contest">
        </div>
        <div class="rs-row" style="margin-bottom:8px">
          <span class="rs-label">Date</span>
          <input class="rs-input" id="c-date" type="date" style="width:140px">
        </div>
        <div class="rs-row" style="margin-bottom:12px">
          <span class="rs-label">Units</span>
          <input class="rs-input" id="c-units" type="number"
            min="1" max="20" value="8" style="width:55px">
          <span class="rs-unit">max pilots/group</span>
        </div>

        <h3 style="margin-top:4px">Round Tasks</h3>
        <table class="round-table" id="round-plan-table">
          <thead><tr>
            <th>Round</th><th>Task</th><th>Status</th><th></th>
          </tr></thead>
          <tbody id="round-plan-body"></tbody>
        </table>
        <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap">
          <button class="btn secondary" onclick="addRound()">+ Round</button>
          <button class="btn secondary" onclick="removeLastRound()">− Round</button>
        </div>
        <!-- Resume banner — shown when interrupted round detected -->
        <div id="resume-banner" style="display:none;margin-bottom:10px;padding:10px 14px;
          background:rgba(255,179,0,.08);border:1.5px solid rgba(255,179,0,.3);
          border-radius:4px;font-size:.8rem">
          <div style="color:var(--warn);font-weight:700;margin-bottom:6px">
            ⚠ Contest interrupted — Round <span id="resume-round-lbl"></span> was not completed
          </div>
          <div style="color:var(--muted);margin-bottom:8px">
            Select which group to resume from, or mark groups as skipped:
          </div>
          <div id="resume-group-btns" style="display:flex;flex-wrap:wrap;gap:6px"></div>
        </div>

        <div class="contest-actions">
          <button id="rescore-all-btn" class="btn" style="background:none;border:2px solid var(--cyan);color:var(--cyan)" onclick="rescoreAll()">↺ Rescore All Rounds</button>
          <button class="btn" onclick="drawAllGroups()">🎲 Draw All Groups</button>
          <button class="btn" style="background:var(--good);color:#000"
            onclick="startContest()">▶ Start Contest</button>
          <span class="save-status" id="contest-status"></span>
        </div>

        <!-- F3XVault integration -->
        <div style="margin-top:12px;padding:10px 12px;background:#f0f4f8;
          border:1.5px solid var(--bdr);border-radius:4px">
          <div style="font-family:var(--mono);font-size:.65rem;font-weight:800;
            letter-spacing:.12em;color:var(--muted);margin-bottom:8px">
            F3XVAULT
            <span id="vault-status-badge" style="margin-left:8px;padding:2px 6px;
              border-radius:2px;font-size:.6rem"></span>
          </div>
          <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
            <button class="btn" id="vault-pull-btn"
              style="background:none;border:2px solid var(--purple);color:var(--purple);
                     font-size:.75rem;padding:6px 14px"
              onclick="vaultPull()">↓ Pull from F3XVault</button>
            <button class="btn" id="vault-push-all-btn"
              style="background:none;border:2px solid var(--good);color:var(--good);
                     font-size:.75rem;padding:6px 14px;opacity:.4;pointer-events:none"
              onclick="vaultPushAll()">↑ Push All Rounds</button>
            <span id="vault-msg" style="font-family:var(--mono);font-size:.65rem;
              color:var(--muted)"></span>
          </div>
          <div id="vault-match-detail" style="margin-top:8px;display:none;
            font-family:var(--mono);font-size:.65rem;line-height:1.6"></div>
        </div>
        <div style="margin-top:8px">
          <button class="btn" style="background:none;border:1.5px solid var(--bdr);
            color:var(--muted);font-size:.72rem" onclick="archiveContest()">
            📁 Archive &amp; New Contest</button>
        </div>
        <div style="margin-top:14px;padding-top:12px;border-top:2px solid var(--bdr);
          display:flex;align-items:center;gap:10px;flex-wrap:wrap">
          <span style="font-family:var(--mono);font-size:.65rem;color:var(--muted);
            letter-spacing:.1em">DROP ROUND:</span>
          <div style="display:flex;gap:6px" id="drop-mode-btns">
            <button class="btn drop-mode-btn" id="drop-btn-auto"
              onclick="setDropMode('auto')"
              title="FAI rule: drop 1 after 5+ rounds, drop 2 after 9+">
              Auto (FAI)</button>
            <button class="btn drop-mode-btn" id="drop-btn-force"
              onclick="setDropMode('force')"
              title="Force: always drop 1 worst round regardless of count">
              Force Drop</button>
            <button class="btn drop-mode-btn" id="drop-btn-none"
              onclick="setDropMode('none')"
              title="No drops applied to any pilot">
              No Drop</button>
          </div>
          <span id="drop-mode-status" style="font-family:var(--mono);
            font-size:.65rem;color:var(--muted)"></span>
        </div>
      </div>

    </div>
  </div>
</div>

<!-- ── Standings ── -->
<div id="tab-standings" class="panel">
  <div style="margin-top:12px;margin-bottom:6px;display:flex;align-items:center;gap:8px" id="drop-indicator-row"><span style="font-family:var(--mono);font-size:.65rem;color:var(--muted);letter-spacing:.08em" id="drop-indicator"></span></div>
<div style="margin-top:4px;overflow-x:auto" id="standings-wrap">
    <div class="nodata" style="padding:40px">No completed rounds yet</div>
  </div>
</div>

<!-- ── Draws ── -->
<div id="tab-draws" class="panel">
  <div style="margin-top:16px" id="draws-content">
    <div class="nodata">Loading draws...</div>
  </div>
</div>

<!-- ── Settings ── -->


<footer>
  <span>F3K SCORING SERVER v3.1</span>
  <span id="upt">UPTIME 0s</span>
</footer>

<script>
// ── State ──────────────────────────────────────────────────────
let state       = null;
let selTask     = 'LL';
let _cardsHash  = '';
let winRunning  = false;
let winTotal    = 0;
let winRemain   = 0;
let fails       = 0;
const S_GROUND=0,S_LAUNCH=1,S_FLIGHT=2,S_LANDED=3;

// ── Tab switching ──────────────────────────────────────────────
function showTab(id) {
  document.querySelectorAll('.tab').forEach((t,i)=>{
    const ids=['dashboard','leaderboard','standings','roster','contest','draws'];
    t.classList.toggle('active', ids[i]===id);
  });
  document.querySelectorAll('.panel').forEach(p=>{
    p.classList.toggle('active', p.id==='tab-'+id);
  });
  // Force draws tab to refresh when switched to
  if(id==='draws' && state){
    _drawsHash=''; _fullRounds=[];
    renderDrawsTab(state.contest ? state : {contest:{}});
  }
}

// ── Polling ────────────────────────────────────────────────────
async function poll() {
  try {
    const d = await(await fetch('/state')).json();
    fails=0; state=d; render(d);
  } catch(e){ if(++fails>3) setOffline(); }
}
function setOffline(){
  document.getElementById('cdot').className='';
  document.getElementById('clbl').textContent='OFFLINE';
}
setInterval(poll,200); poll();

// ── Main render ────────────────────────────────────────────────
function render(d) {
  document.getElementById('cdot').className='live';
  document.getElementById('clbl').textContent='LIVE';
  document.getElementById('pcnt').textContent=d.packet_count;
  document.getElementById('upt').textContent=`UPTIME ${d.uptime_s}s`;
  document.getElementById('sname').textContent=d.session_name;
  document.getElementById('kval').textContent=d.penalty_per_foot;
  document.getElementById('htask').textContent=d.active_task;
  document.getElementById('ucnt').textContent=d.units.filter(u=>!u.stale).length;

  const ti = d.task_info||{};
  document.getElementById('win-task-label').textContent=
    ti.name ? `${ti.name} — ${ti.desc}` : '—';
  document.getElementById('win-task-desc').textContent='';

  renderWindowBar(d);
  const newCardsHash = JSON.stringify(d.units.map(u=>({
    id:u.id,state:u.state,alt:u.altitude_ft,dur:u.duration_s,
    raw:u.raw_time_s,adj:u.adjusted_score_s,norm:u.normalised_score,
    fc:u.flight_count,stale:u.stale,pilot:u.pilot_name
  })));
  if(newCardsHash !== _cardsHash){
    // Snapshot open debug panels before rebuild
    const openDbg = new Set();
    document.querySelectorAll('.dbg-toggle.open').forEach(t=>{
      openDbg.add(parseInt(t.id.replace('dbgtog-','')));
    });
    _cardsHash = newCardsHash;
    renderCards(d.units, d.active_task);
    // Restore open debug panels
    openDbg.forEach(uid=>{
      const tog = document.getElementById('dbgtog-'+uid);
      const pan = document.getElementById('dbgpanel-'+uid);
      if(tog && pan){ tog.classList.add('open'); pan.classList.add('open'); }
    });
  }
  // Refresh data in any open debug panels
  document.querySelectorAll('.dbg-toggle.open').forEach(tog=>{
    const uid = parseInt(tog.id.replace('dbgtog-',''));
    if(!isNaN(uid)) updateDbgPanel(uid);
  });
  renderSparklines(d.units, d.win_total, d.win_phase);
  if(!_lbHistMode) renderLB(d.units, d.active_task);
  updateLBHistButtons(d.contest);
  renderRoster(d);
  renderContest(d);
  _updateVaultUI(d);
  syncInlineControls(d);
  // Draws tab updated separately via drawsTabUpdate()
  drawsTabUpdate(d);
}

// ── Window bar ─────────────────────────────────────────────────
function fmtTime(s){
  if(s===null||s===undefined) return '—';
  const m=Math.floor(s/60), ss=Math.floor(s%60);
  return `${m}:${String(ss).padStart(2,'0')}`;
}

function renderWindowBar(d){
  const timer   = document.getElementById('win-timer');
  const fill    = document.getElementById('win-fill');
  const progWrap= document.getElementById('win-prog-wrap');
  const btn     = document.getElementById('win-btn');
  const label   = document.getElementById('win-label');

  winRunning = d.win_running;
  winTotal   = d.win_total   || 0;
  winRemain  = d.win_remaining;

  // Round / group indicator
  const rgEl = document.getElementById('win-rg-label');
  if(rgEl){
    const ct = d.contest;
    const ri = ct?.active_round;
    const gi = ct?.active_group;
    const rnds = ct?.rounds||[];
    if(ri!=null && gi!=null && rnds[ri]){
      const rnd   = rnds[ri];
      const nGrps = rnd.n_groups||'?';
      rgEl.textContent = `Round ${rnd.round_num}, Group ${gi+1} of ${nGrps}`;
      rgEl.style.display = '';
    } else {
      rgEl.style.display = 'none';
    }
  }

  const phase  = d.win_phase || 'READY';
  const tc     = d.taskc;
  const isPaused = d.paused || false;

  // Pause button — show during pauseable phases, hide during landing/closed
  const pauseBtn    = document.getElementById('pause-btn');
  const pauseBanner = document.getElementById('paused-banner');
  const pauseablePhases = ['PREP','TESTING','NO_FLY'];
  if(pauseBtn){
    if(pauseablePhases.includes(phase)){
      pauseBtn.style.display = '';
      pauseBtn.textContent   = isPaused ? '▶ RESUME' : '⏸';
      pauseBtn.className     = isPaused ? 'win-btn resume' : 'win-btn pause';
    } else {
      pauseBtn.style.display = 'none';
    }
  }
  if(pauseBanner){
    pauseBanner.style.display = isPaused ? '' : 'none';
  }

  // If paused: show frozen timer with blink, no further updates needed
  if(isPaused){
    timer.className = 'win-timer paused';
    btn.style.display = 'none';   // hide start/stop while paused
    return;
  }
  btn.style.display = '';

  // Task C: show attempt context in the task label
  if(tc){
    const ph = tc.phase;
    let taskLabelExtra = '';
    if(ph === 'FLYING')      taskLabelExtra = ` — ATTEMPT ${tc.attempt}/${tc.total} · FLYING`;
    else if(ph==='LANDING_WIN') taskLabelExtra = ` — ATTEMPT ${tc.attempt}/${tc.total} · LAND NOW`;
    else if(ph==='PREP')     taskLabelExtra = ` — ATTEMPT ${tc.attempt}/${tc.total} · PREP`;
    else if(ph==='DONE')     taskLabelExtra = ' — ALL ATTEMPTS COMPLETE';
    document.getElementById('win-task-label').textContent =
      (d.task_info?.name||'—') + ' — ' + (d.task_info?.desc||'') + taskLabelExtra;
  }

  progWrap.style.display = '';
  btn.style.display      = '';

  // READY (not started)
  if(phase === 'READY'){
    label.textContent = tc ? 'ATTEMPT 1' : 'WINDOW';
    timer.textContent = fmtTime(winRemain);
    timer.className   = 'win-timer';
    fill.className    = 'win-fill'; fill.style.width = '100%';
    btn.textContent   = 'START'; btn.className = 'win-btn';
    return;
  }

  // PREP — preparation time
  if(phase === 'PREP'){
    label.textContent = 'PREP TIME';
    const pct = winTotal>0 ? Math.max(0, winRemain/winTotal*100) : 100;
    fill.style.width  = pct+'%';
    timer.textContent = fmtTime(winRemain);
    timer.className   = 'win-timer'; fill.className='win-fill';
    btn.innerHTML     = 'STOP &nbsp;<span style="font-size:.6rem;opacity:.6">SKIP→</span>';
    btn.className     = 'win-btn stop';
    btn.onclick       = ()=>skipPrep();
    return;
  }

  // TESTING — flight testing window
  if(phase === 'TESTING'){
    label.textContent = 'TEST FLIGHTS';
    const pct = winTotal>0 ? Math.max(0, winRemain/winTotal*100) : 100;
    fill.style.width  = pct+'%';
    timer.textContent = fmtTime(winRemain);
    timer.className   = 'win-timer warn'; fill.className='win-fill warn';
    btn.innerHTML     = 'SKIP→';
    btn.className     = 'win-btn skip';
    btn.onclick       = ()=>skipPrep();
    return;
  }

  // NO_FLY — mandatory no-fly before window
  if(phase === 'NO_FLY'){
    label.textContent = 'NO FLY';
    const pct = winTotal>0 ? Math.max(0, winRemain/winTotal*100) : 100;
    fill.style.width  = pct+'%';
    timer.textContent = fmtTime(winRemain);
    timer.className   = winRemain<=10 ? 'win-timer crit' : 'win-timer warn';
    fill.className    = winRemain<=10 ? 'win-fill crit' : 'win-fill warn';
    btn.innerHTML     = 'SKIP→';
    btn.className     = 'win-btn skip';
    btn.onclick       = ()=>skipPrep();
    return;
  }

  // RUNNING — main working window (or Task C flight period)
  if(phase === 'RUNNING'){
    btn.onclick = ()=>toggleWindow();   // restore normal onclick
    label.textContent = tc ? `ATTEMPT ${tc?.attempt||''}/${tc?.total||''}` : 'WINDOW';
    const pct = winTotal>0 ? Math.max(0, winRemain/winTotal*100) : 0;
    fill.style.width  = pct+'%';
    timer.textContent = fmtTime(winRemain);
    if(winRemain<=30){
      timer.className='win-timer crit'; fill.className='win-fill crit';
    } else if(winRemain<=60){
      timer.className='win-timer warn'; fill.className='win-fill warn';
    } else {
      timer.className='win-timer'; fill.className='win-fill';
    }
    btn.textContent='STOP'; btn.className='win-btn stop';
    return;
  }

  // LANDING_WIN — 30s to get down
  if(phase === 'LANDING_WIN'){
    label.textContent = 'LAND NOW';
    const pct = Math.max(0, winRemain/30*100);
    fill.style.width  = pct+'%';
    timer.textContent = fmtTime(winRemain);
    timer.className   = 'win-timer crit';
    fill.className    = 'win-fill crit';
    btn.textContent   = 'STOP'; btn.className='win-btn stop';
    return;
  }

  // PREP — 60s no-fly between Task C attempts
  if(phase === 'PREP'){
    label.textContent = 'PREP';
    const pct = Math.max(0, winRemain/60*100);
    fill.style.width  = pct+'%';
    timer.textContent = fmtTime(winRemain);
    timer.className   = 'win-timer'; fill.className='win-fill';
    btn.textContent   = 'STOP'; btn.className='win-btn stop';
    return;
  }

  // CLOSED or DONE
  label.textContent = phase==='DONE' ? 'COMPLETE' : 'CLOSED';
  timer.textContent = phase==='DONE' ? 'DONE' : fmtTime(0);
  timer.className   = 'win-timer done';
  fill.className    = 'win-fill'; fill.style.width='0%';
  btn.textContent   = 'RESET'; btn.className='win-btn';
}

// ── Debug dropdown ───────────────────────────────────────────────
function toggleDbg(uid){
  const tog = document.getElementById("dbgtog-"+uid);
  const pan = document.getElementById("dbgpanel-"+uid);
  if(!tog||!pan) return;
  const open = tog.classList.toggle("open");
  pan.classList.toggle("open", open);
  if(open) updateDbgPanel(uid);
}

function updateDbgPanel(uid){
  const pan = document.getElementById("dbgpanel-"+uid);
  if(!pan || !pan.classList.contains("open")) return;
  const dbg = state?.debug?.[String(uid)];
  if(!dbg){
    pan.innerHTML = '<div style="grid-column:1/-1;font-family:var(--mono);font-size:.65rem;'
      + 'color:var(--muted)">No debug data received yet</div>';
    return;
  }
  const rssi = dbg.rssi_dbm;
  const rssiCls = rssi >= -60 ? "good" : rssi >= -75 ? "warn" : "bad";
  const rssiBar = rssi >= -60 ? "▂▄▆█" : rssi >= -75 ? "▂▄▆░" : rssi >= -85 ? "▂▄░░" : "▂░░░";
  const cpu = dbg.cpu_pct;
  const cpuCls = cpu < 50 ? "good" : cpu < 80 ? "warn" : "bad";
  const heap = dbg.heap_kb;
  const heapCls = heap > 100 ? "good" : heap > 50 ? "warn" : "bad";
  const temp = dbg.temp_c;
  const age = dbg.timestamp ? Math.round(Date.now()/1000 - dbg.timestamp) : null;
  const ageCls = age !== null && age > 5 ? "warn" : "dim";
  pan.innerHTML =
    `<div class="dbg-row"><span class="dbg-label">RSSI</span>
      <span class="dbg-val ${rssiCls}">${rssiBar} ${rssi} dBm</span></div>
    <div class="dbg-row"><span class="dbg-label">CPU</span>
      <span class="dbg-val ${cpuCls}">${cpu}%</span></div>
    <div class="dbg-row"><span class="dbg-label">Free Heap</span>
      <span class="dbg-val ${heapCls}">${heap} kB</span></div>
    <div class="dbg-row"><span class="dbg-label">Loop Avg</span>
      <span class="dbg-val">${dbg.loop_avg_us} µs</span></div>
    <div class="dbg-row"><span class="dbg-label">Loop Max</span>
      <span class="dbg-val ${dbg.loop_max_us > 10000 ? "warn" : ""}">${dbg.loop_max_us} µs</span></div>
    <div class="dbg-row"><span class="dbg-label">Temp</span>
      <span class="dbg-val">${temp.toFixed(1)} °C</span></div>
    <div class="dbg-row" style="grid-column:1/-1"><span class="dbg-label">Last Pkt</span>
      <span class="dbg-val ${ageCls}">${age !== null ? age+"s ago" : "—"}</span></div>`;
}

// ── Altitude sparklines ──────────────────────────────────────────
function renderSparklines(units, winTotal, winPhase){
  const W = 300, H = 52;
  const PAD_TOP = 4, PAD_BOT = 4;
  const plotH = H - PAD_TOP - PAD_BOT;
  let globalPeak = 0;
  units.forEach(u=>{
    if(u.alt_history && u.alt_history.length)
      globalPeak = Math.max(globalPeak, ...u.alt_history.map(p=>p[1]));
    if(u.peak_alt_ft > globalPeak) globalPeak = u.peak_alt_ft;
  });
  const yMax = Math.max(globalPeak * 1.15, 20);
  const xMax = winTotal > 0 ? winTotal : 600;
  units.forEach(u=>{
    const svg   = document.getElementById("sparksvg-"   + u.id);
    const empty = document.getElementById("spark-empty-" + u.id);
    if(!svg) return;
    const hist = u.alt_history || [];
    if(!hist.length){
      svg.innerHTML = "";
      if(empty) empty.style.display = (winPhase==="RUNNING") ? "none" : "";
      return;
    }
    if(empty) empty.style.display = "none";
    const toX = t  => ((t  / xMax) * W).toFixed(1);
    const toY = ft => (H - PAD_BOT - (ft / yMax) * plotH).toFixed(1);
    const pts = hist.map(p => toX(p[0]) + "," + toY(p[1])).join(" ");
    let launchSVG = "";
    for(let i=1; i<hist.length; i++){
      if(hist[i-1][1] < 3 && hist[i][1] > 8){
        const lx = toX(hist[i][0]);
        launchSVG += "<line x1=\""+lx+"\" y1=\""+PAD_TOP+"\" x2=\""+lx+"\" y2=\""+(H-PAD_BOT)+
          "\" stroke=\"rgba(255,160,0,.55)\" stroke-width=\"1\"/>";
      }
    }
    const gy = toY(0);
    const lx = toX(hist[hist.length-1][0]);
    const ly = toY(hist[hist.length-1][1]);
    svg.innerHTML =
      "<line x1=\"0\" y1=\""+gy+"\" x2=\""+W+"\" y2=\""+gy+
        "\" stroke=\"rgba(255,255,255,.06)\" stroke-width=\"1\"/>"+
      launchSVG+
      "<polyline points=\""+pts+"\" fill=\"none\" stroke=\"var(--cyan)\""+
        " stroke-width=\"1.5\" stroke-linejoin=\"round\" stroke-linecap=\"round\" opacity=\"0.85\"/>"+
      "<circle cx=\""+lx+"\" cy=\""+ly+"\" r=\"2\" fill=\"var(--cyan)\" opacity=\"0.9\"/>";
  });
}

async function toggleWindow(){
  if(winRunning){
    await fetch('/api/window/stop',{method:'POST'});
  } else {
    await fetch('/api/window/start',{method:'POST'});
  }
}

async function skipPrep(){
  await fetch('/api/window/skip_prep',{method:'POST'});
}

async function skipToNextGroup(){
  await fetch('/api/contest/next_group',{method:'POST'});
}

async function pickRescore(evt,ri,gi){
  evt.stopPropagation();
  const btn=evt.currentTarget;
  const orig=btn.textContent;
  btn.textContent='⏳'; btn.disabled=true;
  try{
    const r=await fetch('/api/rescore/pick/'+ri+'/'+gi,{method:'POST'});
    const d=await r.json();
    btn.textContent=d.ok?'✓':orig;
    btn.style.color=d.ok?'var(--good)':'var(--danger)';
    btn.title=d.message;
    setTimeout(()=>{btn.textContent=orig;btn.style.color='';btn.disabled=false;},4000);
  }catch(e){
    btn.textContent=orig; btn.disabled=false;
  }
}

async function rescoreRound(evt,ri,gi){
  evt.stopPropagation();
  const el=evt.currentTarget;
  const isBtn=el.tagName==='BUTTON';
  if(isBtn){el.textContent='...';el.disabled=true;}
  else{el.style.outline='2px solid var(--cyan)';}
  try{
    const r=await fetch('/api/rescore/'+ri+'/'+gi,{method:'POST'});
    const d=await r.json();
    if(isBtn){
      el.textContent=d.ok?'✓':'✗';
      el.style.color=d.ok?'var(--good)':'var(--danger)';
      el.title=d.message;
      setTimeout(()=>{el.textContent='↺';el.style.color='var(--cyan)';el.disabled=false;},3000);
    }else{
      el.style.outline='';
      el.style.background=d.ok?'rgba(46,125,50,.15)':'rgba(198,40,40,.15)';
      el.title=d.message;
      setTimeout(()=>{el.style.background='';},2500);
    }
  }catch(e){
    if(isBtn){el.textContent='✗';el.style.color='var(--danger)';}
  }
}

async function rescoreAll(){
  const btn=document.getElementById('rescore-all-btn');
  if(!btn)return;
  btn.disabled=true;
  const orig=btn.textContent;
  btn.textContent='Rescoring...';
  const st=await fetch('/state').then(r=>r.json());
  const rounds=st.contest?.rounds||[];
  let ok=0,fail=0;
  for(let ri=0;ri<rounds.length;ri++){
    const grps=rounds[ri].groups||[];
    for(let gi=0;gi<grps.length;gi++){
      if(grps[gi].status!=='complete')continue;
      const r=await fetch('/api/rescore/'+ri+'/'+gi,{method:'POST'});
      const d=await r.json();
      if(d.ok)ok++;else fail++;
    }
  }
  btn.textContent=orig+' ('+ok+' ok'+(fail?' '+fail+' failed':'')+')';
  btn.disabled=false;
}

// ── F3XVault JS ────────────────────────────────────────────────────────

function _vaultMsg(msg, color){
  const el=document.getElementById('vault-msg');
  if(el){ el.textContent=msg; el.style.color=color||'var(--muted)'; }
}

function _vaultUpdateBadge(vaultState){
  const badge = document.getElementById('vault-status-badge');
  const pushBtn = document.getElementById('vault-push-all-btn');
  if(!badge) return;
  if(!vaultState?.available){
    badge.textContent='NOT INSTALLED';
    badge.style.cssText='background:#444;color:#aaa;padding:2px 6px;border-radius:2px;font-size:.6rem';
    return;
  }
  if(vaultState.pulled){
    const issues = (vaultState.review||0) + (vaultState.unmatched||0);
    if(issues > 0){
      badge.textContent=`PULLED — ${issues} REVIEW`;
      badge.style.cssText='background:rgba(255,179,0,.2);color:var(--warn);padding:2px 6px;border-radius:2px;font-size:.6rem';
    } else {
      badge.textContent=`PULLED — ${vaultState.matched} pilots`;
      badge.style.cssText='background:rgba(0,230,118,.15);color:var(--good);padding:2px 6px;border-radius:2px;font-size:.6rem';
    }
    // Enable push button
    if(pushBtn){ pushBtn.style.opacity='1'; pushBtn.style.pointerEvents='auto'; }
  } else {
    badge.textContent='NOT PULLED';
    badge.style.cssText='background:rgba(255,255,255,.05);color:var(--muted);padding:2px 6px;border-radius:2px;font-size:.6rem';
    if(pushBtn){ pushBtn.style.opacity='.4'; pushBtn.style.pointerEvents='none'; }
  }
}

async function vaultPull(){
  const btn = document.getElementById('vault-pull-btn');
  const detail = document.getElementById('vault-match-detail');
  if(btn){ btn.disabled=true; btn.textContent='Pulling...'; }
  _vaultMsg('Contacting F3XVault...','var(--cyan)');
  try {
    const r = await fetch('/api/vault/pull', {method:'POST',
      headers:{'Content-Type':'application/json'}, body:'{}'});
    const d = await r.json();
    if(!d.ok){
      _vaultMsg('Pull failed: '+(d.error||'unknown error'), 'var(--danger)');
      return;
    }
    // Show match summary
    const issues = (d.review||[]).length + (d.unmatched||[]).length;
    _vaultMsg(
      `${d.event_name} — ${d.matched.length} matched`
      + (issues ? `, ${issues} need review` : ', all OK'),
      issues ? 'var(--warn)' : 'var(--good)'
    );
    // Build detail panel
    let html = '';
    if((d.review||[]).length){
      html += '<div style="color:var(--warn);margin-bottom:4px">⚠ REVIEW — confirm these matches:</div>';
      d.review.forEach(m=>{
        html += `<div style="color:var(--warn)">&nbsp;&nbsp;'${m.name}' ↔ '${m.vault_name}' (${(m.score*100).toFixed(0)}%)</div>`;
      });
    }
    if((d.unmatched||[]).length){
      html += '<div style="color:var(--danger);margin-top:4px;margin-bottom:2px">✗ NOT FOUND in F3XVault event:</div>';
      d.unmatched.forEach(u=>{
        html += `<div style="color:var(--danger)">&nbsp;&nbsp;'${u.name}'`
          + (u.closest ? ` — closest: '${u.closest}' (${(u.score*100).toFixed(0)}%)` : '') + '</div>';
      });
      html += '<div style="color:var(--muted);margin-top:4px">Add pilot_overrides to config.json to fix persistent mismatches.</div>';
    }
    if(detail){
      detail.innerHTML = html;
      detail.style.display = html ? 'block' : 'none';
    }
  } catch(e){
    _vaultMsg('Network error: '+e.message, 'var(--danger)');
  } finally {
    if(btn){ btn.disabled=false; btn.textContent='↓ Pull from F3XVault'; }
  }
}

async function vaultPushAll(){
  const n_complete = state?.contest?.rounds?.reduce((acc,r)=>
    acc + (r.groups||[]).filter(g=>g.status==='complete').length, 0) || 0;
  if(!n_complete){
    alert('No completed rounds to push.'); return;
  }
  if(!confirm(`Push all ${n_complete} completed round/groups to F3XVault?\n\nThis will post flight data for all pilots. You can re-push at any time to correct scores.`)) return;
  const btn = document.getElementById('vault-push-all-btn');
  if(btn){ btn.disabled=true; btn.textContent='Pushing...'; }
  _vaultMsg('Uploading to F3XVault...','var(--cyan)');
  try {
    const r = await fetch('/api/vault/push_all', {method:'POST',
      headers:{'Content-Type':'application/json'}, body:'{}'});
    const d = await r.json();
    _vaultMsg(d.message || (d.ok ? 'Push started' : d.error),
              d.ok ? 'var(--good)' : 'var(--danger)');
  } catch(e){
    _vaultMsg('Network error: '+e.message, 'var(--danger)');
  } finally {
    if(btn){ btn.disabled=false; btn.textContent='↑ Push All Rounds'; }
  }
}

// Update vault badge whenever state refreshes
function _updateVaultUI(d){
  if(d?.vault) _vaultUpdateBadge(d.vault);
}

async function cancelLogFetch(evt, uid){
  evt.stopPropagation();  // don't trigger card click
  const name = state?.units?.find(u=>u.id===uid)?.pilot_name || ('Unit '+uid);
  if(!confirm(`Record DNF for ${name}?\nRetry queue cleared — zero score for this round.`)) return;
  await fetch(`/api/log/cancel/${uid}`,{method:'POST'});
}

async function togglePause(){
  const isPaused = (document.getElementById('pause-btn')?.textContent||'').includes('RESUME');
  await fetch(isPaused ? '/api/window/resume' : '/api/window/pause', {method:'POST'});
}

// ── Helpers ────────────────────────────────────────────────────
function f(v,dp=1,sfx=''){
  if(v===null||v===undefined) return '—';
  return v.toFixed(dp)+sfx;
}
function sclass(u){
  if(u.stale)            return '';
  if(u.state===S_LAUNCH) return 's-launch';
  if(u.state===S_FLIGHT) return 's-flight';
  if(u.state===S_LANDED) return 's-landed';
  return '';
}

// ── Unit cards ─────────────────────────────────────────────────
function renderCards(units, taskId) {
  const ppf = state?.penalty_per_foot || 1.0;
  const g=document.getElementById('grid');
  if(!units.length){
    g.innerHTML='<div class="nodata" style="grid-column:1/-1">WAITING FOR FLIGHT UNITS</div>';
    return;
  }
  g.innerHTML=units.map(u=>{
    const sc=sclass(u), st=u.stale?'stale':'';
    const alt=u.state===S_GROUND?'—':`${u.altitude_ft.toFixed(1)}ft`;
    const adj=u.adjusted_score_s;
    const adjC=adj<0?'mv ng':'mv gr';
    const bp=u.battery_pct;
    const bc=bp<20?'c':bp<40?'w':'';
    const badgeC=`b-${u.state_name}`;
    const norm=u.normalised_score;
    const normStr=norm===null?'—':`${norm.toFixed(1)}`;
    const taskRaw=u.task_raw_s>0?f(u.task_raw_s,1,'s'):'—';
    const prog=u.task_progress||'';
    const detail=u.task_detail||'';
    const pname=u.pilot_name||'';

    const ls = state?.log_status?.[String(u.id)];
    let lsText = ls ? ({none:'',announced:'📋 READY',fetching:'📥 FETCHING',
      complete:'✓ LOG',error:'✗ ERR',queued:'⏳ QUEUED',dnf:'DNF'}[ls.status]||'') : '';
    if(ls && ls.status==='queued' && ls.retry_attempts){
      lsText = `⏳ RETRY ${ls.retry_attempts}`;}
    // Enhance badge with summary score if available
    let lsTitle = ls?.status==='complete' ? 'Log retrieved' : (ls?.status||'');
    if(ls?.status==='complete' && ls.summary_totals){
      const sf = ls.summary_totals.sum_secsft;
      lsText = `✓ ${sf!=null ? sf.toFixed(1)+'s' : 'LOG'}`;
      const flights = ls.summary_flights||[];
      lsTitle = flights.map((f,i)=>`F${i+1}: ${f.duration_s.toFixed(1)}s `
        +`lh=${f.launch_height_ft.toFixed(0)}ft → ${f.secsft_score.toFixed(1)}s`).join('\n')
        + (ls.summary_totals ? `\nTotal: ${ls.summary_totals.sum_secsft.toFixed(1)}s` : '');
    }
    const canCancel = ls && ['queued','error','announced'].includes(ls.status);
    const cancelBtn = canCancel
      ? `<button class="log-cancel" onclick="cancelLogFetch(event,${u.id})" title="Cancel — record DNF">✕ DNF</button>`
      : '';
    const logBadge = lsText ? `<span class="log-badge ${ls.status}" 
      title="${lsTitle.replace(/"/g,'&quot;')}">${lsText}</span>${cancelBtn}` : '';
    return `
    <div class="card ${sc} ${st}">
      <div class="ctop">
        <div class="card-id-row">
          <div class="uid">${String(u.id).padStart(2,'0')}</div>
          ${pname?`<div class="pilot-name">${pname}</div>`:''}
          ${logBadge}
        </div>
        <div class="badge ${badgeC}">${u.state_name}</div>
      </div>
      <div class="metrics">
        <div class="met"><div class="ml">Alt</div>
          <div class="mv cy">${alt}</div></div>
        <div class="met"><div class="ml">Peak</div>
          <div class="mv">${f(u.peak_alt_ft,0,'ft')}</div></div>
        <div class="met"><div class="ml">Launch Ht</div>
          <div class="mv or">${u.launch_height_ft>0?f(u.launch_height_ft,1,'ft'):'—'}</div></div>
        <div class="met"><div class="ml">Raw</div>
          <div class="mv">${u.raw_time_s>0?f(u.raw_time_s,1,'s'):'—'}</div></div>
        <div class="met"><div class="ml">Penalty</div>
          <div class="mv pe">${u.penalty_s>0?'-'+f(u.penalty_s,1,'s'):'—'}</div></div>
        <div class="met"><div class="ml">Adj Score</div>
          <div class="${adjC}">${u.raw_time_s>0?f(adj,1,'s'):'—'}</div></div>
        <div class="task-score-row">
          <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:5px">
            <div class="met"><div class="ml">Task Raw</div>
              <div class="mv pu">${taskRaw}</div></div>
            <div class="met"><div class="ml">Normalised</div>
              <div class="mv pu">${normStr}</div></div>
            <div class="met"><div class="ml">Flights</div>
              <div class="mv">${u.flight_count||0}</div></div>
          </div>
          ${prog?`<div class="task-score-detail">${prog}</div>`:''}
          ${(u.task_flights&&u.task_flights.length)?(
            '<div class="flight-list">'+
            u.task_flights.map((f,i)=>{
              const raw=f.dur>0?f.dur.toFixed(1)+'s':'DQ';
              const adj=f.dq?'DQ':(f.dur-(f.lh_ft||0)*ppf).toFixed(1)+'s';
              const cls=f.dq?' fl-dq':'';
              return '<span class="fl-item'+cls+'">'
                +'<span class="fl-n">F'+(i+1)+'</span>'
                +' <span class="fl-raw">'+raw+'</span>'
                +(f.dq?'':' <span class="fl-adj">→'+adj+'</span>')
                +'</span>';
            }).join('')+
            '</div>'
          ):''}
        </div>
        <div class="bbar"><div class="bfil ${bc}" style="width:${bp}%"></div></div>
        <div class="spark-wrap" id="spark-${u.id}">
          <svg id="sparksvg-${u.id}" viewBox="0 0 300 52" preserveAspectRatio="none"></svg>
          <div class="spark-empty" id="spark-empty-${u.id}">NO DATA</div>
        </div>
      </div>
      <button class="dbg-toggle" id="dbgtog-${u.id}" onclick="toggleDbg(${u.id})">
        <span class="dbg-arrow">▶</span>DEBUG
      </button>
      <div class="dbg-panel" id="dbgpanel-${u.id}"></div>
    </div>`;
  }).join('');
}

// ── Leaderboard ────────────────────────────────────────────────
// Track which rows are expanded in the leaderboard
const lbExpanded = new Set();
let _lbHistMode  = false;   // true = showing historical group, false = live
let _lbHistRi    = null;    // round index being viewed
let _lbHistGi    = null;    // group index being viewed
let _lbHistBtns  = '';      // last rendered hist buttons hash

function lbSelectLive(){
  _lbHistMode = false; _lbHistRi = null; _lbHistGi = null;
  _lbHash = '';   // force rebuild
  document.getElementById('lb-live-btn')?.classList.add('active');
  document.querySelectorAll('.lb-hist-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('lb-heading').textContent='Live Group — Task Score (Normalised)';
  if(state) renderLB(state.units, state.active_task);
}

async function lbSelectHist(ri, gi, label){
  _lbHistMode = true; _lbHistRi = ri; _lbHistGi = gi;
  document.getElementById('lb-live-btn')?.classList.remove('active');
  document.querySelectorAll('.lb-hist-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('lb-hbtn-'+ri+'-'+gi)?.classList.add('active');
  document.getElementById('lb-heading').textContent = label + ' — Results';
  await renderLBHistoric(ri, gi);
}

async function renderLBHistoric(ri, gi){
  const lb = document.getElementById('lb');
  try{
    const r = await fetch('/contest_data');
    if(!r.ok) return;
    const cd = await r.json();
    const grp = cd.rounds?.[ri]?.groups?.[gi];
    if(!grp || !Object.keys(grp.results||{}).length){
      lb.innerHTML='<div class="nodata">NO RESULTS FOR THIS GROUP</div>'; return;
    }
    const K = state?.penalty_per_foot || 0.5;
    const rlbl=i=>i===0?'<span class="r1">①</span>':
      i===1?'<span class="r2">②</span>':
      i===2?'<span class="r3">③</span>':`<span>${i+1}</span>`;

    const pilots = Object.entries(grp.results)
      .sort((a,b)=>(b[1].normalised||0)-(a[1].normalised||0));
    const unitMap = {};
    Object.entries(grp.pilots||{}).forEach(([uid,name])=>{ unitMap[name]=uid; });

    // Expandable rows — reuse lbExpanded set
    lb.innerHTML = '<div class="lbh">'+
      '<div>#</div><div>Unit</div><div>Pilot</div>'+
      '<div>Normalised</div><div>Task Raw</div><div>Flights</div>'+
    '</div>'+
    pilots.map(([name,res],i)=>{
      const norm   = res.normalised;
      const nStr   = norm===null||norm===undefined?'—':norm.toFixed(1);
      const uid    = unitMap[name]||'—';
      const uidKey = String(name).replace(/\W/g,'_');   // stable key for expand set
      const isExp  = lbExpanded.has('hist_'+uidKey);
      const flights= res.flights||[];

      // Best adjusted flight for highlighting
      const bestAdj = flights.length
        ? Math.max(...flights.map(fl=>{
            if(fl.dq) return -Infinity;
            const c = fl.capped_dur!=null ? fl.capped_dur : fl.dur;
            return c - fl.lh_ft*K;
          }))
        : -Infinity;

      const flightRows = flights.map((fl,fi)=>{
        const raw    = fl.dur||0;
        const capped = fl.capped_dur!=null ? fl.capped_dur : raw;
        const lh     = fl.lh_ft||0;
        const pen    = +(lh*K).toFixed(2);
        const fadj   = fl.dq ? 0 : +(capped-pen).toFixed(2);
        const wasCapd= !fl.dq && (raw-capped)>0.05;
        const isBest = !fl.dq && fadj===bestAdj && flights.length>1;
        const adjCls = fl.dq?'fl-dq':fadj<0?'fl-adj-neg':'fl-adj-pos';
        const bestMk = isBest?'fl-best':'';
        const rawCell= fl.dq?'DQ'
          : wasCapd
            ? '<span style="color:var(--muted);text-decoration:line-through">'+raw.toFixed(1)+'s</span>&nbsp;'+capped.toFixed(1)+'s'
            : raw.toFixed(1)+'s';
        const peak   = fl.peak_alt_ft ? fl.peak_alt_ft.toFixed(1)+'ft' : '—';
        return '<tr>'+
          '<td>'+(fi+1)+'</td>'+
          '<td class="'+bestMk+'">'+rawCell+'</td>'+
          '<td>'+(fl.dq?'—':lh.toFixed(1)+'ft')+'</td>'+
          '<td>'+(fl.dq?'—':'-'+pen.toFixed(2)+'s')+'</td>'+
          '<td class="'+adjCls+' '+bestMk+'">'+(fl.dq?'0 (DQ)':(fadj>=0?'+':'')+fadj.toFixed(2)+'s')+'</td>'+
          '<td style="color:var(--muted)">'+peak+'</td>'+
        '</tr>';
      }).join('');

      const logHtml = flights.length ? (
        '<div class="flight-log '+(isExp?'open':'')+'" id="hist-flog-'+uidKey+'">'+
        '<table class="fl-table"><thead><tr>'+
          '<th>#</th><th>Raw → Scorable</th><th>Launch Ht</th>'+
          '<th>Penalty</th><th>Adjusted</th><th>Peak Alt</th>'+
        '</tr></thead><tbody>'+flightRows+'</tbody></table></div>'
      ) : '';

      return '<div class="lbr '+(isExp?'expanded':'')+'"'+
        ' id="hist-lbr-'+uidKey+'"'+
        ' data-key="'+uidKey+'"'+
        ' onclick="toggleHistFlightLog(this)">'+
        '<div class="rk">'+rlbl(i)+'</div>'+
        '<div class="lid">'+String(uid).padStart(2,'0')+'</div>'+
        '<div class="lname">'+name+'</div>'+
        '<div class="lts">'+nStr+'</div>'+
        '<div class="lr">'+res.task_raw_s.toFixed(1)+'s</div>'+
        '<div class="lf">'+flights.length+' flt ▾</div>'+
      '</div>'+logHtml;
    }).join('');
  }catch(e){ console.error(e); }
}

function toggleHistFlightLog(el){
  const uidKey = el.dataset.key;
  const key = 'hist_'+uidKey;
  const wasOpen = lbExpanded.has(key);
  if(wasOpen) lbExpanded.delete(key); else lbExpanded.add(key);
  const row  = document.getElementById('hist-lbr-'+uidKey);
  const flog = document.getElementById('hist-flog-'+uidKey);
  if(row)  row.classList.toggle('expanded', !wasOpen);
  if(flog) flog.classList.toggle('open',    !wasOpen);
}

function updateLBHistButtons(ct){
  // Rebuild history buttons when contest rounds change
  const rnds = ct?.rounds||[];
  const newHash = rnds.map(r=>r.status+r.n_complete).join(',');
  if(newHash === _lbHistBtns) return;
  _lbHistBtns = newHash;
  const wrap = document.getElementById('lb-hist-btns');
  if(!wrap) return;
  let html = '';
  rnds.forEach((r,ri)=>{
    for(let gi=0; gi<(r.n_complete||0); gi++){
      const lbl = 'R'+r.round_num+' G'+(gi+1);
      html += '<button class="lb-hist-btn" id="lb-hbtn-'+ri+'-'+gi+'"'+
        ' onclick="lbSelectHist('+ri+','+gi+',\'R'+r.round_num+' G'+(gi+1)+' ('+r.task+')\')">'+
        lbl+'</button>';
    }
  });
  wrap.innerHTML = html;
}

let _lbHash = '';      // last rendered data hash — skip rebuild if unchanged
let _lbTaskId = '';    // last rendered task id

function _lbDataHash(units, taskId){
  // Cheap fingerprint of scoring state — only rebuild DOM when this changes
  return taskId + '|' + units
    .filter(u=>u.flight_count>0)
    .sort((a,b)=>b.task_raw_s-a.task_raw_s)
    .map(u=>u.id+':'+u.task_raw_s.toFixed(2)+':'+u.flight_count+':'+
             (u.normalised_score===null?'n':u.normalised_score.toFixed(1)))
    .join(',');
}

function renderLB(units, taskId) {
  const lb=document.getElementById('lb');

  const ranked=units
    .filter(u=>u.flight_count>0)
    .sort((a,b)=>b.task_raw_s-a.task_raw_s);

  if(!ranked.length){
    // Only update if it wasn't already showing the empty state
    if(_lbHash !== 'empty'){
      lb.innerHTML='<div class="nodata">NO COMPLETED FLIGHTS YET</div>';
      _lbHash = 'empty';
    }
    return;
  }

  // Skip full rebuild if data hasn't changed AND no rows are expanded
  // (expanded rows need fresh flight data in case a new flight just landed)
  const newHash = _lbDataHash(units, taskId);
  if(newHash === _lbHash && lbExpanded.size === 0) return;
  _lbHash = newHash;

  const rlbl=i=>i===0?'<span class="r1">①</span>':
    i===1?'<span class="r2">②</span>':
    i===2?'<span class="r3">③</span>':`<span>${i+1}</span>`;

  const K = state?.penalty_per_foot || 0.5;

  lb.innerHTML=`
    <div class="lbh">
      <div>#</div><div>Unit</div><div>Pilot</div>
      <div>Normalised</div><div>Task Raw</div>
      <div>Adj Score</div><div>Launch Ht</div><div>Flights</div>
    </div>
    ${ranked.map((u,i)=>{
      const norm  = u.normalised_score;
      const nStr  = norm===null?'—':`${norm.toFixed(1)}`;
      const adj   = u.adjusted_score_s;
      const adjC  = adj<0?'lts ng':'lts';
      const uid   = u.id;
      const isExp = lbExpanded.has(uid);
      const flights = u.task_flights || [];

      // Determine if flights come from unit summary CSV or real-time UDP
      const hasSummary = flights.some(fl => fl.from_summary);
      const ls = state && state.log_status && state.log_status[String(uid)];

      const bestAdj = flights.length
        ? Math.max(...flights.map(fl => {
            if(fl.dq) return -Infinity;
            const c = fl.capped_dur != null ? fl.capped_dur : fl.dur;
            return c - fl.lh_ft * K;
          }))
        : -Infinity;

      let flightRows, tableHead;

      if(hasSummary){
        tableHead = '<tr><th>#</th><th>Duration</th><th>Launch Ht</th>'
          + '<th>Secs-Ft Score</th><th>JoeD V1</th></tr>';
        const bestSF = Math.max(...flights.map(fl => fl.secsft_score || 0));
        flightRows = flights.map((fl, fi) => {
          const sc = fl.secsft_score != null ? fl.secsft_score : 0;
          const jo = fl.joed_score   != null ? fl.joed_score   : 0;
          const isBest = sc === bestSF && flights.length > 1;
          const scCls  = sc < 0 ? 'fl-adj-neg' : 'fl-adj-pos';
          return '<tr' + (isBest ? ' class="fl-best-row"' : '') + '>'
            + '<td>' + (fi + 1) + '</td>'
            + '<td>' + fl.dur.toFixed(1) + 's</td>'
            + '<td>' + fl.lh_ft.toFixed(1) + 'ft</td>'
            + '<td class="' + scCls + '">' + (sc >= 0 ? '+' : '') + sc.toFixed(2) + 's</td>'
            + '<td style="color:var(--muted)">' + jo.toFixed(1) + '</td>'
            + '</tr>';
        }).join('');
        const tot = ls && ls.summary_totals;
        if(tot){
          const tc = tot.sum_secsft < 0 ? 'fl-adj-neg' : 'fl-adj-pos';
          flightRows += '<tr style="border-top:2px solid var(--bdr);font-weight:700">'
            + '<td>Total</td>'
            + '<td>' + tot.total_dur_s.toFixed(1) + 's</td>'
            + '<td>' + tot.avg_launch_ft.toFixed(1) + 'ft avg</td>'
            + '<td class="' + tc + '">' + (tot.sum_secsft >= 0 ? '+' : '') + tot.sum_secsft.toFixed(2) + 's</td>'
            + '<td style="color:var(--muted)">' + tot.avg_joed.toFixed(1) + ' avg</td>'
            + '</tr>';
        }
      } else {
        tableHead = '<tr><th>#</th><th>Raw to Scorable</th><th>Launch Ht</th>'
          + '<th>Penalty</th><th>Adjusted</th></tr>';
        flightRows = flights.map((fl, fi) => {
          const raw    = fl.dur || 0;
          const capped = fl.capped_dur != null ? fl.capped_dur : raw;
          const lh     = fl.lh_ft || 0;
          const pen    = +(lh * K).toFixed(2);
          const fadj   = fl.dq ? 0 : +(capped - pen).toFixed(2);
          const wasCapd = !fl.dq && (raw - capped) > 0.05;
          const isBest  = !fl.dq && fadj === bestAdj && flights.length > 1;
          const adjCls  = fl.dq ? 'fl-dq' : fadj < 0 ? 'fl-adj-neg' : 'fl-adj-pos';
          const bestMk  = isBest ? 'fl-best' : '';
          const rawCell = fl.dq ? 'DQ'
            : wasCapd
              ? '<span style="color:var(--muted);text-decoration:line-through">'
                + raw.toFixed(1) + 's</span>&nbsp;' + capped.toFixed(1) + 's'
              : raw.toFixed(1) + 's';
          return '<tr>'
            + '<td>' + (fi + 1) + '</td>'
            + '<td class="' + bestMk + '">' + rawCell + '</td>'
            + '<td>' + (fl.dq ? '--' : lh.toFixed(1) + 'ft') + '</td>'
            + '<td>' + (fl.dq ? '--' : '-' + pen.toFixed(2) + 's') + '</td>'
            + '<td class="' + adjCls + ' ' + bestMk + '">'
              + (fl.dq ? '0 (DQ)' : (fadj >= 0 ? '+' : '') + fadj.toFixed(2) + 's')
            + '</td></tr>';
        }).join('');
      }

      const srcLabel = hasSummary
        ? '<span style="font-family:var(--mono);font-size:.58rem;color:var(--good);margin-left:8px">FROM UNIT LOG</span>'
        : '<span style="font-family:var(--mono);font-size:.58rem;color:var(--muted);margin-left:8px">REAL-TIME UDP</span>';

      const logHtml = flights.length ? (
        '<div class="flight-log '+(isExp?'open':'')+' " id="flog-'+uid+'">'
        +'<div style="display:flex;align-items:center;padding:4px 8px 2px">'
        +'<span style="font-family:var(--mono);font-size:.6rem;color:var(--muted)">SCORECARD</span>'
        +srcLabel
        +'</div>'
        +'<table class="fl-table">'
        +'<thead>'+tableHead+'</thead>'
        +'<tbody>'+flightRows+'</tbody>'
        +'</table></div>'
      ) : '';

      return `<div class="lbr ${isExp?'expanded':''}"
          id="lbr-${uid}" onclick="toggleFlightLog(${uid})">
        <div class="rk">${rlbl(i)}</div>
        <div class="lid">${String(uid).padStart(2,'0')}</div>
        <div class="lname">${u.pilot_name||''}</div>
        <div class="${adjC}">${nStr}</div>
        <div class="lr">${f(u.task_raw_s,1,'s')}</div>
        <div class="lr">${u.raw_time_s>0?f(adj,1,'s'):'—'}</div>
        <div class="ll">${f(u.launch_height_ft,1,'ft')}</div>
        <div class="lf">${u.flight_count} flt ▾</div>
      </div>${logHtml}`;
    }).join('')}`;
}

function toggleFlightLog(uid){
  const wasOpen = lbExpanded.has(uid);
  if(wasOpen) lbExpanded.delete(uid);
  else        lbExpanded.add(uid);

  // Toggle DOM directly — much faster than full rebuild
  const row  = document.getElementById('lbr-'  + uid);
  const flog = document.getElementById('flog-' + uid);
  if(row)  row.classList.toggle('expanded', !wasOpen);
  if(flog) flog.classList.toggle('open',    !wasOpen);

  // If opening, force a hash invalidation so next poll refreshes flight data
  if(!wasOpen) _lbHash = '';
}

// ── Settings render ────────────────────────────────────────────
// ── Roster state ──────────────────────────────────────────────
let localRoster    = [];         // local copy of master roster
let checkinSet     = new Set();  // names currently checked in
let rosterInitDone = false;
let activeUnitMap  = {};
let checkinUnits   = {};   // name → pre-assigned uid (from server checkins)   // name → uid for currently active group
let _unitMapHash   = '';   // hash of active group assignment for dirty-check

function renderRoster(d) {
  if(!d.roster) return;
  if(!rosterInitDone || localRoster.length === 0){
    localRoster = [...Object.keys(d.checkins||{}).concat(
      (d.roster||[]).filter(n=>!(n in (d.checkins||{}))))];
    localRoster = [...(d.roster||[])];
    // Only sync checkins from server on first load
    if(d.checkins){
      checkinSet   = new Set(Object.keys(d.checkins));
      checkinUnits = {};
      Object.entries(d.checkins).forEach(([n,u])=>{ if(u) checkinUnits[n]=u; });
    }
    rosterInitDone = true;
    renderPilotList();
    updateDrawPoolInfo();
  }
  // Always update count display (non-destructive)
  const cnt = document.getElementById('roster-count');
  const cc  = document.getElementById('checkin-count');
  if(cnt) cnt.textContent = localRoster.length;
  if(cc)  cc.textContent  = localRoster.filter(n=>checkinSet.has(n)).length;

  // Rebuild name→uid map and only re-render if assignment changed
  const newMap = {};
  (d.groups||[]).forEach(g=>{
    Object.entries(g.pilots||{}).forEach(([uid,name])=>{
      newMap[name] = parseInt(uid);
    });
  });
  const newHash = JSON.stringify(newMap);
  if(newHash !== _unitMapHash){
    _unitMapHash = newHash;
    activeUnitMap = newMap;
    renderPilotList();
  }
  updateDrawPoolInfo();
}

function renderPilotList(){
  const ul  = document.getElementById('pilot-list');
  const cnt = document.getElementById('roster-count');
  const cc  = document.getElementById('checkin-count');
  if(!ul) return;
  cnt.textContent = localRoster.length;
  const inCount = localRoster.filter(n=>checkinSet.has(n)).length;
  if(cc) cc.textContent = inCount;
  // Display alphabetically A-Z by first name
  const sorted = [...localRoster].sort((a,b)=>a.localeCompare(b));
  ul.innerHTML = sorted.map(name=>{
    const inn = checkinSet.has(name);
    const esc = name.replace(/'/g,"\'");
    const preUid = checkinUnits[name];
    const unitVal = preUid ? String(preUid) : '';
    const unitInput = inn
      ? `<input class="unit-input" type="number" min="1" max="200"
          placeholder="--" value="${unitVal}"
          title="Pre-assigned unit number"
          onclick="event.stopPropagation()"
          onchange="setCheckinUnit('${esc}',this.value)"
          onkeydown="if(event.key==='Enter')this.blur();event.stopPropagation()">`
      : '<span></span>';
    return `<li class="pilot-item ${inn?'checked-in':''}"
        onclick="toggleCheckin('${esc}')">
      <div class="ci-toggle ${inn?'on':'off'}">${inn?'✓':''}</div>
      <span class="pname">${name}</span>
      ${unitInput}
      <button class="pilot-del"
        onclick="event.stopPropagation();removePilot('${esc}')">✕</button>
    </li>`;
  }).join('');
}

function updateDrawPoolInfo(){
  const total   = checkinSet.size;
  const avail   = localRoster.filter(n=>checkinSet.has(n)).length;
  const ciEl    = document.getElementById('draw-ci-count');
  const avEl    = document.getElementById('draw-avail-count');
  const cntEl   = document.getElementById('draw-count');
  if(ciEl) ciEl.textContent   = total;
  if(avEl) avEl.textContent   = avail;
  if(cntEl) cntEl.max         = avail || 20;
}

// Show "Add Current Draw" button whenever there's a pending draw
function updateAddDrawBtn(){
  const btn = document.getElementById('add-draw-btn');
}



async function setCheckinUnit(name, val){
  const uid = val && val.trim() ? parseInt(val) : null;
  if(uid !== null && (isNaN(uid) || uid < 1 || uid > 2000)) return;
  if(uid) checkinUnits[name] = uid;
  else    delete checkinUnits[name];
  await fetch('/api/checkin/unit',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({name, unit: uid})});
}

async function toggleCheckin(name){
  const present = !checkinSet.has(name);
  if(present) checkinSet.add(name);
  else      { checkinSet.delete(name); delete checkinUnits[name]; }
  renderPilotList();
  updateDrawPoolInfo();
  // Sync to server — include pre-assigned unit if known
  await fetch('/api/checkin',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({name, present, unit: checkinUnits[name]||null})});
}

async function checkInAll(){
  checkinSet = new Set(localRoster);
  renderPilotList();
  updateDrawPoolInfo();
  await fetch('/api/checkin',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({action:'all'})});
}

async function checkInNone(){
  checkinSet.clear();
  renderPilotList();
  updateDrawPoolInfo();
  await fetch('/api/checkin',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({action:'none'})});
}



function addPilot(){
  const inp  = document.getElementById('add-name');
  const name = inp.value.trim();
  if(!name || localRoster.includes(name)) return;
  localRoster.push(name);
  inp.value = '';
  renderPilotList();
  saveRoster();
}

function removePilot(name){
  localRoster = localRoster.filter(n=>n!==name);
  // Remove from draw if present
  renderPilotList();
  renderDrawResult();
  saveRoster();
}


async function saveRoster(){
  const r = await fetch('/api/roster',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({action:'set', roster:localRoster})});
  if(r.ok) flash('roster-status');
}







// ── Schedule functions ────────────────────────────────────────────
















// ── Contest tab ──────────────────────────────────────────────────
let localRoundPlan = [];   // [{task:'G'}, ...]
let contestInitDone = false;
let _rpHash = '';       // dirty-check for round plan table
let _stHash = '';       // dirty-check for standings table

function _roundPlanHash(ct){
  // Fingerprint: active round/group + per-round status + n_complete
  return (ct.active_round??'x') + '|' + (ct.active_group??'x') + '|' +
    (ct.rounds||[]).map(r=>r.status+r.n_complete).join(',');
}
function _standingsHash(ct){
  const st = ct.standings||{};
  return Object.keys(st).sort()
    .map(n=>(st[n].total||0).toFixed(1)).join(',');
}

function renderContest(d){
  if(!d.contest) return;
  const ct = d.contest;

  // One-time init of editable fields and local round plan
  if(!contestInitDone){
    contestInitDone = true;
    const ni = document.getElementById('c-name');
    const di = document.getElementById('c-date');
    const ui = document.getElementById('c-units');
    if(ni) ni.value = ct.contest?.name || 'F3K Contest';
    if(di) di.value = ct.contest?.date || '';
    if(ui) ui.value = ct.contest?.num_units || 8;
    localRoundPlan = (ct.rounds||[]).map(r=>({task:r.task||'G'}));
    if(!localRoundPlan.length){
      for(let i=0;i<6;i++) localRoundPlan.push({task:'G'});
    }
    _rpHash = ''; _stHash = '';   // force first render
  }

  // Sync drop mode buttons on every render (cheap)
  const dropMode = ct.contest?.drop_mode || 'auto';
  updateDropButtons(dropMode);
  const dropLabels = {auto:'Auto (FAI rule)',force:'Force drop — 1 round dropped',
                      none:'Drops disabled by CD'};
  const dStat = document.getElementById('drop-mode-status');
  if(dStat) dStat.textContent = dropLabels[dropMode]||'';

  // Update drop indicator on Standings tab
  const dInd = document.getElementById('drop-indicator');
  if(dInd){
    const n_done = (ct.rounds||[]).filter(r=>r.status==='complete').length;
    if(dropMode==='none'){
      dInd.textContent = '⊘ No drops — CD override';
      dInd.style.color = 'var(--muted)';
    } else if(dropMode==='force'){
      dInd.textContent = '↓ Force drop: 1 worst round dropped per pilot';
      dInd.style.color = 'var(--warn)';
    } else {
      const drops = n_done>=9?2:n_done>=5?1:0;
      dInd.textContent = drops>0
        ? '↓ FAI drop: '+drops+' worst round'+(drops>1?'s':'')+' dropped per pilot'
        : '— FAI drop rule: no drops yet (need 5+ rounds)';
      dInd.style.color = drops>0?'var(--warn)':'var(--muted)';
    }
  }

  // Round plan — skip if unchanged OR a select is open
  const newRpHash = _roundPlanHash(ct);
  if(newRpHash !== _rpHash){
    // Don't rebuild while a dropdown in the table is focused
    const active = document.activeElement;
    const tbody  = document.getElementById('round-plan-body');
    const inTable= tbody && tbody.contains(active);
    if(!inTable){
      _rpHash = newRpHash;
      renderRoundPlan(ct, d);
    }
  }

  // Standings — skip if unchanged
  const newStHash = _standingsHash(ct);
  if(newStHash !== _stHash){
    _stHash = newStHash;
    renderStandings(ct);
  }

  // Resume banner — always update (cheap DOM check)
  updateResumeBanner(ct);
}

function renderRoundPlan(ct, d){
  const tbody = document.getElementById('round-plan-body');
  if(!tbody) return;
  const rounds = ct.rounds || [];
  const active_ri = ct.active_round;
  const active_gi = ct.active_group;

  tbody.innerHTML = localRoundPlan.map((r, i)=>{
    const srv    = rounds[i] || {};
    const status = srv.status || 'planned';
    const isDone = status === 'complete';
    const isAct  = i === active_ri;
    const nDone  = srv.n_complete || 0;
    const nTotal = srv.n_groups  || 0;
    const _rBtn = isDone ? '<button onclick="rescoreRound(event,'+i+',0)" '
      + 'title="Auto rescore from logs" '
      + 'style="font-family:var(--mono);font-size:.65rem;font-weight:800;'
      + 'padding:2px 6px;border-radius:3px;border:1.5px solid var(--cyan);'
      + 'color:var(--cyan);background:none;cursor:pointer;margin-left:8px">↺</button>'
      + '<button onclick="pickRescore(event,'+i+',0)" '
      + 'title="Browse for summary CSV" '
      + 'style="font-family:var(--mono);font-size:.65rem;font-weight:800;'
      + 'padding:2px 6px;border-radius:3px;border:1.5px solid var(--muted);'
      + 'color:var(--muted);background:none;cursor:pointer;margin-left:2px">📂</button>'
      : '';
    const statusStr = isDone ? ('✓ done' + _rBtn)
      : isAct ? `▶ G${active_gi+1}/${nTotal}`
      : nTotal ? `${nTotal} groups` : 'planned';
    const rowCls = isDone?'done-row':isAct?'active-row':'';

    // Inline controls for active round
    const isTaskC = r.task === 'C';
    const inlineControls = isAct ? `
      <span style="display:inline-flex;align-items:center;gap:6px;flex-wrap:wrap">
        <span style="font-family:var(--mono);font-size:.6rem;color:var(--muted)">TEST</span>
        <select class="rtask-sel" id="inline-testing" style="width:80px"
          onchange="setInlineTesting(this.value)">
          <option value="60">1 min</option>
          <option value="120">2 min</option>
          <option value="180">3 min</option>
          <option value="240">4 min</option>
          <option value="300">5 min</option>
        </select>
        ${isTaskC ? `
        <span style="font-family:var(--mono);font-size:.6rem;color:var(--muted)">ATT</span>
        <select class="rtask-sel" id="inline-attempts" style="width:55px"
          onchange="setInlineAttempts(this.value)">
          <option value="3">3</option>
          <option value="5">5</option>
        </select>` : ''}
        <button class="gact" style="font-size:.65rem;color:var(--danger);border-color:var(--bdr)"
          onclick="resetRound()" title="Reset round scores">↺ Reset</button>
        <button class="gact" style="font-size:.65rem;color:var(--cyan);border-color:var(--bdr)"
          onclick="skipToNextGroup()" title="Skip to next group">⏭ Next Group</button>
      </span>` : '';

    const activateBtn = '<button class="gact" style="font-size:.65rem"'
      + ' onclick="activateRound(' + i + ')">▶</button>';
    const taskOptions = d.task_list.map(t=>
      '<option value="' + t.id + '"'
      + (t.id===r.task ? ' selected' : '') + '>'
      + t.id + ' — ' + t.name + ' — ' + t.desc
      + '</option>').join('');

    return '<tr class="' + rowCls + '">'
      + '<td style="font-family:var(--mono);color:var(--muted)">' + (i+1) + '</td>'
      + '<td><select class="rtask-sel" id="rtask-' + i + '"'
      + ' onchange="localRoundPlan[' + i + '].task=this.value;saveContest()"'
      + (isDone?' disabled':'') + '>'
      + taskOptions
      + '</select></td>'
      + '<td style="font-size:.72rem;color:'
      + (isDone?'var(--good)':isAct?'var(--warn)':'var(--muted)') + '">'
      + statusStr + '</td>'
      + '<td>' + (isAct ? inlineControls : activateBtn) + '</td>'
      + '</tr>';
  }).join('');
}

function renderStandings(ct){
  const wrap = document.getElementById('standings-wrap');
  if(!wrap) return;
  const st   = ct.standings || {};
  const rnds = ct.rounds    || [];
  const names = Object.keys(st).sort((a,b)=>
    (st[b].total||0)-(st[a].total||0));

  if(!names.length){
    wrap.innerHTML='<div class="nodata" style="padding:40px">No completed rounds yet</div>';
    return;
  }

  // Leader total for % column
  const leaderTotal = st[names[0]]?.total || 0;

  // Only show completed rounds in the header
  const completedRnds = rnds.filter(r=>r.status==='complete');
  const roundHeaders = completedRnds.map(r=>
    `<th>R${r.round_num}<br>
      <span style="font-weight:700;opacity:.6">${r.task||''}</span></th>`
  ).join('');

  wrap.innerHTML = `<table class="standings-table">
    <thead><tr>
      <th>#</th><th>Pilot</th>
      <th style="color:var(--cyan)">Total</th>
      <th>%</th>
      ${roundHeaders}
    </tr></thead>
    <tbody>
    ${names.map((name,rank)=>{
      const s      = st[name];
      const dropped= new Set(s.dropped||[]);
      const pct    = leaderTotal > 0
        ? (s.total / leaderTotal * 100).toFixed(1) + '%'
        : '—';
      const pctColor = rank===0 ? 'var(--good)' : 'var(--muted)';
      const pctCell  = `<td style="font-family:var(--mono);color:${pctColor}">${rank===0?'100.0%':pct}</td>`;

      // Cells only for completed rounds
      const cells = s.rounds.map((v,i)=>{
        if(!rnds[i] || rnds[i].status !== 'complete') return '';
        if(v===null) return '<td style="color:var(--muted)">—</td>';
        const isDrop = dropped.has(i);
        const tdClass = isDrop ? 's-drop' : '';
        const tdTitle = isDrop ? 'Dropped' : 'Rescore R'+(i+1);
        const tdStyle = isDrop ? '' : 'cursor:pointer';
        const tdClick = isDrop ? '' : 'onclick="rescoreRound(event,'+i+',0)"';
        const tdVal   = (!isDrop && v===0) ? '<b style="color:#c62828">0</b>' : v.toFixed(0);
        return '<td class="'+tdClass+'" title="'+tdTitle+'" style="'+tdStyle+'" '+tdClick+'>'+tdVal+'</td>';
      }).join('');

      const medal = rank===0?'①':rank===1?'②':rank===2?'③':(rank+1);
      return `<tr>
        <td style="color:${rank<3?'var(--cyan)':'var(--muted)'};font-size:1rem">${medal}</td>
        <td style="text-align:left;font-size:.85rem;font-weight:800">${name}</td>
        <td class="s-total">${s.total.toFixed(0)}</td>
        ${pctCell}
        ${cells}
      </tr>`;
    }).join('')}
    </tbody></table>`;
}

// ── Draws tab ────────────────────────────────────────────────────
let _drawsHash = '';
let _fullRounds = [];   // cached full round data from /contest_data

async function fetchFullRounds(){
  try{
    const r = await fetch('/contest_data');
    if(r.ok) _fullRounds = (await r.json()).rounds || [];
  }catch(e){}
}

// Sync wrapper called from render() — triggers async refresh only when needed
function drawsTabUpdate(d){
  const ct = d.contest || {};
  const newHash = (ct.rounds||[]).map(r=>r.status+r.n_complete+r.n_groups).join('|');
  if(newHash === _drawsHash && _fullRounds.length) return;
  // State changed — queue async redraw
  _drawsHash = newHash;
  renderDrawsTab(d);
}

async function renderDrawsTab(d){
  const wrap = document.getElementById('draws-content');
  if(!wrap) return;

  const ct = d.contest || {};
  await fetchFullRounds();
  // After fetch, re-check hash hasn't changed under us
  const rounds = _fullRounds;

  if(!rounds.length){
    wrap.innerHTML='<div class="nodata" style="padding:40px">No contest plan yet — set up rounds in the Contest tab</div>';
    return;
  }

  const taskMap = {};
  (d.task_list||[]).forEach(t=>{ taskMap[t.id]={name:t.name,desc:t.desc}; });
  const activeRi = ct.active_round;
  const activeGi = ct.active_group;

  wrap.innerHTML = rounds.map((rnd, ri)=>{
    const ti = taskMap[rnd.task] || {name:rnd.task,desc:''};
    const statusCls = rnd.status==='complete'?'var(--good)':
      rnd.status==='active'?'var(--warn)':'var(--muted)';
    const statusLbl = rnd.status==='complete'?'✓ COMPLETE':
      rnd.status==='active'?'▶ ACTIVE':'PLANNED';
    const groups = rnd.groups || [];

    const groupCards = !groups.length
      ? '<div style="font-family:var(--mono);font-size:.72rem;color:var(--muted);padding:6px 0">Groups not yet drawn</div>'
      : '<div style="display:flex;flex-wrap:wrap;gap:10px;margin-top:8px">'+
        groups.map(g=>{
          const isAct = ri===activeRi && (g.group_num-1)===activeGi;
          const isDone= g.status==='complete';
          const bdr   = isDone?'rgba(0,230,118,.25)':isAct?'rgba(255,179,0,.35)':'var(--bdr)';
          const pilots= Object.entries(g.pilots||{})
            .sort((a,b)=>parseInt(a[0])-parseInt(b[0]));
          return '<div style="background:var(--surf);border:1.5px solid '+bdr+';'+
            'border-radius:5px;overflow:hidden;min-width:180px;flex:1">'+
            '<div style="background:#0d1117;padding:5px 10px;display:flex;'+
              'align-items:center;justify-content:space-between">'+
              '<span style="font-family:var(--mono);font-size:.65rem;'+
                'font-weight:700;letter-spacing:.15em;color:var(--muted)">'+
                'GROUP '+g.group_num+'</span>'+
              (isDone?'<span style="color:var(--good);font-size:.65rem">✓</span>':
               isAct?'<span style="color:var(--warn);font-family:var(--mono);font-size:.6rem">▶ FLYING</span>':'')+
            '</div>'+
            pilots.map(([uid,name])=>{
              const uidPad = String(parseInt(uid)).padStart(2,'0');
              const safeName = JSON.stringify(name);
              // Only numeric args in data attrs — name looked up at click time
              const uidInt = parseInt(uid);
              const uidSpan = isDone
                ? '<span style="font-family:var(--mono);font-size:.7rem;color:var(--cyan);font-weight:700">'+uidPad+'</span>'
                : '<span class="draws-uid"'
                    +' data-ri="'+ri+'"'
                    +' data-gi="'+(g.group_num-1)+'"'
                    +' data-uid="'+uidInt+'"'
                    +' title="Click to change unit"'
                    +' onclick="drawsEditUid(this)">'
                    +uidPad+'</span>';
              return '<div style="display:grid;grid-template-columns:32px 1fr;gap:6px;'+
                'padding:5px 10px;border-bottom:1px solid rgba(33,38,45,.5);align-items:center">'+
                uidSpan+
                '<span style="font-size:.82rem;font-weight:800">'+name+'</span>'+
              '</div>';
            }).join('')+
          '</div>';
        }).join('')+
        '</div>';

    return '<div style="margin-bottom:22px">'+
      '<div style="display:flex;align-items:baseline;gap:10px;'+
        'padding-bottom:6px;border-bottom:1px solid var(--bdr);margin-bottom:8px">'+
        '<span style="font-family:var(--mono);font-size:.68rem;font-weight:700;'+
          'letter-spacing:.2em;color:var(--muted)">ROUND '+rnd.round_num+'</span>'+
        '<span style="font-size:.95rem;font-weight:700">'+ti.name+'</span>'+
        '<span style="font-size:.75rem;color:var(--muted)">'+ti.desc+'</span>'+
        '<span style="font-family:var(--mono);font-size:.6rem;padding:2px 7px;'+
          'border-radius:2px;margin-left:auto;color:'+statusCls+';'+
          'border:1.5px solid '+statusCls.replace('var(--','rgba(').replace(')',',.3)')+'">'+
          statusLbl+'</span>'+
      '</div>'+
      groupCards+
    '</div>';
  }).join('');
}

// ── Draws tab inline unit edit ───────────────────────────────────
function drawsEditUid(spanEl){
  // Read numeric args from data attributes — name looked up from cached data
  const ri     = parseInt(spanEl.dataset.ri);
  const gi     = parseInt(spanEl.dataset.gi);
  const oldUid = parseInt(spanEl.dataset.uid);
  // Look up pilot name from _fullRounds — avoids any quoting in HTML attrs
  const grpPilots = _fullRounds[ri]?.groups[gi]?.pilots || {};
  const pilotName = grpPilots[String(oldUid)] || '';
  if(!pilotName){ console.warn('drawsEditUid: no pilot at uid', oldUid); return; }

  const inp = document.createElement('input');
  inp.type      = 'text';
  inp.className = 'draws-uid-input';
  inp.value     = String(oldUid).padStart(2,'0');
  inp.maxLength = 2;
  spanEl.replaceWith(inp);
  inp.focus(); inp.select();

  function restore(){
    const span = document.createElement('span');
    span.className      = 'draws-uid';
    span.title          = 'Click to change unit';
    span.textContent    = String(oldUid).padStart(2,'0');
    span.dataset.ri  = ri;
    span.dataset.gi  = gi;
    span.dataset.uid = oldUid;
    span.onclick     = ()=>drawsEditUid(span);
    inp.replaceWith(span);
  }

  async function commit(){
    const newUid = parseInt(inp.value);
    if(isNaN(newUid)||newUid<1||newUid>20||newUid===oldUid){restore();return;}
    const r = await fetch('/api/contest/edit_uid',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({
        round_idx:ri, group_idx:gi,
        old_uid:oldUid, new_uid:newUid,
        name:pilotName
      })});
    const res = await r.json();
    if(res.ok){
      _drawsHash=''; _fullRounds=[];
    } else if(res.error==='collision'){
      // Unit already assigned — flash error then restore
      inp.style.borderColor = 'var(--danger)';
      inp.title = 'Unit ' + String(newUid).padStart(2,'0')
        + ' is already assigned to ' + res.taken_by;
      inp.value = String(newUid).padStart(2,'0');  // show the bad value briefly
      setTimeout(()=>{
        restore();
        // Brief error span in place of the uid
        const errSpan = document.getElementById
          ? document.querySelector('#draws-content .draws-uid[data-uid="'+oldUid+'"]')
          : null;
      }, 1200);
    } else {
      restore();
    }
  }

  inp.addEventListener('blur', commit);
  inp.addEventListener('keydown', e=>{
    if(e.key==='Enter')  inp.blur();
    if(e.key==='Escape') restore();
  });
}

function setInlineTesting(val){
  fetch('/api/round_settings',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({round_settings:{testing_s:parseInt(val)}})});
}

function setInlineAttempts(val){
  // Re-apply current task with new attempt count
  const n = parseInt(val);
  fetch('/api/task',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({task:'C',cfg:{numFlights:n,maxScore:n*180,window:0}})});
}

function syncInlineControls(d){
  // Populate testing dropdown with current server value
  const ts = document.getElementById('inline-testing');
  if(ts && d.round_settings) ts.value = String(d.round_settings.testing_s);
  // Populate attempts dropdown
  const at = document.getElementById('inline-attempts');
  if(at && d.task_info) at.value = String(d.task_info.numFlights||3);
}

function addRound(){
  localRoundPlan.push({task:'G'});
  _rpHash = '';   // force re-render bypassing dirty check
  if(state) renderRoundPlan(state.contest, state);
  saveContest();
}
function removeLastRound(){
  if(localRoundPlan.length > 1) localRoundPlan.pop();
  _rpHash = '';   // force re-render
  if(state) renderRoundPlan(state.contest, state);
  saveContest();
}

// ── Drop mode control ────────────────────────────────────────────
async function setDropMode(mode){
  const r = await fetch('/api/contest/drop_mode',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({mode})});
  if(r.ok){
    // Optimistically update buttons; full re-render on next poll
    updateDropButtons(mode);
    const labels={auto:'FAI auto-drop active',
      force:'Force drop enabled (1 round)',
      none:'Drops disabled by CD'};
    const el=document.getElementById('drop-mode-status');
    if(el) el.textContent=labels[mode]||'';
    _stHash='';   // force standings redraw
  }
}

function updateDropButtons(mode){
  ['auto','force','none'].forEach(m=>{
    const b=document.getElementById('drop-btn-'+m);
    if(!b) return;
    b.classList.toggle('active', m===mode);
    b.classList.remove('force','none');
    if(m===mode && m!=='auto') b.classList.add(m);
  });
}

// ── Archive contest ─────────────────────────────────────────────
async function archiveContest(){
  const ct = state?.contest?.contest;
  const name = ct?.name || 'F3K Contest';
  const date = ct?.date || '';
  // Build expected filename for user confirmation
  const safe = (name+'_'+date).replace(/[^a-zA-Z0-9\-_]/g,'_');
  const fname = 'contest_'+safe+'.json';

  if(!confirm(
    'Archive current contest?\n\n'+
    'Will save to: contests/'+fname+'\n\n'+
    'Then start a fresh blank contest.\n'+
    'This cannot be undone from the UI.')){
    return;
  }

  // First attempt — server will warn if a round is active
  let r = await fetch('/api/contest/archive',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({confirm:true})});

  if(r.status === 409){
    // Active round warning from server
    const warn = await r.json();
    if(!confirm('⚠ '+warn.message+'\n\nArchive anyway?')){
      return;
    }
    // Re-submit with force=true
    r = await fetch('/api/contest/archive',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({confirm:true, force:true})});
  }

  if(r.ok){
    const res = await r.json();
    alert('✓ Contest archived as:\ncontests/'+res.archived_as+'\n\nA fresh contest has been started.');
    // Reset all local UI state
    contestInitDone = false;
    _rpHash = ''; _stHash = '';
    _lbHistBtns = '';
    _drawsHash = ''; _fullRounds = [];
    showTab('contest');
  } else {
    const err = await r.json().catch(()=>({}));
    alert('Archive failed: '+(err.error||'unknown error'));
  }
}

async function saveContest(){
  const name  = document.getElementById('c-name')?.value||'F3K Contest';
  const date  = document.getElementById('c-date')?.value||'';
  const units = parseInt(document.getElementById('c-units')?.value||8);
  const rounds= localRoundPlan.map(r=>({task:r.task}));
  const r = await fetch('/api/contest',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({
      contest:{name,date,num_units:units,num_rounds:rounds.length},
      rounds
    })});
  if(r.ok){ flash('contest-status'); contestInitDone=false; _rpHash=''; _stHash=''; }
}

async function drawAllGroups(){
  const units = parseInt(document.getElementById('c-units')?.value||8);
  const r = await fetch('/api/contest/draw',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({group_size:units})});
  if(r.ok){
    const d = await r.json();
    const el = document.getElementById('contest-status');
    if(el){
      const rds=d.rounds||0, grps=d.groups||0, pts=d.pilots||0;
      const perGrp = grps>0 ? Math.round(pts/grps) : 0;
      el.textContent = `Drew ${pts} pilots across ${rds} rounds, ${grps} groups (~${perGrp}/group)`;
      el.style.color = 'var(--good)';
      el.style.opacity = '1';
      el.style.transition = '';
      clearTimeout(el._fadeTimer);
      el._fadeTimer = setTimeout(()=>{
        el.style.transition = 'opacity 1.5s';
        el.style.opacity = '0';
      }, 3000);
    }
    contestInitDone=false; _rpHash=''; _stHash='';
  }
}

async function startContest(){
  if(!confirm('Start contest? This will activate the first group of Round 1.')) return;
  const r = await fetch('/api/contest/start',{method:'POST'});
  if(r.ok) showTab('dashboard');
}

async function activateRound(ri){
  if(!confirm(`Manually activate Round ${ri+1}?`)) return;
  await fetch('/api/contest/activate',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({round_idx:ri,group_idx:0})});
  showTab('dashboard');
}

async function resumeFromGroup(ri, gi){
  if(!confirm(`Resume from Round ${ri+1} Group ${gi+1}?\nThis will activate that group and start the prep sequence.`)) return;
  await fetch('/api/contest/activate',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({round_idx:ri,group_idx:gi})});
  showTab('dashboard');
}

async function skipGroup(ri, gi){
  if(!confirm(`Skip Round ${ri+1} Group ${gi+1} and mark it complete with no results?\nThose pilots will receive no score for this round.`)) return;
  const r = await fetch('/api/contest/mark_group_complete',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({round_idx:ri,group_idx:gi})});
  if(r.ok){ _rpHash=''; _stHash=''; }
}

function updateResumeBanner(ct){
  const banner = document.getElementById('resume-banner');
  const rnds   = ct.rounds || [];
  if(!banner) return;

  // Find first round that has any planned groups but is not complete
  // (interrupted = round status planned but some groups planned and contest
  //  was previously running i.e. earlier rounds are complete)
  const hasComplete = rnds.some(r=>r.status==='complete');
  if(!hasComplete){ banner.style.display='none'; return; }

  // Find the first non-complete round
  const ri = rnds.findIndex(r=>r.status!=='complete');
  if(ri<0){ banner.style.display='none'; return; }

  // Only show banner if it has drawn groups (can resume)
  // We detect drawn groups from n_groups > 0
  const rnd = rnds[ri];
  if(!rnd || rnd.n_groups===0){ banner.style.display='none'; return; }

  // Show banner
  banner.style.display='';
  document.getElementById('resume-round-lbl').textContent =
    `${rnd.round_num} (Task ${rnd.task})`;

  // Build per-group buttons — need full group data from /contest_data
  // Use cached _fullRounds if available, otherwise show generic buttons
  const n = rnd.n_groups;
  const nDone = rnd.n_complete || 0;
  const btns = document.getElementById('resume-group-btns');
  if(!btns) return;

  let html = '';
  for(let gi=0; gi<n; gi++){
    const isDone = gi < nDone;
    if(isDone){
      html += `<span style="font-family:var(--mono);font-size:.65rem;
        color:var(--good);padding:3px 8px;border:1.5px solid rgba(0,230,118,.3);
        border-radius:2px">G${gi+1} ✓</span>`;
    } else {
      html +=
        `<button class="btn" style="font-size:.68rem;padding:4px 10px"
          onclick="resumeFromGroup(${ri},${gi})">▶ G${gi+1}</button>` +
        `<button class="btn secondary" style="font-size:.65rem;padding:3px 8px"
          onclick="skipGroup(${ri},${gi})" title="Mark group complete with no results">
          skip G${gi+1}</button> `;
    }
  }
  btns.innerHTML = html;
}





// Task-specific config UI
// Window times reference:
//   LL  — configurable 30s–600s (5s steps)
//   A   — toggle 7min(420s,cap300) or 10min(600s,cap480)
//   B   — fixed 600s
//   C   — no window; flight count 3-5
//   D   — fixed 600s
//   E   — toggle 10min(600) or 15min(900)
//   F-N — fixed (F,G,H,I,J,K,L,N=600s  M=900s)


// ── API calls ──────────────────────────────────────────────────
async function savePilots(){
  const pilots={};
  document.querySelectorAll('.pilot-input').forEach(inp=>{
    const uid=parseInt(inp.id.replace('pilot-',''));
    if(inp.value.trim()) pilots[uid]=inp.value.trim();
  });
  await fetch('/api/pilots',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({pilots})});
  flash('pilot-status');
}

async 

async function resetRound(){
  if(!confirm('Reset all flight scores for this round?')) return;
  await fetch('/api/reset',{method:'POST'});
}





function flash(id){
  const el=document.getElementById(id);
  el.classList.add('show');
  setTimeout(()=>el.classList.remove('show'),2000);
}

// ── Clock ──────────────────────────────────────────────────────
setInterval(()=>{
  document.getElementById('clk').textContent=new Date().toTimeString().slice(0,8);
},1000);
</script>
</body>
</html>"""

# ─────────────────────────────────────────────
#  Audio Engine
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
#  Audio Engine
#  Windows-native: pyttsx3 (SAPI5 TTS) +
#  sounddevice + numpy (tone generation)
#
#  Install once:
#    pip install pyttsx3 sounddevice numpy
#
#  Falls back gracefully if libs not present —
#  server runs fine without audio, just silent.
# ─────────────────────────────────────────────

import threading, queue, time as _time

# Attempt imports — audio is optional
try:
    import numpy as _np
    import sounddevice as _sd
    _HAVE_AUDIO = True
except (ImportError, OSError):
    _HAVE_AUDIO = False
    _np = None
    _sd = None

try:
    import winsound as _winsound
    _HAVE_WINSOUND = True
except ImportError:
    _HAVE_WINSOUND = False
    _winsound = None

try:
    import pyttsx3 as _pyttsx3
    _HAVE_TTS = True
except (ImportError, OSError):
    _HAVE_TTS = False
    _pyttsx3 = None

# ── Audio queue ──────────────────────────────
# Items are dicts: {'type': 'tone'|'tts'|'seq', ...}
_audio_q = queue.Queue()
_audio_thread = None

# ── TTS engine — re-initialised per utterance on Windows (SAPI5 fix) ─

def _fmt_time(s):
    """Format seconds as '47.3 seconds' or '1 minute 47 seconds'."""
    s = round(s, 1)
    if s < 60:
        return f"{s} seconds"
    m = int(s // 60)
    r = round(s % 60, 1)
    if r == 0:
        return f"{m} minute{'s' if m!=1 else ''}"
    return f"{m} minute{'s' if m!=1 else ''} {r} seconds"

def _tone(freq_hz, duration_s, volume=0.7, sample_rate=44100):
    """Generate and play a pure tone. Blocks until done.
    Prefers winsound.Beep on Windows (zero startup overhead).
    Falls back to sounddevice+numpy on other platforms.
    """
    dur_ms = int(duration_s * 1000)
    if _HAVE_WINSOUND:
        try:
            _winsound.Beep(int(freq_hz), dur_ms)
            return
        except Exception:
            pass   # fall through to sounddevice
    if not _HAVE_AUDIO:
        return
    try:
        t   = _np.linspace(0, duration_s, int(sample_rate * duration_s), False)
        wav = volume * _np.sin(2 * _np.pi * freq_hz * t).astype(_np.float32)
        fade = int(sample_rate * 0.005)
        if len(wav) > 2*fade:
            wav[:fade]  *= _np.linspace(0, 1, fade)
            wav[-fade:] *= _np.linspace(1, 0, fade)
        _sd.play(wav, sample_rate, blocking=True)
    except Exception as e:
        print(f"[AUDIO] Tone error: {e}")


def _horn(duration_s, volume=1.0, sample_rate=44100):
    """
    Multi-tone air horn simulation.
    Fundamental ~300 Hz with harmonics at 2x, 3x, 4x weighted to
    replicate the bright+bassy character of a compressed-air horn.
    Uses sounddevice+numpy even on Windows for multi-tone synthesis.
    Falls back to two sequential winsound.Beep calls if numpy unavailable.
    """
    if _HAVE_AUDIO:
        try:
            n   = int(sample_rate * duration_s)
            t   = _np.linspace(0, duration_s, n, False)
            # Harmonics: fundamental 300Hz, 2nd 600Hz, 3rd 900Hz, 4th 1200Hz
            # Weights approximate a real air horn spectrum
            wav = (0.55 * _np.sin(2*_np.pi*300*t) +
                   0.25 * _np.sin(2*_np.pi*600*t) +
                   0.12 * _np.sin(2*_np.pi*900*t) +
                   0.08 * _np.sin(2*_np.pi*1200*t))
            wav = (wav / _np.max(_np.abs(wav))) * volume
            wav = wav.astype(_np.float32)
            # Fast attack, slow decay envelope
            attack  = int(sample_rate * 0.015)
            release = int(sample_rate * 0.08)
            if len(wav) > attack + release:
                wav[:attack]   *= _np.linspace(0, 1, attack)
                wav[-release:] *= _np.linspace(1, 0, release)
            _sd.play(wav, sample_rate, blocking=True)
            return
        except Exception as e:
            print(f"[AUDIO] Horn error: {e}")
    # Fallback: single 500Hz winsound for full duration
    if _HAVE_WINSOUND:
        try:
            _winsound.Beep(500, int(duration_s * 1000))
        except Exception:
            pass

# Preferred Windows SAPI5 voice id — resolved once, reused each call
_tts_voice_id = None

def _resolve_voice():
    """Find the best available Windows SAPI5 voice. Called once."""
    global _tts_voice_id
    if _tts_voice_id is not None:
        return
    try:
        tmp = _pyttsx3.init()
        voices = tmp.getProperty('voices')
        for v in voices:
            n = v.name.lower()
            if 'david' in n or 'zira' in n or 'mark' in n:
                _tts_voice_id = v.id
                break
        if _tts_voice_id is None and voices:
            _tts_voice_id = voices[0].id   # fallback: first available voice
        try:
            tmp.stop()
        except Exception:
            pass
    except Exception as e:
        print(f"[AUDIO] Voice resolve error: {e}")

def _speak(text):
    """
    Speak text via SAPI5, routed through the Windows audio mixer so it
    respects the default playback device (e.g. Bluetooth headset).
    Uses win32com directly to set SpAudioOutput to the default device
    before speaking. Falls back to pyttsx3 if win32com is unavailable.
    Re-initialises the engine every call (pyttsx3 Windows requirement).
    """
    if not _HAVE_TTS or not text:
        return
    _resolve_voice()
    print(f"[AUDIO] TTS: {text}")
    try:
        import win32com.client as _win32
        # SpVoice created fresh each call uses the current Windows default
        # audio endpoint — no AudioOutput assignment needed or wanted.
        # pyttsx3 caches the device on first init, which is why it ignores
        # device changes; raw SpVoice does not have this problem.
        sapi = _win32.Dispatch("SAPI.SpVoice")
        sapi.Rate   = -1    # roughly 155 wpm equivalent
        sapi.Volume = 100
        if _tts_voice_id:
            for v in sapi.GetVoices():
                if v.Id == _tts_voice_id:
                    sapi.Voice = v
                    break
        sapi.Speak(text)
        return
    except ImportError:
        pass   # win32com not available — fall through to pyttsx3
    except Exception as e:
        print(f"[AUDIO] win32com TTS error: {e} — falling back to pyttsx3")
    # pyttsx3 fallback (may use pinned device on some systems)
    try:
        engine = _pyttsx3.init()
        engine.setProperty('rate',   155)
        engine.setProperty('volume', 1.0)
        if _tts_voice_id:
            engine.setProperty('voice', _tts_voice_id)
        engine.say(text)
        engine.runAndWait()
        try:
            engine.stop()
        except Exception:
            pass
    except Exception as e:
        print(f"[AUDIO] TTS error: {e}")

def _audio_worker():
    """
    Dedicated audio worker thread.
    Processes events from _audio_q sequentially.
    TTS engine must be created in this thread (pyttsx3 requirement).
    """
    print(f"[AUDIO] Worker started  "
          f"(TTS={'yes' if _HAVE_TTS else 'NO — pip install pyttsx3'}  "
          f"Tones={'yes' if _HAVE_AUDIO else 'NO — pip install sounddevice numpy'})")
    while True:
        try:
            event = _audio_q.get(timeout=1.0)
        except queue.Empty:
            continue

        if event is None:       # shutdown signal
            break

        etype = event.get('type')

        if etype == 'tone':
            _tone(event['freq'], event['dur'],
                  event.get('vol', 0.7))

        elif etype == 'tts':
            _speak(event['text'])

        elif etype == 'seq':
            for step in event['steps']:
                if step['type'] == 'tone':
                    _tone(step['freq'], step['dur'], step.get('vol', 0.7))
                elif step['type'] == 'horn':
                    _horn(step['dur'], step.get('vol', 0.85))
                elif step['type'] == 'pause':
                    _time.sleep(step['dur'])
                elif step['type'] == 'tts':
                    _speak(step['text'])

        elif etype == 'countdown':
            _do_countdown(event.get('label', ''))


        _audio_q.task_done()

def _q(event):
    """Queue an audio event (non-blocking, fire-and-forget)."""
    _audio_q.put(event)

def audio_start():
    """Start the audio worker thread. Called at server startup."""
    global _audio_thread
    _audio_thread = threading.Thread(target=_audio_worker, daemon=True)
    _audio_thread.start()

# ── Public audio event helpers ───────────────

def audio_prep_start(pilot_names):
    """Announces round/group then pilot names."""
    if not pilot_names:
        return
    if len(pilot_names) == 1:   name_str = pilot_names[0]
    elif len(pilot_names) == 2: name_str = f"{pilot_names[0]} and {pilot_names[1]}"
    else: name_str = ', '.join(pilot_names[:-1]) + f", and {pilot_names[-1]}"

    # Build round/group prefix from live contest state
    rg_prefix = ''
    if _c_round_idx is not None and _c_group_idx is not None:
        try:
            rnd   = contest_rounds[_c_round_idx]
            n_grp = len(rnd['groups'])
            rg_prefix = (f"Round {_c_round_idx+1}, "
                         f"Group {_c_group_idx+1} of {n_grp}. ")
        except Exception:
            pass

    _q({'type':'seq', 'steps':[
        {'type':'tts',   'text':f"{rg_prefix}{name_str}"},
        {'type':'pause', 'dur':0.8},
    ]})

def audio_testing_open():
    """Flight testing period opens."""
    _q({'type':'tts', 'text':"Test flights permitted"})

def audio_nofly_start():
    """No-fly period begins — stand by for working window."""
    _q({'type':'tts', 'text':"No fly — stand by"})

def audio_countdown_10(label):
    """
    10-second spoken countdown into the next phase.

    Uses a single TTS engine call for the entire countdown —
    one engine reinit instead of 10, eliminating the ~150ms
    per-digit overhead that caused the lag.

    The tone before "10" is played first (fast winsound.Beep),
    then the entire count is spoken in one utterance.
    SAPI5 speaks digits rapidly so the whole sequence takes ~4–5s.
    """
    _q({'type':'countdown', 'label': label})

def _do_countdown(label):
    """
    Two-part countdown:
    1. Verbal announcement: "[label] in"
    2. Configurable delay (COUNTDOWN_DELAY_S)
    3. Count: "10, 9, 8, 7, 6, 5, 4, 3, 2, 1"
    4. Horn fires immediately after "1" — no wall-clock sync
    """
    _speak(f"{label} in")
    _time.sleep(COUNTDOWN_DELAY_S)
    _speak("10, 9, 8, 7, 6, 5, 4, 3, 2, 1")
    _horn(2.50)

# Delay between verbal announcement and start of count.
# Adjust this to align the "1" with the window transition.
COUNTDOWN_DELAY_S = 0.0   # seconds — tune as needed

# Named wrappers for each transition
def audio_countdown_to_testing():  audio_countdown_10("Test flights")
def audio_countdown_to_nofly():    audio_countdown_10("No fly")
def audio_countdown_to_window():   audio_countdown_10("Window")
def audio_countdown_to_launch():   audio_countdown_10("Launch")
def audio_nofly_10():              audio_countdown_10("Window")  # legacy alias


MIKE_SMITH_EXCUSES = [
    "He had to wait in line for Flock of Seagulls reunion tickets.",
    "His servo is held together with a Band-Aid and a prayer.",
    "He\'s still reading the manual. For his radio. From 2009.",
    "He spotted a food truck and had to investigate immediately.",
    "His battery is charging. It has been charging since Tuesday.",
    "He needs a moment. His glider and his ego both took a hit on the last landing.",
    "He\'s recalibrating his throw. This is apparently a very lengthy process.",
    "He\'s on hold with tech support. They\'ve been playing the same hold music since 1987.",
    "He left his transmitter in the car. The car is in Ohio.",
    "He\'s waiting for a firmware update. Estimated time remaining: always.",
    "He\'s consulting his flight coach. The flight coach is a golden retriever.",
    "He dropped a screw. The search and rescue operation is ongoing.",
    "He\'s reviewing the tape. There is no tape. He\'s just stalling.",
    "His CG is off. His mental CG is also off.",
    "He\'s still arguing with the wind about the wind direction.",
    "He had to take an urgent call from his therapist. She says he\'s making progress.",
    "He\'s applying a decal. Aerodynamics, apparently.",
    "His launching arm needs more coffee.",
    "He\'s Googling how to fly. Results were not encouraging.",
    "He momentarily forgot which end of the glider is the front.",
    "He\'s updating his LinkedIn to include competitive glider pilot before he\'s allowed to fly.",
    "He\'s in a heated debate with himself about rudder mix. He\'s losing.",
    "He received a text. It was his glider. It quit.",
    "His pre-flight checklist has 47 items. He\'s on item three.",
    "He\'s been asked to please stop explaining launch biomechanics to strangers.",
    "He\'s negotiating with a seagull for exclusive airspace rights.",
    "He\'s rethinking his entire life trajectory. The glider trajectory is secondary.",
    "His hands are full. One holds a glider. The other holds a breakfast burrito. Priorities.",
    "He saw a cloud he didn\'t like the look of and is waiting for it to leave.",
    "He is loading the dishwasher. This is not related. He just remembered he forgot.",
]

def audio_mike_smith():
    """The Mike Smith rule — contest paused with a random excuse."""
    excuse = random.choice(MIKE_SMITH_EXCUSES)
    _q({'type':'tts', 'text': f"The Mike Smith rule has been invoked. "
                               f"{excuse} "
                               f"We will return to your regularly scheduled contest shortly."})

def audio_resume():
    """Contest resuming after pause."""
    _q({'type':'tts', 'text':"Contest resumed"})

def audio_task_set(task_name, task_desc):
    """Announce when operator selects a new task."""
    _q({'type':'tts', 'text':f"{task_name} — {task_desc}"})

def audio_call_to_line(pilot_names):
    """Alias for audio_prep_start — kept for backward compat."""
    audio_prep_start(pilot_names)

def audio_window_start(task_name, window_s):
    """
    Window-open TTS announcement — horn already fired at end of countdown.
    """
    mins = int(window_s // 60)
    win_str = f"{mins} minute{'s' if mins!=1 else ''}" if window_s >= 60 else f"{int(window_s)} seconds"
    _q({'type':'seq', 'steps':[
        {'type':'pause', 'dur':0.3},
        {'type':'tts',   'text':f"Working window — {task_name} — {win_str}"},
    ]})

def audio_window_60():
    """60 seconds remaining — double beep."""
    _q({'type':'tts', 'text':"60 seconds"})

def audio_window_30():
    """30 seconds remaining — urgent triple beep + call."""
    _q({'type':'tts', 'text':"30 seconds"})

def audio_window_10():
    """Superseded by countdown — no-op."""
    pass

def audio_landing_window_open():
    """Working window closed — sustained horn, then land-now call."""
    _q({'type':'seq', 'steps':[
        {'type':'horn',  'dur':2.50},                 # air horn at window close
        {'type':'pause', 'dur':0.15},
        {'type':'tts',   'text':"Window closed — land now — 30 seconds"},
    ]})

def audio_landing_window_10():
    """10-second landing window countdown — queues precise beep countdown."""
    _q({'type':'countdown', 'label':'Land'})

def audio_landing_window_closed():
    """Landing window has closed."""
    _q({'type':'tts', 'text':"Landing window closed"})

def audio_pilot_landed(pilot_name, raw_s, adj_s, task_name):
    """Announce a pilot's landing with their score."""
    name = pilot_name if pilot_name else f"Unit"
    adj_str  = _fmt_time(abs(adj_s))
    sign_str = "minus " if adj_s < 0 else ""
    raw_str  = _fmt_time(raw_s)
    text = (f"{name} — {raw_str} raw — "
            f"adjusted {sign_str}{adj_str}")
    _q({'type':'tts', 'text':text})

def audio_round_complete():
    """All pilots done / window fully closed."""
    _q({'type':'tts', 'text':"Round complete"})

# Task C specific
def audio_taskc_attempt(attempt_num, total_attempts):
    """Signal the start of a Task C attempt — FAI 3-beep signal."""
    _q({'type':'tts', 'text':f"Attempt {attempt_num} of {total_attempts}"})

def audio_taskc_prep(prep_s=60):
    """Start of Task C no-fly period between attempts."""
    _q({'type':'tts', 'text':f"No fly — {prep_s} seconds"})

def audio_taskc_done():
    """All Task C attempts complete."""
    audio_round_complete()


# ── State-diff engine ────────────────────────
# Tracks previous window/taskc state to fire
# audio events exactly once per transition.
# Called from build_state_json every poll cycle.
# ─────────────────────────────────────────────

_prev_audio_state = {
    'win_phase':      None,
    'taskc_phase':    None,
    'taskc_att':      None,
    'task':           None,
    # Prep phase milestones
    'said_testing':      False,
    'said_nofly':        False,
    'said_prep_10':      False,   # countdown into TESTING
    'said_testing_10':   False,   # countdown into NO_FLY
    'said_nofly_10':     False,   # countdown into working window
    # Working window milestones
    'said_60':           False,
    'said_30':           False,
    'said_10':           False,
    'said_lw_10':        False,
    # Task C prep countdown
    'said_taskc_prep_10':False,
}

def audio_tick(win_phase, win_remaining, win_total,
               taskc_phase, taskc_attempt, taskc_total,
               task_id, task_name, task_desc, window_s):
    """
    Called every ~200ms from build_state_json.
    Fires audio events on state transitions and countdowns.
    """
    p = _prev_audio_state
    now_task = task_id

    # ── Task changed ────────────────────────────
    if now_task != p['task'] and p['task'] is not None:
        audio_task_set(task_name, task_desc)
        p.update({'said_60':False,'said_30':False,'said_10':False,
                  'said_lw_10':False})
    p['task'] = now_task

    # ── Non-Task-C window phases ─────────────────
    if task_id != 'C':
        prev = p['win_phase']
        curr = win_phase

        if prev != curr:
            _tkey = ('t', prev, curr)
            # Any →PREP transition means a new group is starting — clear ALL
            # tuple guards unconditionally so Group 2+ fires correctly.
            if curr == 'PREP':
                # Clear all guards on any →PREP transition regardless of previous state
                print(f"[AUDIO] →PREP transition: prev={prev} curr={curr} — clearing guards")
                for k in [k for k in list(p) if isinstance(k, tuple)]: del p[k]

            elif curr == 'TESTING' and prev == 'PREP' and not p.get(('t','PREP','TESTING')):
                p[('t','PREP','TESTING')] = True
                audio_testing_open()
                p['said_nofly'] = False

            elif curr == 'NO_FLY' and prev == 'TESTING' and not p.get(('t','TESTING','NO_FLY')):
                p[('t','TESTING','NO_FLY')] = True
                audio_nofly_start()
                p['said_nofly_10'] = False

            # Working window opens (auto after no-fly, or skip)
            elif curr == 'RUNNING' and prev in ('NO_FLY','READY',None,'CLOSED') and not p.get(('t','x','RUNNING')):
                p[('t','x','RUNNING')] = True
                if prev not in ('NO_FLY',):
                    _active_names = [v for v in pilots.values() if v]
                    audio_prep_start(_active_names)
                audio_window_start(task_name, window_s)
                p.update({'said_60':False,'said_30':False,
                          'said_10':False,'said_lw_10':False})

            elif curr == 'LANDING_WIN' and prev == 'RUNNING' and not p.get(('t','RUNNING','LANDING_WIN')):
                p[('t','RUNNING','LANDING_WIN')] = True
                audio_landing_window_open()
                p['said_lw_10'] = False

            elif curr == 'CLOSED' and prev == 'LANDING_WIN' and not p.get(('t','LANDING_WIN','CLOSED')):
                p[('t','LANDING_WIN','CLOSED')] = True
                audio_landing_window_closed()

        # Countdown during PREP (last 10s → into TESTING)
        if curr == 'PREP' and win_remaining is not None:
            if not p['said_prep_10'] and 9 < win_remaining <= 12:
                audio_countdown_to_testing(); p['said_prep_10'] = True

        # Countdown during TESTING (last 10s → into NO_FLY)
        if curr == 'TESTING' and win_remaining is not None:
            if not p['said_testing_10'] and 9 < win_remaining <= 12:
                audio_countdown_to_nofly(); p['said_testing_10'] = True

        # Countdown during NO_FLY (last 10s → into working window)
        if curr == 'NO_FLY' and win_remaining is not None:
            if not p['said_nofly_10'] and 9 < win_remaining <= 12:
                audio_countdown_to_window(); p['said_nofly_10'] = True

        # Countdown milestones during RUNNING
        if curr == 'RUNNING' and win_remaining is not None:
            if not p['said_60'] and 59 < win_remaining <= 62:
                audio_window_60(); p['said_60'] = True
            if not p['said_30'] and 29 < win_remaining <= 32:
                audio_window_30(); p['said_30'] = True
            if not p['said_10'] and 9 < win_remaining <= 12:
                audio_countdown_to_window(); p['said_10'] = True

        # Countdown during LANDING_WIN
        if curr == 'LANDING_WIN' and win_remaining is not None:
            if not p['said_lw_10'] and 9 < win_remaining <= 12:
                audio_landing_window_10(); p['said_lw_10'] = True

        p['win_phase'] = curr

    # ── Task C phases ────────────────────────────
    # Flight 1: full PREP→TESTING→NO_FLY sequence (same as standard tasks)
    # Flights 2-N: NO_FLY only → countdown → horn → fly → land
    else:
        prev_ph  = p['taskc_phase']
        prev_att = p['taskc_att']
        curr_ph  = taskc_phase
        curr_att = taskc_attempt

        # ── Flight 1: full prep sequence via win_phase ───────────
        if curr_ph == TASKC_READY or prev_ph is None:
            prev_wp = p.get('win_phase')
            curr_wp = win_phase
            if prev_wp != curr_wp:
                _tkey = ('tc', prev_wp, curr_wp)
                if curr_wp == 'PREP' and prev_wp in (None, 'READY', 'CLOSED'):
                    for k in [k for k in list(p) if isinstance(k, tuple) and k[0]=='tc']: del p[k]
                    _active_names = [v for v in pilots.values() if v]
                    audio_prep_start(_active_names)
                    p.update({'said_testing':False,'said_nofly':False,
                              'said_prep_10':False,'said_testing_10':False,
                              'said_nofly_10':False})
                elif curr_wp == 'TESTING' and prev_wp == 'PREP' and not p.get(('tc','PREP','TESTING')):
                    p[('tc','PREP','TESTING')] = True
                    audio_testing_open()
                elif curr_wp == 'NO_FLY' and prev_wp == 'TESTING' and not p.get(('tc','TESTING','NO_FLY')):
                    p[('tc','TESTING','NO_FLY')] = True
                    audio_nofly_start()
                    p['said_nofly_10'] = False
            if curr_wp == 'PREP' and win_remaining is not None:
                if not p.get('said_prep_10') and 9 < win_remaining <= 12:
                    audio_countdown_to_testing(); p['said_prep_10'] = True
            if curr_wp == 'TESTING' and win_remaining is not None:
                if not p.get('said_testing_10') and 9 < win_remaining <= 12:
                    audio_countdown_to_nofly(); p['said_testing_10'] = True
            if curr_wp == 'NO_FLY' and win_remaining is not None:
                if not p.get('said_nofly_10') and 9 < win_remaining <= 12:
                    audio_countdown_to_launch(); p['said_nofly_10'] = True
            p['win_phase'] = curr_wp

        if curr_ph != prev_ph or curr_att != prev_att:
            _tkey2 = ('tc2', prev_ph, curr_ph, curr_att)

            if curr_ph == TASKC_FLYING and not p.get(_tkey2):
                p[_tkey2] = True
                if curr_att == 1:
                    # Flight 1 — horn comes from end of NO_FLY countdown
                    # Just announce the working window duration
                    audio_window_start(task_name, window_s)
                else:
                    # Flights 2-N — horn comes from end of NO_FLY countdown
                    audio_window_start(task_name, window_s)
                p.update({'said_30':False,'said_10':False,
                          'said_lw_10':False,'said_taskc_prep_10':False})

            elif curr_ph == TASKC_LANDING and prev_ph == TASKC_FLYING and not p.get(_tkey2):
                p[_tkey2] = True
                audio_landing_window_open()
                p['said_lw_10'] = False

            elif curr_ph == TASKC_PREP and prev_ph == TASKC_LANDING and not p.get(_tkey2):
                p[_tkey2] = True
                audio_landing_window_closed()
                if curr_att < taskc_total:
                    # Between attempts: announce no-fly then countdown into next flight
                    audio_nofly_start()
                p.update({'said_30':False,'said_10':False,
                          'said_taskc_prep_10':False})

            elif curr_ph == TASKC_DONE and prev_ph == TASKC_PREP and not p.get(_tkey2):
                p[_tkey2] = True
                audio_taskc_done()

        # Countdown during FLYING — 60s, 30s, 10s marks
        if curr_ph == TASKC_FLYING and win_remaining is not None:
            if not p.get('said_60c') and 59 < win_remaining <= 62:
                audio_window_60(); p['said_60c'] = True
            if not p['said_30'] and 29 < win_remaining <= 32:
                audio_window_30(); p['said_30'] = True
            if not p['said_10'] and 9 < win_remaining <= 12:
                audio_countdown_to_window(); p['said_10'] = True

        # Countdown during PREP (between attempts 2-N) — countdown into next flight
        if curr_ph == TASKC_PREP and win_remaining is not None:
            if not p['said_taskc_prep_10'] and 9 < win_remaining <= 12:
                audio_countdown_to_launch(); p['said_taskc_prep_10'] = True

        # Countdown during landing window
        if curr_ph == TASKC_LANDING and win_remaining is not None:
            if not p['said_lw_10'] and 9 < win_remaining <= 12:
                audio_landing_window_10(); p['said_lw_10'] = True

        p['taskc_phase'] = curr_ph
        p['taskc_att']   = curr_att




PILOTS_HTML = '<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">\n<title>F3K — Live Pilots</title>\n<style>\n*{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#0d1117;--surf:#161b22;--bdr:#21262d;--text:#e6edf3;\n  --muted:#8b949e;--cyan:#00e5ff;--good:#00e676;--warn:#ffb300;\n  --danger:#ff3d3d;--purple:#c792ea;--mono:\'Courier New\',monospace;\n  --disp:\'Segoe UI\',system-ui,sans-serif;\n}\nhtml,body{background:var(--bg);color:var(--text);font-family:var(--disp);\n  min-height:100vh;padding:0}\nheader{background:var(--surf);border-bottom:1px solid var(--bdr);\n  padding:10px 16px;display:flex;justify-content:space-between;align-items:center}\n.h-title{font-size:.85rem;font-weight:700;letter-spacing:.2em;\n  text-transform:uppercase;color:var(--cyan)}\n.h-meta{font-family:var(--mono);font-size:.7rem;color:var(--muted);\n  display:flex;gap:14px;align-items:center}\n.h-task{color:var(--text);font-weight:600}\n.win-bar{padding:8px 16px;background:#0a0c0f;border-bottom:1px solid var(--bdr);\n  display:flex;align-items:center;gap:12px}\n.win-phase{font-family:var(--mono);font-size:.72rem;font-weight:700;\n  letter-spacing:.15em;padding:2px 8px;border-radius:2px}\n.win-phase.RUNNING{color:var(--good);border:1.5px solid rgba(0,230,118,.3)}\n.win-phase.LANDING_WIN{color:var(--warn);border:1.5px solid rgba(255,179,0,.4)}\n.win-phase.PREP,.win-phase.TESTING,.win-phase.NO_FLY{\n  color:var(--muted);border:1.5px solid var(--bdr)}\n.win-phase.CLOSED,.win-phase.READY{color:var(--muted);border:1.5px solid var(--bdr)}\n.win-time{font-family:var(--mono);font-size:1.1rem;font-weight:700;\n  color:var(--text);min-width:70px}\n.win-prog{flex:1;height:4px;background:var(--bdr);border-radius:2px;overflow:hidden}\n.win-prog-fill{height:100%;border-radius:2px;transition:width .8s linear;\n  background:var(--good)}\n.win-prog-fill.warn{background:var(--warn)}\n.win-prog-fill.crit{background:var(--danger)}\n.grid{display:grid;\n  grid-template-columns:repeat(auto-fill,minmax(300px,1fr));\n  gap:12px;padding:14px}\n.card{background:var(--surf);border:2px solid var(--bdr);border-radius:8px;box-shadow:0 1px 4px rgba(0,0,0,.08);\n  overflow:hidden;transition:border-color .3s}\n.card.flying{border-color:rgba(0,229,255,.5);\n  box-shadow:0 0 12px rgba(0,229,255,.08)}\n.card.landed{border-color:rgba(0,230,118,.3)}\n.card-top{display:flex;align-items:center;gap:10px;padding:12px 14px 8px}\n.unit-num{font-family:var(--mono);font-size:.7rem;color:var(--muted);\n  background:#0a0c0f;border:1.5px solid var(--bdr);border-radius:3px;\n  padding:2px 6px;min-width:28px;text-align:center}\n.pilot-name{flex:1;font-size:1.05rem;font-weight:700;color:var(--text);\n  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n.state-badge{font-family:var(--mono);font-size:.62rem;font-weight:700;\n  letter-spacing:.12em;padding:3px 7px;border-radius:2px}\n.state-badge.GROUND{color:var(--muted);border:1.5px solid var(--bdr)}\n.state-badge.FLYING,.state-badge.LAUNCH-WIN{\n  color:var(--cyan);border:1.5px solid rgba(0,229,255,.4);\n  animation:pulse .8s ease-in-out infinite alternate}\n.state-badge.LANDED{color:var(--good);border:1.5px solid rgba(0,230,118,.3)}\n@keyframes pulse{from{opacity:.7}to{opacity:1}}\n.card-vitals{padding:4px 14px 12px;display:grid;\n  grid-template-columns:1fr 1fr;gap:6px 14px}\n.vital{display:flex;flex-direction:column;gap:1px}\n.vital-label{font-family:var(--mono);font-size:.58rem;color:var(--muted);\n  letter-spacing:.1em;text-transform:uppercase}\n.vital-val{font-family:var(--mono);font-size:.95rem;font-weight:700;color:var(--text)}\n.vital-val.hi{color:var(--cyan)}\n.vital-val.good{color:var(--good)}\n.vital-val.dim{color:var(--muted)}\n.batt{height:3px;background:var(--bdr);border-radius:2px;margin:8px 14px 4px;\n  overflow:hidden}\n.batt-fill{height:100%;border-radius:2px}\n.no-pilots{text-align:center;padding:60px 20px;color:var(--muted);\n  font-family:var(--mono);font-size:.8rem;letter-spacing:.15em}\nfooter{text-align:center;padding:10px;font-family:var(--mono);\n  font-size:.6rem;color:var(--bdr);letter-spacing:.1em}\n</style>\n</head>\n<body>\n<header>\n  <div class="h-title">F3K Live</div>\n  <div class="h-meta">\n    <span class="h-task" id="task-name">—</span>\n    <span id="session-name"></span>\n    <span id="clock"></span>\n  </div>\n</header>\n<div class="win-bar" id="win-bar">\n  <span class="win-phase READY" id="win-phase">READY</span>\n  <span class="win-time" id="win-time">—</span>\n  <div class="win-prog"><div class="win-prog-fill" id="win-fill" style="width:0%;transition:none"></div></div>\n</div>\n<div class="grid" id="grid">\n  <div class="no-pilots">WAITING FOR DATA</div>\n</div>\n<footer>F3K SCORING SYSTEM · AUTO-REFRESH</footer>\n\n<script>\nfunction fmtTime(s){\n  if(s===null||s===undefined) return \'—\';\n  s=Math.max(0,s);\n  const m=Math.floor(s/60),sec=Math.floor(s%60);\n  return m+\':\'+(sec<10?\'0\':\'\')+sec;\n}\nfunction fmtVal(v,dec,unit){\n  if(v===null||v===undefined||v===0) return \'—\';\n  return v.toFixed(dec)+(unit||\'\');\n}\n\nasync function refresh(){\n  try{\n    const r = await fetch(\'/state\');\n    if(!r.ok) return;\n    const d = await r.json();\n    document.getElementById(\'task-name\').textContent =\n      (d.task_info?.name||\'—\') + \' — \' + (d.task_info?.desc||\'\');\n    document.getElementById(\'session-name\').textContent = d.session_name||\'\';\n    document.getElementById(\'clock\').textContent = d.timestamp||\'\';\n\n    // Window bar\n    const ph   = d.win_phase||\'READY\';\n    const phEl = document.getElementById(\'win-phase\');\n    phEl.textContent = ph.replace(\'_\',\' \');\n    phEl.className   = \'win-phase \' + ph.replace(\'_WIN\',\'\');\n    document.getElementById(\'win-time\').textContent = fmtTime(d.win_remaining);\n    const pct = d.win_total>0 ? Math.max(0,(d.win_remaining||0)/d.win_total*100) : 0;\n    const fill = document.getElementById(\'win-fill\');\n    fill.style.width = pct+\'%\';\n    fill.className   = \'win-prog-fill\' +\n      (d.win_remaining<30?\' crit\':d.win_remaining<60?\' warn\':\'\');\n\n    // Pilot cards\n    const units = d.units||[];\n    if(!units.length){\n      document.getElementById(\'grid\').innerHTML =\n        \'<div class="no-pilots">NO ACTIVE GROUP</div>\';\n      return;\n    }\n    // Sort: flying first, then landed, then ground\n    const order = u => u.state===2?0:u.state===3?1:2;\n    const sorted = [...units].sort((a,b)=>order(a)-order(b));\n\n    document.getElementById(\'grid\').innerHTML = sorted.map(u=>{\n      const st = u.state;\n      const stName = st===2?\'FLYING\':st===1?\'LAUNCH WIN\':st===3?\'LANDED\':\'GROUND\';\n      const stCls  = st===2?\'FLYING\':st===1?\'LAUNCH-WIN\':st===3?\'LANDED\':\'GROUND\';\n      const cardCls= st===2||st===1?\'flying\':st===3?\'landed\':\'\';\n      const alt    = st===2||st===1 ? fmtVal(u.altitude_ft,1,\'ft\') : \'—\';\n      const dur    = st===2||st===1 ? fmtTime(u.duration_s) : \'—\';\n      const best   = u.best_adjusted_s!==null && u.best_adjusted_s!==undefined\n                     ? fmtVal(u.best_adjusted_s,1,\'s\') : \'—\';\n      const bpct   = u.battery_pct||0;\n      const bcol   = bpct<20?\'#ff3d3d\':bpct<40?\'#ffb300\':\'#00e676\';\n\n      return \'<div class="card \'+cardCls+\'">\'+\n        \'<div class="card-top">\'+\n          \'<span class="unit-num">\'+String(u.id).padStart(2,\'0\')+\'</span>\'+\n          \'<span class="pilot-name">\'+(u.pilot_name||(\'Unit \'+u.id))+\'</span>\'+\n          \'<span class="state-badge \'+stCls+\'">\'+stName+\'</span>\'+\n        \'</div>\'+\n        \'<div class="card-vitals">\'+\n          \'<div class="vital"><span class="vital-label">Altitude</span>\'+\n            \'<span class="vital-val hi">\'+alt+\'</span></div>\'+\n          \'<div class="vital"><span class="vital-label">Flight Time</span>\'+\n            \'<span class="vital-val hi">\'+dur+\'</span></div>\'+\n          \'<div class="vital"><span class="vital-label">Best Score</span>\'+\n            \'<span class="vital-val good">\'+best+\'</span></div>\'+\n          \'<div class="vital"><span class="vital-label">Flights</span>\'+\n            \'<span class="vital-val dim">\'+u.flight_count+\'</span></div>\'+\n        \'</div>\'+\n        \'<div class="batt"><div class="batt-fill" style="width:\'+bpct+\'%;background:\'+bcol+\'"></div></div>\'+\n      \'</div>\';\n    }).join(\'\');\n  }catch(e){ console.error(e); }\n}\n\nrefresh();\nsetInterval(refresh, 1500);\n</script>\n</body>\n</html>'

STANDINGS_HTML = '<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">\n<title>F3K — Standings</title>\n<style>\n*{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#0d1117;--surf:#161b22;--bdr:#21262d;--text:#e6edf3;\n  --muted:#8b949e;--cyan:#00e5ff;--good:#00e676;--warn:#ffb300;\n  --danger:#ff3d3d;--mono:\'Courier New\',monospace;\n  --disp:\'Segoe UI\',system-ui,sans-serif;\n}\nhtml,body{background:var(--bg);color:var(--text);font-family:var(--disp);min-height:100vh}\nheader{background:var(--surf);border-bottom:1px solid var(--bdr);\n  padding:10px 18px;display:flex;justify-content:space-between;align-items:center}\n.h-title{font-size:.85rem;font-weight:700;letter-spacing:.2em;\n  text-transform:uppercase;color:var(--cyan)}\n.h-right{font-family:var(--mono);font-size:.7rem;color:var(--muted);\n  display:flex;gap:14px;align-items:center}\n.h-task{color:var(--text);font-weight:600}\n.status-bar{padding:7px 18px;background:#0a0c0f;border-bottom:1px solid var(--bdr);\n  font-family:var(--mono);font-size:.68rem;color:var(--muted);\n  display:flex;gap:16px;align-items:center}\n.status-pill{padding:2px 8px;border-radius:2px;border:1.5px solid var(--bdr)}\n.status-pill.active{color:var(--warn);border-color:rgba(255,179,0,.35)}\n.status-pill.done{color:var(--good);border-color:rgba(0,230,118,.3)}\n.wrap{padding:14px 18px;max-width:960px;margin:0 auto;overflow-x:auto}\ntable{width:100%;border-collapse:collapse;min-width:420px}\nthead tr{background:#0a0c0f}\nth{font-family:var(--mono);font-size:.58rem;font-weight:700;letter-spacing:.12em;\n  color:var(--muted);text-transform:uppercase;padding:6px 10px;\n  border-bottom:1px solid var(--bdr);text-align:right;white-space:nowrap}\nth:nth-child(1),th:nth-child(2){text-align:left}\nth.total-col{color:var(--cyan)}\ntd{font-family:var(--mono);font-size:.78rem;padding:7px 10px;\n  border-bottom:1px solid rgba(33,38,45,.5);text-align:right;white-space:nowrap}\ntd:nth-child(1){text-align:center;font-size:1rem}\ntd:nth-child(2){text-align:left;font-size:.85rem;font-weight:600;\n  font-family:var(--disp);white-space:nowrap;max-width:160px;\n  overflow:hidden;text-overflow:ellipsis}\ntr:last-child td{border-bottom:none}\ntr:nth-child(even) td{background:rgba(255,255,255,.012)}\n.total{color:var(--cyan);font-weight:700;font-size:.88rem}\n.pct{color:var(--muted)}\n.pct.leader{color:var(--good)}\n.drop{color:var(--muted);text-decoration:line-through}\n.medal-1{color:gold}.medal-2{color:silver}.medal-3{color:#cd7f32}\n.medal-n{color:var(--muted)}\n.nodata{text-align:center;padding:60px;color:var(--muted);\n  font-family:var(--mono);font-size:.8rem;letter-spacing:.15em}\nfooter{text-align:center;padding:10px;font-family:var(--mono);\n  font-size:.6rem;color:var(--bdr);letter-spacing:.1em}\n</style>\n</head>\n<body>\n<header>\n  <div class="h-title">F3K Standings</div>\n  <div class="h-right">\n    <span class="h-task" id="contest-name">—</span>\n    <span id="clock"></span>\n  </div>\n</header>\n<div class="status-bar" id="status-bar">\n  <span id="round-status">—</span>\n</div>\n<div class="wrap">\n  <div id="table-wrap"><div class="nodata">WAITING FOR DATA</div></div>\n</div>\n<footer>F3K SCORING SYSTEM · AUTO-REFRESH</footer>\n\n<script>\nasync function refresh(){\n  try{\n    const r = await fetch(\'/state\');\n    if(!r.ok) return;\n    const d = await r.json();\n    const ct = d.contest||{};\n    const st = ct.standings||{};\n    const rnds = ct.rounds||[];\n\n    document.getElementById(\'contest-name\').textContent = ct.contest?.name||\'F3K\';\n    document.getElementById(\'clock\').textContent = d.timestamp||\'\';\n\n    // Status bar\n    const nDone = rnds.filter(r=>r.status===\'complete\').length;\n    const nTot  = rnds.length;\n    const actR  = rnds.find(r=>r.status!==\'complete\'&&r.n_complete>0);\n    let statusHtml = `<span>${nDone} of ${nTot} rounds complete</span>`;\n    if(actR){\n      statusHtml += `<span class="status-pill active">▶ R${actR.round_num} G${actR.n_complete+1}/${actR.n_groups} flying</span>`;\n    }\n    document.getElementById(\'status-bar\').innerHTML = statusHtml;\n\n    const names = Object.keys(st).sort((a,b)=>(st[b].total||0)-(st[a].total||0));\n    if(!names.length){\n      document.getElementById(\'table-wrap\').innerHTML=\n        \'<div class="nodata">NO COMPLETED ROUNDS YET</div>\';\n      return;\n    }\n\n    const leaderTotal = st[names[0]]?.total||0;\n    const completedRnds = rnds.filter(r=>r.status===\'complete\');\n\n    const headers = completedRnds.map(r=>\n      `<th>R${r.round_num}<br><span style="font-weight:700;opacity:.55">${r.task}</span></th>`\n    ).join(\'\');\n\n    const medalCls = i => i===0?\'medal-1\':i===1?\'medal-2\':i===2?\'medal-3\':\'medal-n\';\n    const medal    = i => i===0?\'①\':i===1?\'②\':i===2?\'③\':(i+1);\n\n    const rows = names.map((name,rank)=>{\n      const s = st[name];\n      const dropped = new Set(s.dropped||[]);\n      const pct = leaderTotal>0 ? (s.total/leaderTotal*100).toFixed(1)+\'%\' : \'—\';\n      const cells = s.rounds.map((v,i)=>{\n        if(!rnds[i]||rnds[i].status!==\'complete\') return \'\';\n        if(v===null) return \'<td style="color:var(--muted)">—</td>\';\n        return `<td class="${dropped.has(i)?\'drop\':\'\'}">${v.toFixed(0)}</td>`;\n      }).join(\'\');\n      return `<tr>\n        <td><span class="${medalCls(rank)}">${medal(rank)}</span></td>\n        <td>${name}</td>\n        <td class="total">${s.total.toFixed(0)}</td>\n        <td class="pct${rank===0?\' leader\':\'\'}">${rank===0?\'100.0%\':pct}</td>\n        ${cells}\n      </tr>`;\n    }).join(\'\');\n\n    document.getElementById(\'table-wrap\').innerHTML =\n      `<table>\n        <thead><tr>\n          <th>#</th><th>Pilot</th>\n          <th class="total-col">Total</th><th>%</th>\n          ${headers}\n        </tr></thead>\n        <tbody>${rows}</tbody>\n      </table>`;\n  }catch(e){ console.error(e); }\n}\nrefresh();\nsetInterval(refresh, 3000);\n</script>\n</body>\n</html>'

DRAWS_HTML = '<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">\n<title>F3K — Draws</title>\n<style>\n*{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#0d1117;--surf:#161b22;--bdr:#21262d;--text:#e6edf3;\n  --muted:#8b949e;--cyan:#00e5ff;--good:#00e676;--warn:#ffb300;\n  --danger:#ff3d3d;--mono:\'Courier New\',monospace;\n  --disp:\'Segoe UI\',system-ui,sans-serif;\n}\nhtml,body{background:var(--bg);color:var(--text);font-family:var(--disp);min-height:100vh}\nheader{background:var(--surf);border-bottom:1px solid var(--bdr);\n  padding:10px 18px;display:flex;justify-content:space-between;align-items:center}\n.h-title{font-size:.85rem;font-weight:700;letter-spacing:.2em;\n  text-transform:uppercase;color:var(--cyan)}\n.h-meta{font-family:var(--mono);font-size:.7rem;color:var(--muted)}\n.wrap{padding:16px 18px;max-width:960px;margin:0 auto}\n.round-block{margin-bottom:28px}\n.round-hdr{display:flex;align-items:baseline;gap:12px;margin-bottom:10px;\n  padding-bottom:6px;border-bottom:1px solid var(--bdr)}\n.round-num{font-family:var(--mono);font-size:.72rem;font-weight:700;\n  letter-spacing:.2em;color:var(--muted)}\n.round-task{font-size:1.0rem;font-weight:700;color:var(--text)}\n.round-desc{font-size:.78rem;color:var(--muted)}\n.round-status{font-family:var(--mono);font-size:.62rem;padding:2px 7px;\n  border-radius:2px;margin-left:auto}\n.round-status.complete{color:var(--good);border:1.5px solid rgba(0,230,118,.3)}\n.round-status.active{color:var(--warn);border:1.5px solid rgba(255,179,0,.35);\n  animation:pulse .9s ease-in-out infinite alternate}\n.round-status.planned{color:var(--muted);border:1.5px solid var(--bdr)}\n@keyframes pulse{from{opacity:.6}to{opacity:1}}\n.groups-row{display:flex;flex-wrap:wrap;gap:12px}\n.group-card{background:var(--surf);border:1.5px solid var(--bdr);\n  border-radius:5px;overflow:hidden;min-width:200px;flex:1}\n.group-card.active{border-color:rgba(255,179,0,.4)}\n.group-card.complete{border-color:rgba(0,230,118,.2)}\n.group-hdr{background:#0d1117;padding:6px 12px;display:flex;\n  align-items:center;justify-content:space-between}\n.group-lbl{font-family:var(--mono);font-size:.68rem;font-weight:700;\n  letter-spacing:.15em;color:var(--muted)}\n.group-status{font-family:var(--mono);font-size:.6rem;\n  color:var(--good)}\n.pilot-row{display:grid;grid-template-columns:28px 1fr;\n  gap:6px;padding:5px 12px;border-bottom:1px solid rgba(33,38,45,.5);\n  align-items:center}\n.pilot-row:last-child{border-bottom:none}\n.p-unit{font-family:var(--mono);font-size:.7rem;color:var(--cyan);\n  font-weight:700}\n.p-name{font-size:.85rem;font-weight:600;color:var(--text)}\n.nodata{text-align:center;padding:60px;color:var(--muted);\n  font-family:var(--mono);font-size:.8rem;letter-spacing:.15em}\nfooter{text-align:center;padding:10px;font-family:var(--mono);\n  font-size:.6rem;color:var(--bdr);letter-spacing:.1em}\n</style>\n</head>\n<body>\n<header>\n  <div class="h-title">F3K — Draws</div>\n  <div class="h-meta" id="meta">—</div>\n</header>\n<div class="wrap" id="wrap">\n  <div class="nodata">LOADING...</div>\n</div>\n<footer>F3K SCORING SYSTEM · AUTO-REFRESH</footer>\n\n<script>\nasync function refresh(){\n  try{\n    const r = await fetch(\'/state\');\n    if(!r.ok) return;\n    const d = await r.json();\n    const ct = d.contest || {};\n    const rounds = ct.rounds || [];\n    document.getElementById(\'meta\').textContent =\n      (ct.contest?.name||\'F3K\') + \'  ·  \' + (d.timestamp||\'\');\n\n    if(!rounds.length){\n      document.getElementById(\'wrap\').innerHTML =\n        \'<div class="nodata">NO CONTEST PLAN YET</div>\';\n      return;\n    }\n\n    const taskMap = {};\n    (d.task_list||[]).forEach(t=>{ taskMap[t.id]={name:t.name,desc:t.desc}; });\n\n    // Fetch full group data from contest state via /state contest.rounds\n    // We need the full groups with pilots — these are in ct.rounds but\n    // the compact version only has n_groups/n_complete. Fetch /contest_full.\n    // Actually: the full groups are in d.contest.rounds from /state if we\n    // include them. Since we don\\\'t (compact), fetch a dedicated endpoint.\n    const cr = await fetch(\'/contest_data\');\n    const cd = cr.ok ? await cr.json() : {rounds:[]};\n    const fullRounds = cd.rounds || [];\n\n    document.getElementById(\'wrap\').innerHTML = fullRounds.map((rnd, ri)=>{\n      const ti = taskMap[rnd.task] || {name:rnd.task,desc:\'\'};\n      const statusCls = rnd.status;\n      const statusLbl = rnd.status===\'complete\'?\'COMPLETE\':\n        rnd.status===\'active\'?\'ACTIVE\':\'PLANNED\';\n      const groups = rnd.groups || [];\n\n      if(!groups.length){\n        return `<div class="round-block">\n          <div class="round-hdr">\n            <span class="round-num">ROUND ${rnd.round_num}</span>\n            <span class="round-task">${ti.name}</span>\n            <span class="round-desc">${ti.desc}</span>\n            <span class="round-status ${statusCls}">${statusLbl}</span>\n          </div>\n          <div style="font-family:var(--mono);font-size:.72rem;color:var(--muted);\n            padding:8px 0">Groups not yet drawn</div>\n        </div>`;\n      }\n\n      const groupCards = groups.map(g=>{\n        const gCls = g.status===\'complete\'?\'complete\':\n          g.status===\'active\'?\'active\':\'\';\n        const pilots = Object.entries(g.pilots||{})\n          .sort((a,b)=>parseInt(a[0])-parseInt(b[0]));\n        return `<div class="group-card ${gCls}">\n          <div class="group-hdr">\n            <span class="group-lbl">GROUP ${g.group_num}</span>\n            ${g.status===\'complete\'?\'<span class="group-status">✓</span>\':\'\'}\n            ${g.status===\'active\'?\'<span class="group-status" style="color:var(--warn)">▶ FLYING</span>\':\'\'}\n          </div>\n          ${pilots.map(([uid,name])=>`\n            <div class="pilot-row">\n              <span class="p-unit">${String(parseInt(uid)).padStart(2,\'0\')}</span>\n              <span class="p-name">${name}</span>\n            </div>`).join(\'\')}\n        </div>`;\n      }).join(\'\');\n\n      return `<div class="round-block">\n        <div class="round-hdr">\n          <span class="round-num">ROUND ${rnd.round_num}</span>\n          <span class="round-task">${ti.name}</span>\n          <span class="round-desc">${ti.desc}</span>\n          <span class="round-status ${statusCls}">${statusLbl}</span>\n        </div>\n        <div class="groups-row">${groupCards}</div>\n      </div>`;\n    }).join(\'\');\n  }catch(e){ console.error(e); }\n}\nrefresh();\nsetInterval(refresh, 8000);\n</script>\n</body>\n</html>'

# ─────────────────────────────────────────────
#  Simulator — task-aware, window-driven v2
#  Respects landing windows and Task C attempt cycles
# ─────────────────────────────────────────────

TASK_FLIGHT_TARGETS = {
    'LL': [60,60,60,60,60],
    'A':  [420],
    'B':  [240,240],
    'C':  [180,180,180,180,180],   # trimmed to num_attempts at runtime
    'D':  [300,300],
    'E':  [150,150,150],
    'F':  [180,180,180],
    'G':  [120,120,120,120,120],
    'H':  [240,180,120,60],
    'I':  [200,200,200],
    'J':  [180,180,180],
    'K':  [60,90,120,150,180],
    'L':  [540],
    'M':  [180,300,420],
    'N':  [540],
}

LAUNCH_WIN_S = 5.0

def sim_plan(task_id, num_attempts=3):
    targets = TASK_FLIGHT_TARGETS.get(task_id, [60])
    if task_id == 'C':
        targets = targets[:num_attempts]
    return [max(5.0, t * random.uniform(0.75, 1.25)) for t in targets]

def sim_lh():
    return random.uniform(15.0, 60.0)

def make_sim(uid, now):
    return {
        'uid':uid, 'state':'WAITING', 'alt':0.0, 'peak':0.0,
        'lh':0.0, 'lt':None, 'fs':60.0, 'flight_idx':0,
        'plan':[], 'launch_delay':0.0, 'task_done':False, 'batt':100,
        'lh_target':40.0, 'landed_at':None,
    }

async def run_sim(udp_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print("[SIM] 8 virtual pilots — task-aware window-driven v2")

    SIM_COUNT = 8
    sim = {uid: make_sim(uid, time.time()) for uid in range(1, SIM_COUNT+1)}

    # Use active pilot assignments if available (from roster draw),
    # otherwise fall back to default names so sim works standalone
    default_names = {
        1:'Alice', 2:'Bob',    3:'Carlos', 4:'Dana',
        5:'Eli',   6:'Fiona',  7:'George', 8:'Hannah',
    }
    for uid in range(1, SIM_COUNT+1):
        if uid not in pilots:
            pilots[uid] = default_names[uid]

    last_task   = None
    last_window = None
    last_taskc  = None   # detect Task C phase changes

    def send_pkt(uid, pkt_state, s, el_s):
        a10  = int(max(0.0, s['alt'])*10)
        d10  = int(el_s*10)
        pk   = int(s['peak'])
        lh10 = int(s['lh']*10)
        ts   = int((time.time()-server_start)*1000) & 0xFFFFFFFF
        buf  = bytearray(PACKET_SIZE)
        buf[0]=uid; buf[1]=pkt_state
        buf[2]=(a10>>8)&0xFF; buf[3]=a10&0xFF
        buf[4]=(ts>>24)&0xFF; buf[5]=(ts>>16)&0xFF
        buf[6]=(ts>>8)&0xFF;  buf[7]=ts&0xFF
        buf[8]=(d10>>8)&0xFF; buf[9]=d10&0xFF
        buf[10]=min(255,pk)
        buf[11]=(lh10>>8)&0xFF; buf[12]=lh10&0xFF
        buf[13]=s['batt']
        sock.sendto(bytes(buf),('127.0.0.1',udp_port))

    def replan_all():
        num_att = taskc_state['num_attempts'] if active_task=='C' else 3
        for uid,s in sim.items():
            plan = sim_plan(active_task, num_att)
            s.update({'plan':plan,'flight_idx':0,'task_done':False,
                      'state':'WAITING','alt':0.0,'peak':0.0,'lh':0.0,
                      'lt':None,'landed_at':None,
                      'launch_delay': random.uniform(1.0,6.0),
                      'batt': max(0,100-int((time.time()-server_start)/36))})

    while True:
        now = time.time()

        # Detect task changes
        task_changed   = (active_task != last_task)
        window_started = (window_start_time is not None and window_start_time != last_window)
        # Detect Task C new attempt
        tc_ph = taskc_state['phase'] if active_task=='C' else None
        tc_att = taskc_state['attempt'] if active_task=='C' else 0
        taskc_changed = (active_task=='C' and tc_ph==TASKC_FLYING and
                         (last_taskc is None or last_taskc != tc_att))

        if task_changed or window_started:
            last_task   = active_task
            last_window = window_start_time
            last_taskc  = None
            replan_all()

        if taskc_changed:
            last_taskc = tc_att
            # New attempt: reset sim pilots to WAITING with short delay
            for s in sim.values():
                if s['state'] in ('DONE','LANDED') or s['task_done']:
                    pass  # leave done pilots done
                else:
                    s.update({'state':'WAITING','alt':0.0,'peak':0.0,
                               'lh':0.0,'lt':None,'landed_at':None,
                               'launch_delay': random.uniform(0.5, 3.0)})

        # ── Determine window end for landing deadline ──
        if active_task == 'C':
            ph = taskc_state['phase']
            phase_start = taskc_state['phase_start']
            if ph == TASKC_FLYING and phase_start:
                window_end = phase_start + TASK_C_CAP_S
            elif ph == TASKC_LANDING and phase_start:
                window_end = phase_start + LANDING_WINDOW_S
            else:
                window_end = None
        else:
            if window_start_time and active_window_s > 0:
                window_end = window_start_time + active_window_s + LANDING_WINDOW_S
            else:
                window_end = None

        for uid,s in sim.items():
            s['batt'] = max(0, 100-int((now-server_start)/36))
            pkt_state = STATE_GROUND
            el_s = 0.0

            if s['state'] == 'WAITING':
                s['alt'] = random.uniform(-0.1,0.2)
                # Check if we should launch
                can_launch = False
                if active_task == 'C':
                    # Launch only during FLYING phase
                    can_launch = (tc_ph == TASKC_FLYING and
                                  taskc_state['phase_start'] is not None and
                                  not s['task_done'] and
                                  s['flight_idx'] < len(s['plan']) and
                                  now >= taskc_state['phase_start'] + s['launch_delay'])
                else:
                    can_launch = (window_start_time is not None and
                                  not s['task_done'] and
                                  s['flight_idx'] < len(s['plan']) and
                                  now >= window_start_time + s['launch_delay'])

                if can_launch:
                    # Don't launch if window is closing soon and flight won't fit
                    ft = s['plan'][s['flight_idx']]
                    deadline = window_end or (now + ft * 2)
                    if now + ft * 0.4 < deadline:
                        s.update({'state':'LAUNCH_WIN','lt':now,
                                  'peak':0.0,'lh':0.0,
                                  'lh_target':sim_lh()})

            elif s['state'] == 'LAUNCH_WIN':
                el   = now - s['lt']
                frac = min(1.0, el/LAUNCH_WIN_S)
                lht  = s.get('lh_target',40.0)
                alt  = lht * math.sin(math.pi*frac*0.5) * (1+random.uniform(-0.03,0.03))
                s['alt']  = max(0.0, alt)
                s['lh']   = max(s['lh'], s['alt'])
                s['peak'] = max(s['peak'], s['alt'])
                pkt_state = STATE_LAUNCH_WINDOW
                el_s = el

                if el >= LAUNCH_WIN_S:
                    # Compute actual flight duration — cap to land before window+30s
                    ft = s['plan'][s['flight_idx']]
                    if window_end:
                        max_ft = (window_end - now) - 2.0   # 2s buffer
                        ft = min(ft, max(3.0, max_ft))
                    s['fs']    = ft
                    s['state'] = 'FLIGHT'

            elif s['state'] == 'FLIGHT':
                el = now - s['lt'] - LAUNCH_WIN_S
                if el < 0: el = 0
                frac = min(1.0, el/s['fs'])
                alt  = 50.0*math.sin(math.pi*frac)*(1+random.uniform(-0.04,0.04))
                s['alt']  = max(0.0, alt)
                s['peak'] = max(s['peak'], s['alt'])
                pkt_state = STATE_FLIGHT
                el_s = el

                # Force land if window is closing
                if window_end and now > window_end - 1.0:
                    s['fs'] = el  # land now

                if el >= s['fs']:
                    s['state'] = 'LANDED'; s['alt'] = 0.0
                    s['landed_at'] = now
                    el_s = el

            elif s['state'] == 'LANDED':
                pkt_state = STATE_LANDED
                el_s = (now - s['lt'] - LAUNCH_WIN_S) if s['lt'] else 0.0
                el_s = max(0.0, el_s)

                rest = 3.0 if active_task=='C' else random.uniform(5.0,12.0)
                if s['landed_at'] and (now - s['landed_at']) > rest:
                    s['flight_idx'] += 1
                    more = s['flight_idx'] < len(s['plan'])
                    # For Task C: wait for next FLYING phase
                    if active_task == 'C':
                        s['state'] = 'DONE' if not more else 'WAITING'
                        s['launch_delay'] = random.uniform(0.5, 3.0)
                    else:
                        time_ok = (window_end is None or now < window_end - 10)
                        if more and time_ok:
                            s['state'] = 'WAITING'
                            s['launch_delay'] = (now - window_start_time +
                                                 random.uniform(3.0,8.0)) if window_start_time else 0
                        else:
                            s['state'] = 'DONE'; s['task_done'] = True
                    s['lt'] = None; s['alt'] = 0.0; s['peak'] = 0.0; s['lh'] = 0.0

            elif s['state'] == 'DONE':
                s['alt'] = random.uniform(-0.1,0.1)
                pkt_state = STATE_GROUND

            send_pkt(uid, pkt_state, s, el_s)

        await asyncio.sleep(0.2)



# ─────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────
async def main_async(args):
    _load_retry_queue()
    Thread(target=_summary_timeout_worker, daemon=True).start()
    Thread(target=_log_retry_worker, daemon=True).start()
    Thread(target=run_http, args=(args.http_port,), daemon=True).start()
    await start_udp('0.0.0.0', args.udp_port)
    await start_sensor_udp('0.0.0.0')
    if args.sim:
        await run_sim(args.udp_port)
    else:
        print("[SERVER] Waiting for flight units...")
        await asyncio.Event().wait()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--udp-port',  type=int, default=UDP_PORT)
    parser.add_argument('--http-port', type=int, default=HTTP_PORT)
    parser.add_argument('--sim',       action='store_true')
    parser.add_argument('--no-audio',  action='store_true',
                        help='Disable audio output')
    args = parser.parse_args()

    if args.no_audio:
        audio_tick._disabled = True
        print("[AUDIO] Disabled via --no-audio")
    else:
        audio_start()

    print("="*44)
    print("  F3K Scoring Server  v4.1")
    print(f"  Session:   {SESSION_NAME}")
    print(f"  UDP:       {args.udp_port}  (scoring)")
    print(f"  UDP:       {GPS_PORT}   (GPS)")
    print(f"  UDP:       {FLIGHT_PORT}   (flight metrics)")
    print(f"  HTTP:      {args.http_port}")
    print(f"  Penalty:   {PENALTY_PER_FOOT}s/ft")
    print(f"  Broadcast: {BROADCAST_ADDR}")
    print(f"  Log retry: every {LOG_RETRY_INTERVAL_S}s (indefinite)")
    print(f"  Score wait: {SUMMARY_TIMEOUT_S}s summary timeout")
    print(f"  Log delay:  {LOG_FETCH_DELAY_S}s post-announcement fetch delay")
    print(f"  Tasks:     All 14 FAI + Low Launch")
    print(f"  Landing:   30s window enforced")
    print(f"  Task C:    3 or 5 attempts, per-attempt scoring")
    tones_ok = _HAVE_WINSOUND or _HAVE_AUDIO
    print(f"  Audio:     {'OFF' if args.no_audio else ('ON' if (_HAVE_TTS and tones_ok) else 'MISSING LIBS')}")
    if not args.no_audio and _HAVE_WINSOUND:
        print(f"  Tones:     winsound (Windows native — fast)")
    elif not args.no_audio and _HAVE_AUDIO:
        print(f"  Tones:     sounddevice+numpy")
    print(f"  Sim:       {'ON' if args.sim else 'OFF'}")
    print("="*44)
    print(f"  Spectator:   http://[ip]:{args.http_port}/pilots")
    print(f"  Standings:   http://[ip]:{args.http_port}/standings")
    print(f"  Draws:       http://[ip]:{args.http_port}/draws")
    print("="*44+"\n")

    load_contest()

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\n[SERVER] Stopped.")
