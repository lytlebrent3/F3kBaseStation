"""
F3XVault Push Pipeline Diagnostic
Run from F3K_Ground_System_Prototype folder.
Tests each step independently and reports exactly what F3XVault sees.

Usage: python3 vault_push_diag.py 4365
"""
import json, sys, urllib.request, urllib.parse, time

EVENT_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 4365
BASE = "https://www.f3xvault.com/api.php"

with open('config.json') as f:
    cfg = json.load(f)
LOGIN = {'login': cfg['f3xvault']['login'], 'password': cfg['f3xvault']['password']}

def api(fn, **kw):
    params = {'function': fn, 'output_format': 'json', **LOGIN, **kw}
    data = urllib.parse.urlencode(params).encode()
    with urllib.request.urlopen(BASE, data=data, timeout=15) as r:
        return json.loads(r.read())

def ok(msg): print(f"  ✓  {msg}")
def fail(msg): print(f"  ✗  {msg}")
def info(msg): print(f"     {msg}")

print(f"\n{'='*60}")
print(f"F3XVAULT PUSH DIAGNOSTIC — event {EVENT_ID}")
print(f"{'='*60}\n")

# ── STEP 1: Auth ─────────────────────────────────────────────
print("STEP 1: Auth")
r = api('checkUser')
if r.get('response_code') == 1:
    ok(f"Authenticated as user_id={r.get('user_id')} pilot_id={r.get('pilot_id')}")
    MY_PILOT_ID = r.get('pilot_id')
else:
    fail(f"Auth failed: {r.get('error_string')}")
    sys.exit(1)
print()

# ── STEP 2: Event info ───────────────────────────────────────
print("STEP 2: Event Info")
r = api('getEventInfo', event_id=EVENT_ID)
if r.get('response_code') != 1:
    fail(f"getEventInfo failed: {r.get('error_string')}")
    sys.exit(1)
event = r.get('event', {})
ok(f"Event: {event.get('event_name')}  type={event.get('event_type_code')}")
info(f"Total rounds: {event.get('total_rounds')}")

# Tasks
tasks = event.get('tasks', [])
info(f"Tasks ({len(tasks)}):")
task_map = {}  # round_number → {code, subs}
for t in tasks:
    rn   = t.get('round_number')
    code = t.get('flight_type_code')
    subs = t.get('flight_type_sub_flights')
    task_map[rn] = {'code': code, 'subs': subs}
    info(f"  R{rn}: {code}  ({subs} subs)  — {t.get('flight_type_name')}")

# Pilots
pilots = event.get('pilots', [])
ok(f"Pilots ({len(pilots)}):")
pilot_ids = {}  # name → pilot_id
for p in pilots:
    pid  = p.get('pilot_id')
    name = f"{p.get('pilot_first_name')} {p.get('pilot_last_name')}"
    bib  = p.get('pilot_bib', 0)
    pilot_ids[name] = pid
    info(f"  pilot_id={pid}  bib={bib}  {name}")
print()

# ── STEP 3: Check each round for pilot rows ──────────────────
print("STEP 3: Round pilot row verification (getEventRound)")
round_pilot_map = {}  # round_number → list of pilot_ids with rows
all_rounds_ok = True
for rn in sorted(task_map.keys()):
    r = api('getEventRound', event_id=EVENT_ID, round_number=rn)
    if r.get('response_code') != 1:
        fail(f"R{rn}: getEventRound failed: {r.get('error_string')}")
        all_rounds_ok = False
        continue
    flights = r.get('flights', [])
    pids    = [f['pilot_id'] for f in flights]
    round_pilot_map[rn] = pids
    scored  = [f for f in flights if f.get('score_status') == 1]
    status  = f"{len(flights)} pilots  {len(scored)} already scored"
    if len(flights) == len(pilots):
        ok(f"R{rn} ({task_map[rn]['code']}): {status}")
    else:
        fail(f"R{rn} ({task_map[rn]['code']}): {status}  ← MISSING {len(pilots)-len(flights)} pilots!")
        all_rounds_ok = False
    if flights:
        info(f"  pilot_ids in round: {pids}")
        # Check subs
        f0 = flights[0]
        info(f"  Sample pilot {f0['pilot_id']} subs: {f0.get('subs')}")
    time.sleep(0.2)
print()

# ── STEP 4: Test postScore for round 1 ───────────────────────
print("STEP 4: Test postScore — round 1, one pilot, known sub value")
if not task_map.get(1):
    fail("No round 1 task found")
else:
    test_pilot_id = pilots[0].get('pilot_id') if pilots else None
    test_pilot_in_round = test_pilot_id in round_pilot_map.get(1, [])
    info(f"Test pilot: {test_pilot_id}  in round 1: {test_pilot_in_round}")

    # Post a test score (0:05.0 = 5 seconds, clearly fake)
    r = api('postScore',
            event_id  = EVENT_ID,
            pilot_id  = test_pilot_id,
            round     = 1,
            group     = '',
            order     = 0,
            penalty   = 0,
            sub1      = '0:05.0')
    info(f"postScore response: {json.dumps(r)}")
    if r.get('response_code') == 1:
        ok("postScore accepted")
    else:
        fail(f"postScore rejected: {r.get('error_string')}")

    # Verify it landed
    time.sleep(1.0)
    r2 = api('getEventRound', event_id=EVENT_ID, round_number=1)
    for f in r2.get('flights', []):
        if f['pilot_id'] == test_pilot_id:
            info(f"  After post — pilot {test_pilot_id} subs: {f.get('subs')}  score={f.get('score')}")
            if f.get('subs') and f['subs'][0].get('sub_val') == '0:05.0':
                ok("Sub value confirmed written to F3XVault")
            else:
                fail(f"Sub value mismatch — expected 0:05.0, got: {f.get('subs')}")
print()

# ── STEP 5: Test updateEventRoundStatus ──────────────────────
print("STEP 5: Test updateEventRoundStatus round 1")
r = api('updateEventRoundStatus',
        event_id=EVENT_ID,
        round_number=1,
        event_round_score_status=1)
info(f"Response: {json.dumps(r)}")
if r.get('response_code') == 1:
    ok("Round marked scored")
else:
    fail(f"Failed: {r.get('error_string')}")
print()

# ── STEP 6: Check standings after round 1 scored ─────────────
print("STEP 6: Check standings after round 1 marked scored")
r = api('getEventInfo', event_id=EVENT_ID)
standings = r.get('event', {}).get('prelim_standings', {})
pilot_standings = standings.get('standings', [])
info(f"prelim_standings keys: {list(standings.keys())}")
if pilot_standings:
    ok(f"Standings have {len(pilot_standings)} pilots:")
    for p in pilot_standings:
        info(f"  #{p.get('pilot_position')} {p.get('pilot_first_name')} {p.get('pilot_last_name')}  total={p.get('total_score')}")
else:
    fail("No standings returned — F3XVault may require manual recalculation")
    info("This confirms updateEventRoundStatus does NOT trigger score recalculation")
print()

print("="*60)
print("DIAGNOSTIC COMPLETE")
print("="*60)


# ── STEP 7: Test unmark → post → remark cycle ────────────────
print("\nSTEP 7: Unmark → postScore → remark cycle (the real fix)")
test_pid = pilots[0].get('pilot_id') if pilots else None
TEST_VAL = '0:07.7'

# Unmark round 2
r = api('updateEventRoundStatus', event_id=EVENT_ID, round_number=2, event_round_score_status=0)
if r.get('response_code') == 1:
    ok("Round 2 unmarked (unscored)")
else:
    fail(f"Unmark failed: {r.get('error_string')}")

time.sleep(0.5)

# Post a known value
r = api('postScore', event_id=EVENT_ID, pilot_id=test_pid, round=2,
        group='', order=0, penalty=0, sub1=TEST_VAL, sub2='0:03.3')
info(f"postScore response: {json.dumps(r)}")

time.sleep(0.5)

# Read back
r2 = api('getEventRound', event_id=EVENT_ID, round_number=2)
for f in r2.get('flights', []):
    if f['pilot_id'] == test_pid:
        got = f.get('subs', [])
        sub1_val = next((s['sub_val'] for s in got if s['sub_num']==1), None)
        if sub1_val == TEST_VAL:
            ok(f"Sub value confirmed: {sub1_val} ✓  (unmark→post→remark works!)")
        else:
            fail(f"Sub value mismatch — expected {TEST_VAL}, got {sub1_val}")
            info(f"  Full subs: {got}")

# Remark scored
r = api('updateEventRoundStatus', event_id=EVENT_ID, round_number=2, event_round_score_status=1)
ok("Round 2 re-marked scored") if r.get('response_code')==1 else fail("Re-mark failed")

# ── STEP 8: Test web form save ────────────────────────────────
print("\nSTEP 8: Test vault_save_round_web (event_round_id needed)")
print("  NOTE: event_round_id values must be obtained from the event view page")
print("  For event 4365, rounds should be 30759-30764")
print("  Skipping automated test — requires event_round_id lookup")
print("  The web form POST approach will be verified during real push")



# ── STEP 9: Read actual contest.json to see what flights are saved ──
print("\nSTEP 9: Read contest.json — check saved flight data")
import os
contest_file = 'contest.json'
if not os.path.exists(contest_file):
    print(f"  contest.json not found in {os.getcwd()}")
else:
    with open(contest_file) as f:
        contest = json.load(f)
    rounds = contest.get('rounds', [])
    print(f"  {len(rounds)} rounds in contest.json")
    for i, rnd in enumerate(rounds):
        task   = rnd.get('task', '?')
        status = rnd.get('status', '?')
        groups = rnd.get('groups', [])
        print(f"\n  R{i+1} task={task} status={status} groups={len(groups)}")
        for gi, grp in enumerate(groups):
            results = grp.get('results', {})
            grp_status = grp.get('status', '?')
            print(f"    G{gi+1} status={grp_status} pilots={len(results)}")
            for pname, res in list(results.items())[:2]:  # show first 2 pilots
                flights = res.get('flights', [])
                raw     = res.get('task_raw_s', 0)
                print(f"      {pname}: raw={raw}s  flights={len(flights)}")
                for f in flights[:3]:
                    print(f"        dur={f.get('dur')}  lh_ft={f.get('lh_ft')}  capped={f.get('capped_dur')}  dq={f.get('dq')}")


# ── STEP 10: Simulate what vault_post_score would send ───────
print("\nSTEP 10: Simulate sub params for each round")

import sys
sys.path.insert(0, '.')
try:
    from f3xvault import _select_flights_for_task, _build_sub_params, seconds_to_vault_sub, FLIGHT_TYPE_CODE_MAP
    print("  f3xvault module loaded OK")
except Exception as e:
    print(f"  Could not load f3xvault: {e}")
    sys.exit(0)

with open('contest.json') as f:
    contest = json.load(f)

for i, rnd in enumerate(contest.get('rounds', [])):
    task_code = rnd.get('task', '?')
    groups    = rnd.get('groups', [])
    if not groups: continue
    grp     = groups[0]
    results = grp.get('results', {})
    print(f"\n  R{i+1} task={task_code}")
    for pname, res in list(results.items())[:2]:
        flights = res.get('flights', [])
        raw     = res.get('task_raw_s', 0)
        print(f"    {pname}: raw={raw}s  total_flights={len(flights)}")
        if flights:
            # Simulate _build_sub_params
            try:
                sub_params, penalty = _build_sub_params(
                    task_id          = task_code,
                    scored_flights   = [],
                    task_flights     = flights,
                    n_subs_override  = None,
                    penalty_per_foot = 0.5,
                    use_ll_penalty   = False,
                )
                print(f"      → subs: {sub_params}  penalty={penalty}")
            except Exception as e:
                print(f"      → ERROR: {e}")
        else:
            print(f"      → no flights — would send all zeros")

