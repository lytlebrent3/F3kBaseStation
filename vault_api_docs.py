"""
F3XVault API full documentation reader.
Run from F3K_Ground_System_Prototype folder (needs config.json for credentials).
Usage: python3 vault_api_docs.py  ->  vault_api_docs.txt
"""
import json, urllib.request, urllib.parse, time, sys

BASE  = "https://www.f3xvault.com/api.php"
with open('config.json') as f:
    cfg = json.load(f)['f3xvault']
LOGIN = {'login': cfg['login'], 'password': cfg['password']}

def api(fn, **kw):
    params = {'function': fn, 'output_format': 'json', **LOGIN, **kw}
    data = urllib.parse.urlencode(params).encode()
    with urllib.request.urlopen(BASE, data=data, timeout=15) as r:
        return json.loads(r.read())

lines = []
def p(*args):
    s = ' '.join(str(a) for a in args)
    print(s); lines.append(s)

KNOWN_FUNCTIONS = [
    'checkUser','getEventInfo','getEventInfoFull','getEventRound',
    'getEventRoundFull','getPilotInfo','getPilotResults','getPilotProfile',
    'getEventList','getEventListFull','getEventResults','getEventResultsFull',
    'getEventDraw','getEventDrawFull','getFlightTypes','getFlightTypeList',
    'getRoundTypes','postScore','postScoreFull','updateEventRoundStatus',
    'updateEventRound','updateEventStatus','createEvent','createEventRound',
    'addPilotToEvent','removePilotFromEvent','setEventDraw','showParameters',
]

p('='*70); p('F3XVAULT API REFERENCE'); p('='*70); p()
found, not_found = [], []
for fn_name in KNOWN_FUNCTIONS:
    time.sleep(0.25)
    try:
        resp = api('showParameters', function_name=fn_name)
        if resp.get('response_code') == 1:
            found.append(fn_name)
            p('─'*60); p(f'FUNCTION: {fn_name}'); p('─'*60)
            for f in (resp.get('input_fields') or []):
                req  = 'REQUIRED' if f.get('mandatory') else 'optional'
                p(f"  IN  {f['field_name']:<28} {f['field_type']:<12} {req:<10} {f.get('description','')}")
            for f in (resp.get('output_fields') or []):
                p(f"  OUT {f['field_name']:<28} {f['field_type']:<12} {f.get('description','')}")
            p()
        else:
            not_found.append(fn_name)
    except Exception as e:
        not_found.append(fn_name); p(f'  {fn_name}: ERR {e}')

p(f'Found: {len(found)}  Not available: {not_found}'); p()

for label, eid, rn in [('4361 R1',4361,1),('2637 R1',2637,1)]:
    p('='*70); p(f'getEventRound sample — event {label}'); p('='*70)
    try:
        er = api('getEventRound', event_id=eid, round_number=rn)
        if er.get('response_code') == 1:
            p(f"Keys: {[k for k in er if k not in ('response_code','error_string')]}")
            fl = (er.get('flights') or [None])[0]
            if fl:
                p(f"Flight keys: {list(fl.keys())}")
                p(json.dumps(fl, indent=2))
        else: p(f"Error: {er.get('error_string')}")
    except Exception as e: p(f'ERR: {e}')
    p()

p('='*70); p('getEventInfo — round + pilot field inventory'); p('='*70)
try:
    ev = api('getEventInfo', event_id=4361)
    if ev.get('response_code') == 1:
        p(f"Top keys: {[k for k in ev if k not in ('response_code','error_string')]}")
        if ev.get('rounds'):
            r0 = ev['rounds'][0]
            p(f"Round keys: {list(r0.keys())}"); p(json.dumps(r0, indent=2))
        if ev.get('pilots'):
            p0 = ev['pilots'][0]
            p(f"Pilot keys: {list(p0.keys())}"); p(json.dumps(p0, indent=2))
    else: p(f"Error: {ev.get('error_string')}")
except Exception as e: p(f'ERR: {e}')
p()

p('='*70); p('getEventInfoFull — extra fields?'); p('='*70)
try:
    ev2 = api('getEventInfoFull', event_id=4361)
    if ev2.get('response_code') == 1:
        p(f"Top keys: {[k for k in ev2 if k not in ('response_code','error_string')]}")
        if ev2.get('rounds'):
            r0 = ev2['rounds'][0]
            p(f"Round keys: {list(r0.keys())}"); p(json.dumps(r0, indent=2))
    else: p(f"Error: {ev2.get('error_string')}")
except Exception as e: p(f'ERR: {e}')
p()

p('='*70); p('ALL FLIGHT TYPE CODES (events 4361,4359,2637,4277)'); p('='*70)
all_codes = {}
for eid in [4361, 4359, 2637, 4277]:
    try:
        ev = api('getEventInfo', event_id=eid)
        if ev.get('response_code') == 1:
            for rnd in ev.get('rounds', []):
                fid = rnd.get('flight_type_id')
                if fid and fid not in all_codes:
                    all_codes[fid] = {
                        'id': fid,
                        'code': rnd.get('flight_type_code'),
                        'name': rnd.get('flight_type_name'),
                        'n_subs': rnd.get('num_scores'),
                        'working_time': rnd.get('working_time'),
                    }
        time.sleep(0.3)
    except Exception as e: p(f'event {eid}: {e}')

p(f"  {'ID':>4}  {'Code':<24}  {'Subs':>4}  {'WorkingTime':>12}  Name")
p(f"  {'─'*4}  {'─'*24}  {'─'*4}  {'─'*12}  {'─'*45}")
for fid in sorted(all_codes.keys()):
    t = all_codes[fid]
    p(f"  {str(t['id']):>4}  {str(t['code']):<24}  {str(t['n_subs']):>4}  "
      f"{str(t['working_time']):>12}  {t['name']}")

with open('vault_api_docs.txt', 'w') as f:
    f.write('\n'.join(lines))
p(); p('Saved to vault_api_docs.txt')
