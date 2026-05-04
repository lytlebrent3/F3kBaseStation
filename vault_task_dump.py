"""Dump raw task data from event 4361 to see all flight_type fields."""
import json, urllib.request, urllib.parse

with open('config.json') as f:
    cfg = json.load(f)['f3xvault']

LOGIN = {'login': cfg['login'], 'password': cfg['password']}

def api(fn, **kw):
    params = {'function': fn, 'output_format': 'json', **LOGIN, **kw}
    data = urllib.parse.urlencode(params).encode()
    with urllib.request.urlopen('https://www.f3xvault.com/api.php', data=data, timeout=15) as r:
        return json.loads(r.read())

result = api('getEventInfo', event_id=4361)
event  = result.get('event', {})
tasks  = event.get('tasks', [])

print(f"Total tasks: {len(tasks)}")
print()
print(f"{'RN':>3}  {'flight_type_id':>16}  {'flight_type_code':<20}  {'sub_flights':>10}  {'time_choice':>11}  flight_type_name")
print('─' * 120)
for t in tasks:
    rn   = t.get('round_number', '?')
    fid  = t.get('flight_type_id', '?')
    code = t.get('flight_type_code', '?')
    subs = t.get('flight_type_sub_flights', '?')
    tc   = t.get('event_task_time_choice', '?')
    name = t.get('flight_type_name', '?')
    print(f"{rn:>3}  {str(fid):>16}  {code:<20}  {str(subs):>10}  {str(tc):>11}  {name}")

# Also dump the full raw dict for Task C variants (rounds 5 and 7)
print()
print("FULL RAW TASK DATA for Task C variants:")
for t in tasks:
    if 'f3k_c' in (t.get('flight_type_code') or ''):
        print(json.dumps(t, indent=2))
        print()

# And Task D variants
print("FULL RAW TASK DATA for Task D variants:")
for t in tasks:
    if 'f3k_d' in (t.get('flight_type_code') or ''):
        print(json.dumps(t, indent=2))
        print()

# And Task E variants
print("FULL RAW TASK DATA for Task E variants:")
for t in tasks:
    if 'f3k_e' in (t.get('flight_type_code') or ''):
        print(json.dumps(t, indent=2))
        print()

# Also Task A variants
print("FULL RAW TASK DATA for Task A variants:")
for t in tasks:
    if 'f3k_a' in (t.get('flight_type_code') or ''):
        print(json.dumps(t, indent=2))
        print()

