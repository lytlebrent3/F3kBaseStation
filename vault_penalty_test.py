"""
Test whether postScore accepts a 'penalty' field for Low Launch penalty.
Posts to event 2637 (test event) with sub1=raw duration and penalty=lh_penalty.
"""
import json, urllib.request, urllib.parse

with open('config.json') as f:
    cfg = json.load(f)['f3xvault']

def api(function, **kwargs):
    params = {'login': cfg['login'], 'password': cfg['password'],
              'function': function, 'output_format': 'json', **kwargs}
    data = urllib.parse.urlencode(params).encode()
    with urllib.request.urlopen('http://www.f3xvault.com/api.php',
                                data=data, timeout=10) as r:
        return json.loads(r.read())

# Also check showParameters for postScore with credentials
print("=== showParameters for postScore ===")
result = api('showParameters', function_name='postScore')
print(json.dumps(result, indent=2))

print()
print("=== Test postScore with penalty field ===")
# Post to event 2637, pilot 2401 (Brent Lytle), round 4
# Raw flight = 55.0s, launch height penalty = 11.5s (23ft × 0.5s/ft)
result = api('postScore',
    event_id = 2637,
    pilot_id = 2401,
    round    = 4,
    group    = 'A',
    sub1     = '0:55.0',   # raw flight duration
    penalty  = 12,         # launch height penalty in whole seconds
)
print(json.dumps(result, indent=2))
