#!/usr/bin/env python3
"""Two-step Yahoo re-authorisation. Splits the interactive flow so the
browser step and the token exchange are separate commands.

  Step 1:  python tools/yahoo_reauth.py url
  Step 2:  python tools/yahoo_reauth.py exchange <code>

The authorization request asks for scope `fspt-r` (Fantasy Sports, read) explicitly; override with
YAHOO_SCOPE. A request WITHOUT a scope yields a token that carries no scope, and every Fantasy API call
then returns 403 "This application is not authorized to perform this action".
`exchange` finishes by calling the Fantasy API once, so success or failure is unambiguous.
"""
import json, os, sys, urllib.parse, urllib.request, ssl, certifi
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
try:
    from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
except ImportError:
    sys.exit("pip install python-dotenv")

CK = os.getenv("YAHOO_CONSUMER_KEY"); CS = os.getenv("YAHOO_CONSUMER_SECRET")
if not CK or not CS:
    sys.exit("Missing YAHOO_CONSUMER_KEY / YAHOO_CONSUMER_SECRET in .env")
CTX = ssl.create_default_context(cafile=certifi.where())

def post(data):
    req = urllib.request.Request(
        "https://api.login.yahoo.com/oauth2/get_token",
        data=urllib.parse.urlencode(data).encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"})
    return json.loads(urllib.request.urlopen(req, timeout=30, context=CTX).read())

cmd = sys.argv[1] if len(sys.argv) > 1 else "url"

if cmd == "url":
    q = urllib.parse.urlencode({"client_id": CK, "redirect_uri": "oob", "response_type": "code",
                                "language": "en-us", "scope": os.getenv("YAHOO_SCOPE", "fspt-r")})
    print("\nOpen this, sign in, and APPROVE:\n")
    print(f"  https://api.login.yahoo.com/oauth2/request_auth?{q}\n")
    print("  >> The consent screen MUST mention Fantasy Sports.")
    print("     If it does not, the app permission is not applied.\n")
    print("Then run:  python tools/yahoo_reauth.py exchange <CODE>\n")

elif cmd == "exchange":
    if len(sys.argv) < 3: sys.exit("usage: yahoo_reauth.py exchange <CODE>")
    try:
        tok = post({"client_id": CK, "client_secret": CS, "redirect_uri": "oob",
                    "code": sys.argv[2].strip(), "grant_type": "authorization_code"})
    except urllib.error.HTTPError as e:
        sys.exit(f"Exchange failed HTTP {e.code}: {e.read().decode()[:300]}")
    out = ROOT / "yahoo_oauth2.json"
    out.write_text(json.dumps(tok, indent=2))
    print(f"\nwrote {out}")
    print(f"  refresh_token present : {'refresh_token' in tok}")
    scope = tok.get("scope")
    print(f"  scope                 : {scope if scope else '(none)'}")
    print("\n  ✅ Token carries a scope" if scope else "\n  ⚠️  Token carries no scope")
    # The decisive test: does the Fantasy API accept this token?
    api = urllib.request.Request("https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games?format=json",
                                 headers={"Authorization": f"Bearer {tok['access_token']}"})
    try:
        r = urllib.request.urlopen(api, timeout=30, context=CTX)
        print(f"  ✅ Fantasy API test: HTTP {r.status}. THE APP WORKS.")
    except urllib.error.HTTPError as e:
        print(f"  ❌ Fantasy API test: HTTP {e.code} {e.read().decode()[:140]}")
else:
    sys.exit(__doc__)
