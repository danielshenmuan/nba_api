#!/usr/bin/env python3
"""Two-step Yahoo re-authorisation. Splits the interactive flow so the
browser step and the token exchange are separate commands.

  Step 1:  python tools/yahoo_reauth.py url
  Step 2:  python tools/yahoo_reauth.py exchange <code>
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
    q = urllib.parse.urlencode({"client_id": CK, "redirect_uri": "oob",
                                "response_type": "code", "language": "en-us"})
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
    print("\n  ✅ Looks right" if scope else "\n  ❌ Still no scope — app permission is not applied")
else:
    sys.exit(__doc__)
