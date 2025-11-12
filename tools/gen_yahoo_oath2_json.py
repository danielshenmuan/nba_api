# nba_api/tools/gen_yahoo_oauth2_json.py
import os, json, sys
from pathlib import Path

try:
    from dotenv import load_dotenv, find_dotenv
    from yahoo_oauth import OAuth2
except ImportError:
    print("Install deps first: pip install yahoo-oauth python-dotenv", file=sys.stderr)
    sys.exit(1)

# Load .env from repo root (nba_api/.env)
REPO_ROOT = Path(__file__).resolve().parents[1]   # .../nba_api
ENV_PATH = REPO_ROOT / ".env"
loaded = load_dotenv(dotenv_path=str(ENV_PATH), override=False) or load_dotenv(find_dotenv(usecwd=True), override=False)

ck = os.getenv("YAHOO_CONSUMER_KEY")
cs = os.getenv("YAHOO_CONSUMER_SECRET")
if not ck or not cs:
    print(f"[error] Missing YAHOO_CONSUMER_KEY and/or YAHOO_CONSUMER_SECRET in {ENV_PATH}", file=sys.stderr)
    sys.exit(2)

print("[info] starting Yahoo device flow...")
# Use the simplified constructor as requested
oauth = OAuth2(consumer_key=ck, consumer_secret=cs)

# Pull the token dict (newer versions use .token; older used .credentials)
token = getattr(oauth, "token", None) or getattr(oauth, "credentials", None)
if not isinstance(token, dict):
    print("[error] OAuth flow did not yield a token. Did you complete the verifier step?", file=sys.stderr)
    sys.exit(3)

out_path = REPO_ROOT / "yahoo_oauth2.json"
with out_path.open("w") as f:
    json.dump(token, f, indent=2)

print(f"[done] wrote {out_path}")
print(f"         has refresh_token: {'refresh_token' in token}")
if "refresh_token" not in token:
    print("[warn] No refresh_token present. Re-run and make sure you finish the browser/device verification.", file=sys.stderr)
