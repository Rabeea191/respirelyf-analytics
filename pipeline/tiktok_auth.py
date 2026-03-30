"""
TikTok OAuth — one-time local script to get ACCESS_TOKEN + REFRESH_TOKEN.

Run this ONCE locally after creating your TikTok Developer app:
  python pipeline/tiktok_auth.py

Steps:
  1. Paste your CLIENT_KEY and CLIENT_SECRET when prompted
  2. A browser URL will be printed — open it, authorize the app
  3. Copy the 'code' param from the redirect URL and paste it here
  4. Script prints ACCESS_TOKEN, REFRESH_TOKEN, OPEN_ID
  5. Add those 3 values as GitHub Secrets

Requirements:
  pip install requests
"""
import sys
import urllib.parse
import webbrowser
import requests

REDIRECT_URI = "https://www.example.com/callback"   # must match your TikTok app settings
SCOPE        = "user.info.basic,video.list"           # add more scopes as needed
API_BASE     = "https://open.tiktokapis.com/v2"
TOKEN_URL    = "https://open.tiktokapis.com/v2/oauth/token/"


def main():
    print("=" * 60)
    print("TikTok OAuth — one-time token setup")
    print("=" * 60)

    client_key    = input("\nPaste your TIKTOK_CLIENT_KEY:    ").strip()
    client_secret = input("Paste your TIKTOK_CLIENT_SECRET: ").strip()

    if not client_key or not client_secret:
        print("ERROR: Both CLIENT_KEY and CLIENT_SECRET are required.")
        sys.exit(1)

    # Build authorization URL
    params = urllib.parse.urlencode({
        "client_key":     client_key,
        "scope":          SCOPE,
        "response_type":  "code",
        "redirect_uri":   REDIRECT_URI,
        "state":          "respirelyf_analytics",
    })
    auth_url = f"https://www.tiktok.com/v2/auth/authorize/?{params}"

    print(f"\n{'='*60}")
    print("STEP 1: Open this URL in your browser and authorize the app:")
    print(f"\n{auth_url}\n")
    print("=" * 60)

    try:
        webbrowser.open(auth_url)
        print("(Browser opened automatically)")
    except Exception:
        print("(Could not open browser — please copy URL manually)")

    print("\nAfter authorizing, you'll be redirected to:")
    print(f"  {REDIRECT_URI}?code=XXXX&state=respirelyf_analytics")
    print("\nCopy the full redirect URL and paste it below:")
    redirect_url = input("Redirect URL: ").strip()

    # Extract code
    parsed  = urllib.parse.urlparse(redirect_url)
    qs      = urllib.parse.parse_qs(parsed.query)
    code    = qs.get("code", [None])[0]

    if not code:
        print("ERROR: Could not find 'code' in the redirect URL.")
        sys.exit(1)

    print(f"\n[tiktok_auth] Got code: {code[:20]}...")
    print("[tiktok_auth] Exchanging code for tokens...")

    # Exchange code for tokens
    r = requests.post(TOKEN_URL, data={
        "client_key":     client_key,
        "client_secret":  client_secret,
        "code":           code,
        "grant_type":     "authorization_code",
        "redirect_uri":   REDIRECT_URI,
    }, timeout=30)

    if r.status_code != 200:
        print(f"ERROR {r.status_code}: {r.text}")
        sys.exit(1)

    data          = r.json().get("data", r.json())
    access_token  = data.get("access_token")
    refresh_token = data.get("refresh_token")
    open_id       = data.get("open_id")

    if not access_token:
        print(f"ERROR: No access_token in response: {r.text}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("SUCCESS! Add these 3 values as GitHub Secrets:")
    print("=" * 60)
    print(f"\nTIKTOK_ACCESS_TOKEN:  {access_token}")
    print(f"TIKTOK_REFRESH_TOKEN: {refresh_token or '(not provided)'}")
    print(f"TIKTOK_OPEN_ID:       {open_id or '(not provided)'}")
    print(f"\nAlso add:")
    print(f"TIKTOK_CLIENT_KEY:    {client_key}")
    print(f"TIKTOK_CLIENT_SECRET: {client_secret}")
    print("=" * 60)


if __name__ == "__main__":
    main()
