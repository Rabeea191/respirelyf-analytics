"""
One-time script to get Google OAuth Refresh Token for YouTube Analytics.

Steps:
  1. Fill in CLIENT_ID and CLIENT_SECRET below (from Google Cloud Console)
  2. Run:  python get_youtube_token.py
  3. Browser opens → login with YouTube channel owner account → Allow
  4. Paste the code shown in browser → script prints REFRESH_TOKEN
  5. Add REFRESH_TOKEN to GitHub Secrets

Scopes needed:
  - youtube.readonly        (channel + video data)
  - yt-analytics.readonly   (analytics metrics)
"""

import urllib.parse
import webbrowser

# ── Fill these in ─────────────────────────────────────────────────────────────
CLIENT_ID     = "111765701895-bsff6q29nrgdmk1oh9p2md4027mmhlat.apps.googleusercontent.com"  # see KEYS.md
CLIENT_SECRET = "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"  # see KEYS.md
# ─────────────────────────────────────────────────────────────────────────────

REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"
SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]

def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("❌ Fill in CLIENT_ID and CLIENT_SECRET at the top of this file first.")
        return

    # Step 1: Open browser for auth
    auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth?"
        + urllib.parse.urlencode({
            "client_id":     CLIENT_ID,
            "redirect_uri":  REDIRECT_URI,
            "response_type": "code",
            "scope":         " ".join(SCOPES),
            "access_type":   "offline",
            "prompt":        "consent",
        })
    )
    print(f"\n🌐 Opening browser...\n{auth_url}\n")
    webbrowser.open(auth_url)

    # Step 2: Get auth code from user
    code = input("📋 Paste the authorization code from the browser here:\n> ").strip()

    # Step 3: Exchange code for tokens
    import urllib.request, json
    data = urllib.parse.urlencode({
        "code":          code,
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri":  REDIRECT_URI,
        "grant_type":    "authorization_code",
    }).encode()

    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=data,
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        tokens = json.loads(resp.read())

    refresh_token = tokens.get("refresh_token", "")
    if not refresh_token:
        print(f"\n❌ No refresh_token in response: {tokens}")
        return

    print(f"""
✅ Success! Add these to GitHub Secrets:

  GOOGLE_CLIENT_ID     = {CLIENT_ID}
  GOOGLE_CLIENT_SECRET = {CLIENT_SECRET}
  GOOGLE_REFRESH_TOKEN = {refresh_token}
""")


if __name__ == "__main__":
    main()
