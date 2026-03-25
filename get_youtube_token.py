"""
One-time script to get Google OAuth Refresh Token for YouTube Analytics.

Steps:
  1. Run:  python get_youtube_token.py
  2. Browser opens → login with YouTube channel owner account → Allow
  3. Script auto-captures the code and prints REFRESH_TOKEN
  4. Add REFRESH_TOKEN to GitHub Secrets

Scopes needed:
  - youtube.readonly        (channel + video data)
  - yt-analytics.readonly   (analytics metrics)
"""

import http.server
import json
import threading
import urllib.parse
import urllib.request
import webbrowser

# ── Credentials ───────────────────────────────────────────────────────────────
CLIENT_ID     = "111765701895-4v50taonu9qgqs4aaojqpjj6g5c1ib2q.apps.googleusercontent.com"
CLIENT_SECRET = "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"
# ─────────────────────────────────────────────────────────────────────────────

PORT = 8080
REDIRECT_URI = f"http://localhost:{PORT}"
SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
]

auth_code = None


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        auth_code = params.get("code", [None])[0]
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b"<h2>Authorization complete! You can close this tab.</h2>")

    def log_message(self, *args):
        pass  # suppress server logs


def main():
    # Step 1: Build auth URL (localhost redirect — works in production)
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

    # Step 2: Start local server to catch redirect
    server = http.server.HTTPServer(("localhost", PORT), _Handler)
    thread = threading.Thread(target=server.handle_request)
    thread.start()

    print(f"\n🔗 Open this URL in your browser:\n\n{auth_url}\n")
    print("⏳ Waiting for authorization (login in browser and click Allow)...")

    thread.join(timeout=120)
    server.server_close()

    if not auth_code:
        print("❌ No authorization code received. Did you allow access in the browser?")
        return

    # Step 3: Exchange code for tokens
    data = urllib.parse.urlencode({
        "code":          auth_code,
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
    try:
        with urllib.request.urlopen(req) as resp:
            tokens = json.loads(resp.read())
    except Exception as e:
        print(f"❌ Token exchange failed: {e}")
        return

    refresh_token = tokens.get("refresh_token", "")
    if not refresh_token:
        print(f"\n❌ No refresh_token in response: {tokens}")
        return

    print(f"""
✅ Success! Add to GitHub Secrets:

  GOOGLE_REFRESH_TOKEN = {refresh_token}
""")


if __name__ == "__main__":
    main()
