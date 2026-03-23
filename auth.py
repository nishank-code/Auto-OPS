"""
One-time Google Drive authentication.
Run once: python3 auth.py
Opens a browser → you approve → token.json is saved.
All subsequent runs use token.json silently (auto-refreshed).
"""

from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES           = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = Path(__file__).parent / "credentials.json"
TOKEN_FILE       = Path(__file__).parent / "token.json"


def main():
    if not CREDENTIALS_FILE.exists():
        print(f"""
ERROR: credentials.json not found at {CREDENTIALS_FILE}

Steps to get it:
  1. Go to https://console.cloud.google.com/
  2. Select your project
  3. APIs & Services → Credentials → + Create Credentials → OAuth 2.0 Client ID
  4. Application type: Desktop app → Create
  5. Download JSON → rename to credentials.json → place it in this folder
""")
        return

    print("Opening browser for Google authentication…")
    flow  = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), SCOPES)
    creds = flow.run_local_server(port=0, prompt="consent")
    TOKEN_FILE.write_text(creds.to_json())
    print(f"\n✅ Done! token.json saved. You won't need to run this again.")


if __name__ == "__main__":
    main()
