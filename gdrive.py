"""
Google Drive uploader — OAuth2 with your personal Google account.
Run python3 auth.py once to authenticate. token.json auto-refreshes.
"""

import logging
import re
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

log = logging.getLogger("gdrive")
SCOPES = ["https://www.googleapis.com/auth/drive"]


class GDriveUploader:

    def __init__(self, root_folder_id: str, token_file: str = "token.json"):
        token_path = Path(token_file)
        if not token_path.exists():
            raise FileNotFoundError(
                f"token.json not found at: {token_path}\n"
                "Run  python3 auth.py  once to authenticate."
            )

        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json())
            log.debug("OAuth token refreshed")

        # Accept a full Drive URL or a bare folder ID
        if "drive.google.com" in root_folder_id:
            match = re.search(r"/folders/([a-zA-Z0-9_-]+)", root_folder_id)
            if match:
                root_folder_id = match.group(1)
                log.debug(f"Extracted folder ID: {root_folder_id}")
            else:
                raise ValueError(f"Could not extract folder ID from: {root_folder_id}")

        self._svc  = build("drive", "v3", credentials=creds, cache_discovery=False)
        self._root = root_folder_id

    def get_or_create_date_folder(self, date_str: str) -> str:
        q = (
            f"name='{date_str}' and '{self._root}' in parents "
            f"and mimeType='application/vnd.google-apps.folder' and trashed=false"
        )
        files = (
            self._svc.files()
            .list(q=q, fields="files(id)", spaces="drive")
            .execute()
            .get("files", [])
        )
        if files:
            log.info(f"Drive folder exists: {date_str}")
            return files[0]["id"]

        meta = {
            "name":     date_str,
            "mimeType": "application/vnd.google-apps.folder",
            "parents":  [self._root],
        }
        folder = self._svc.files().create(body=meta, fields="id").execute()
        log.info(f"Created Drive folder: {date_str} (id={folder['id']})")
        return folder["id"]

    def upload_file(self, local_path: Path, folder_id: str) -> str:
        name  = local_path.name
        q     = f"name='{name}' and '{folder_id}' in parents and trashed=false"
        existing = (
            self._svc.files()
            .list(q=q, fields="files(id)")
            .execute()
            .get("files", [])
        )
        media = MediaFileUpload(str(local_path), mimetype="application/pdf", resumable=True)

        if existing:
            self._svc.files().update(fileId=existing[0]["id"], media_body=media).execute()
            log.debug(f"  Updated: {name}")
            return existing[0]["id"]
        else:
            f = self._svc.files().create(
                body={"name": name, "parents": [folder_id]},
                media_body=media,
                fields="id",
            ).execute()
            return f["id"]
