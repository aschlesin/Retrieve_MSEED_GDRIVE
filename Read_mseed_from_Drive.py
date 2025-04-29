# pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

# IMPORTANT!!! Follow set-up instructions on https://developers.google.com/workspace/drive/api/quickstart/python

# You will need to change variables 'credential_json' and 'token_json' for your own setup


from obspy import UTCDateTime

import io
import struct
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os.path

from obspy import read, Stream
from obspy import UTCDateTime
import pandas as pd
import os


from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def list_all_mseed_files(service, folder_id, file_type):
    all_files = []

    def walk_folder(folder_id, file_type):
        # Find all files with .mseed
        results = service.files().list(
            q=f"'{folder_id}' in parents and name contains '{file_type}' and trashed = false",
            fields="files(id, name, mimeType, parents)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        all_files.extend(results.get("files", []))

        # Filter for names that truly end with file type (e.g. mseed)
        #for f in resp.get("files", []):
            #if f["name"].lower().endswith(file_type):
        #    all_files.append(f)

        # Now recurse into subfolders
        subfolders = service.files().list(
            q=f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute().get("files", [])

        for folder in subfolders:
            walk_folder(folder["id"], file_type)

    walk_folder(folder_id, file_type)
    return all_files

def get_credentials(credential_json,token_json,SCOPES):
  """Shows basic usage of the Drive v3 API.
  Prints the names and ids of the first 10 files the user has access to.
  """
  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists(token_json):
    creds = Credentials.from_authorized_user_file(token_json, SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          credential_json , SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open(token_json, "w") as token:
      token.write(creds.to_json())
    
  return creds

def get_folder_id(drive, name, parent_id=None):
    """
    Find the folder named `name`. If parent_id is given, restrict the search
    to items with that parent. Returns the first matching folder ID.
    """
    q_parts = [
        f"name = '{name}'",
        "mimeType = 'application/vnd.google-apps.folder'",
        "trashed = false"
    ]
    if parent_id:
        # only look inside that parent
        q_parts.append(f"'{parent_id}' in parents")
    query = " and ".join(q_parts)

    resp = drive.files().list(
        q=query,
        fields="files(id, name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        corpora="allDrives"
    ).execute()
    items = resp.get("files", [])
    if not items:
        raise FileNotFoundError(f"No folder named {name!r} found")
    return items[0]["id"]  # or disambiguate if len(items)>1
    
def get_file_path(service, file_id):
    path_parts = []

    while True:
        file = service.files().get(
            fileId=file_id,
            fields="name, parents",
            supportsAllDrives=True
        ).execute()

        path_parts.insert(0, file["name"])  # prepend to path

        parents = file.get("parents")
        if not parents:
            break  # reached root (Shared Drive root or My Drive)

        file_id = parents[0]  # move up to parent

    return "/" + "/".join(path_parts)

# --- Fetch only the first 48 bytes via Range request -------------
def fetch_header_bytes(file_id):
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    r = session.get(url, headers={"Range": "bytes=0-47"}, stream=True)
    r.raise_for_status()
    return r.content  # exactly 48 bytes

# --- Fetch the MSEED content for capturing in stream object

def fetch_full_mseed(file_id, session):
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    r = session.get(url, stream=True)
    r.raise_for_status()
    return read(io.BytesIO(r.content), format='MSEED')


# --- Parse those 48 bytes ----------------------------------------
def parse_mseed_header(hdr_bytes: bytes):
    station = hdr_bytes[8:13].decode("ascii").strip()
    location = hdr_bytes[13:15].decode("ascii").strip()
    channel = hdr_bytes[15:18].decode("ascii").strip()
    network = hdr_bytes[18:20].decode("ascii").strip().strip()
    
    time = hdr_bytes[20:30]
    year = struct.unpack('>H',time[0:2])[0]
    julday = struct.unpack('>H',time[2:4])[0]
    hour = time[4]
    minute = time[5]
    second = time[6]
    msecs = struct.unpack('>H',time[8:10])[0]
    dtime = UTCDateTime(year=year, julday=julday, hour=hour,
                        minute=minute, second=second,
                        microsecond=msecs * 100)
    samples = struct.unpack(">H", hdr_bytes[30:32])[0]
    sample_rate = hdr_bytes[33]
    endtime = dtime + samples/sample_rate

    
    return {
        "station": station,
        "location": location,
        "channel": channel,
        "network": network,
        'starttime': dtime,
        'endtime': endtime,
        'samples': samples,
        'sample_rate': sample_rate
    }



