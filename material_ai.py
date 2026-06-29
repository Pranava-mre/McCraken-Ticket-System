


# import time
# from datetime import datetime
# import requests


# # ==========================
# # CHANGE THESE VALUES
# # ==========================

# CLOUD_BASE_URL = "https://sync.wavevms.com"
# SYSTEM_ID = "edbde632-d846-49f5-8fc8-7fdaaa2504e7"
# BASE_URL = f"https://{SYSTEM_ID}.relay-us-chi-2-prod-dp.vmsproxy.com"
# CAMERA_ID = "4f68efaf-da80-b7f7-94af-f895e6c42e5e"

# TICKET_TIME = "2026-06-26T02:48:00"
# DURATION_SECONDS = 8
# OUTPUT_FILE = "truck_video.webm"
# RESOLUTION = "1024x452"


# r = requests.post(
#     f"{CLOUD_BASE_URL}/cdb/oauth2/token",
#     json={
#         "grant_type": "password",
#         "response_type": "token",
#         "client_id": "3rdParty",
#         "scope": f"cloudSystemId={SYSTEM_ID}",
#         "username": USERNAME,
#         "password": PASSWORD,
#     },
#     timeout=30,
# )
# BEARER_TOKEN = r.json()["access_token"]





# # ==========================
# # HELPERS
# # ==========================

# def ticket_time_to_ms(ticket_time: str) -> int:
#     dt = datetime.strptime(ticket_time, "%Y-%m-%dT%H:%M:%S")
#     return int(dt.timestamp() * 1000)


# headers = {
#     "Authorization": f"Bearer {BEARER_TOKEN}",
#     "Accept": "application/json, text/plain, */*",
#     "Content-Type": "application/json",
# }


# # ==========================
# # STEP 1 : CHECK CONNECTION
# # ==========================

# print("Checking WAVE connection...")

# r = requests.get(
#     f"{BASE_URL}/rest/v2/servers",
#     headers=headers,
#     timeout=30
# )

# if r.status_code != 200:
#     print("Cannot connect to WAVE.")
#     print("Status:", r.status_code)
#     print(r.text)
#     exit()

# print("Connected to WAVE")


# # ==========================
# # STEP 2 : CHECK CAMERA
# # ==========================

# print("Checking camera...")

# r = requests.get(
#     f"{BASE_URL}/rest/v2/devices",
#     headers=headers,
#     timeout=30
# )

# if r.status_code != 200:
#     print("Could not get devices.")
#     print("Status:", r.status_code)
#     print(r.text)
#     exit()

# if CAMERA_ID not in r.text:
#     print("Camera not found.")
#     exit()

# print("Camera found")


# # ==========================
# # STEP 3 : CHECK ARCHIVE API
# # ==========================

# print("Checking archive availability API...")

# r = requests.get(
#     f"{BASE_URL}/ec2/recordedTimePeriods",
#     headers=headers,
#     params={"cameraId": CAMERA_ID},
#     timeout=30
# )

# if r.status_code != 200:
#     print("Could not query archive.")
#     print("Status:", r.status_code)
#     print(r.text)
#     exit()

# print("Archive API responded")


# # ==========================
# # STEP 4 : CREATE PLAYBACK TICKET
# # ==========================

# print("Creating playback ticket...")

# r = requests.post(
#     f"{BASE_URL}/rest/v3/login/tickets",
#     headers=headers,
#     json={},
#     timeout=30
# )

# if r.status_code != 200:
#     print("Could not create playback ticket.")
#     print("Status:", r.status_code)
#     print(r.text)
#     exit()

# playback_ticket = r.json()["token"]

# print("Playback ticket created")


# # ==========================
# # STEP 5 : DOWNLOAD PLAYBACK STREAM
# # ==========================

# import os
# import json
# from urllib.parse import urlencode

# position_ms = ticket_time_to_ms(TICKET_TIME)

# media_url = f"{BASE_URL}/rest/v3/devices/{CAMERA_ID}/media.webm"

# params = {
#     "positionMs": position_ms,
#     "durationMs": DURATION_SECONDS * 1000,
#     "resolution": RESOLUTION,
#     "stream": "secondary",
#     "accurateSeek": "true",
#     "download": "true",
#     "_ticket": playback_ticket,
# }

# print("\nDEBUG PARAMS")
# print(json.dumps(params, indent=2))
# print("\nFULL URL")
# print(media_url + "?" + urlencode(params))

# print("\nCHECKING FOOTAGE FOR SAME TIME")
# footage_params = {
#     "startTimeMs": position_ms - 60000,
#     "endTimeMs": position_ms + 60000,
#     "preciseBounds": "true",
#     "periodType": "recording",
# }

# footage = requests.get(
#     f"{BASE_URL}/rest/v3/devices/{CAMERA_ID}/footage",
#     headers=headers,
#     params=footage_params,
#     timeout=30
# )

# print("Footage status:", footage.status_code)
# print("Footage response:", footage.text[:1000])

# response = requests.get(
#     media_url,
#     params=params,
#     stream=True,
#     timeout=300
# )

# print("\nMEDIA RESPONSE")
# print("Status:", response.status_code)
# print("URL:", response.url)
# print("Headers:", dict(response.headers))

# downloaded = 0

# with open(OUTPUT_FILE, "wb") as f:
#     for chunk in response.iter_content(chunk_size=1024 * 1024):
#         if chunk:
#             f.write(chunk)
#             downloaded += len(chunk)
#             print("Downloaded bytes:", downloaded)

# response.close()

# print("\nDONE")
# print("Saved to:", OUTPUT_FILE)
# print("Final size:", os.path.getsize(OUTPUT_FILE))

import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlencode
import requests


CLOUD_BASE_URL = "https://sync.wavevms.com"
SYSTEM_ID = "edbde632-d846-49f5-8fc8-7fdaaa2504e7"
BASE_URL = f"https://{SYSTEM_ID}.relay-us-chi-2-prod-dp.vmsproxy.com"
USERNAME = "bwoods@sos-llc.net"
PASSWORD = "Walk36toolfun!"
CAMERA_ID = "4f68efaf-da80-b7f7-94af-f895e6c42e5e"
SERVER_GUID = "fafd22b6-2eb6-a286-1746-922a1d62e77c"
TICKET_TIME = "2026-06-25T12:48:00"
TIMEZONE = "America/New_York"
DURATION_SECONDS = 8
OUTPUT_FILE = "truck_video.webm"
RESOLUTION = "1024x452"


def ticket_time_to_ms(ticket_time: str) -> int:
    dt = datetime.strptime(ticket_time, "%Y-%m-%dT%H:%M:%S")
    dt = dt.replace(tzinfo=ZoneInfo(TIMEZONE))
    return int(dt.timestamp() * 1000)

#========================== STEP 1 : GET CLOUD ACCESS TOKEN ==========================
print("Getting cloud access token...")

r = requests.post(
    f"{CLOUD_BASE_URL}/cdb/oauth2/token",
    json={
        "grant_type": "password",
        "response_type": "token",
        "client_id": "3rdParty",
        "scope": f"cloudSystemId={SYSTEM_ID}",
        "username": USERNAME,
        "password": PASSWORD,
    },
    timeout=30,
)

print("OAuth status:", r.status_code)
#========================== STEP 2 : CHECK  WAVE RESPONSE ==========================
if r.status_code != 200:
    print(r.text)
    exit()

access_token = r.json()["access_token"]
print("Access token:", access_token)
if not access_token.startswith("nxcdb-"):
    access_token = "nxcdb-" + access_token

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
}
print(headers)
r = requests.get(
    f"{BASE_URL}/rest/v3/login/sessions/{access_token}",
    timeout=30
)

print(r.status_code)
print(r.text)
print("Checking WAVE connection...")

r = requests.get(
    f"{BASE_URL}/rest/v3/servers",
    headers=headers,
    timeout=30,
)

if r.status_code != 200:
    print("Cannot connect to WAVE.")
    print(r.text)
    exit()

print("Connected to WAVE")

#========================== STEP 2 : CHECK  Camera RESPONSE ==========================
print("Checking camera...")

r = requests.get(
    f"{BASE_URL}/rest/v3/devices",
    headers=headers,
    timeout=30,
)

if r.status_code != 200:
    print("Could not get devices.")
    print(r.text)
    exit()

if CAMERA_ID not in r.text:
    print("Camera not found.")
    exit()

print("Camera found")

#========================== Format 1 to extract footage ==========================

print("Creating playback ticket...")

r = requests.post(
    f"{BASE_URL}/rest/v3/login/tickets",
    headers=headers,
    json={},
    timeout=30,
)

if r.status_code != 200:
    print("Could not create playback ticket.")
    print(r.text)
    exit()

playback_ticket = r.json()["token"]

print("Playback ticket created")

position_ms = ticket_time_to_ms(TICKET_TIME)

media_url = f"{BASE_URL}/rest/v3/devices/{CAMERA_ID}/media.webm"

params = {
    "resolution": RESOLUTION,
    "positionMs": position_ms,
    "_ticket": playback_ticket,
    "Server-Guid": SERVER_GUID,
}

print("Downloading playback stream...")
print("Start time:", TICKET_TIME)
print("Position ms:", position_ms)

response = requests.get(
    media_url,
    params=params,
    stream=True,
    timeout=120,
)

print("HTTP Status:", response.status_code)
print("Content-Type:", response.headers.get("Content-Type"))

if response.status_code != 200:
    print("Download failed.")
    print(response.text)
    exit()

downloaded = 0
start_time = time.time()

with open(OUTPUT_FILE, "wb") as f:
    for chunk in response.iter_content(chunk_size=1024):
        if chunk:
            f.write(chunk)
            downloaded += len(chunk)

        if time.time() - start_time >= DURATION_SECONDS:
            break

response.close()

print("Download completed.")
print("Downloaded bytes:", downloaded)
print("Saved to:", OUTPUT_FILE)

if downloaded == 0:
    print("Warning: File is empty. Check timestamp and archive availability.")


#========================== Format 2 to extract footage ==========================


# position_ms = ticket_time_to_ms(TICKET_TIME)

# print("Checking footage at requested time...")

# footage_params = {
#     "startTimeMs": position_ms - 60000,
#     "endTimeMs": position_ms + 60000,
#     "preciseBounds": "true",
#     "periodType": "recording",
# }

# footage = requests.get(
#     f"{BASE_URL}/rest/v3/devices/{CAMERA_ID}/footage",
#     headers=headers,
#     params=footage_params,
#     timeout=30,
# )

# print("Footage status:", footage.status_code)
# print("Footage response:", footage.text[:1000])

# if footage.status_code != 200:
#     print("Could not check footage.")
#     exit()

# if footage.text.strip() == "[]":
#     print("No footage found at this time. Not downloading.")
#     exit()


# print("Creating playback ticket...")

# r = requests.post(
#     f"{BASE_URL}/rest/v3/login/tickets",
#     headers=headers,
#     json={},
#     timeout=30,
# )

# if r.status_code != 200:
#     print("Could not create playback ticket.")
#     print(r.text)
#     exit()

# playback_ticket = r.json()["token"]

# print("Playback ticket created")


# media_url = f"{BASE_URL}/rest/v3/devices/{CAMERA_ID}/media.webm"

# params = {
#     "positionMs": position_ms,
#     "durationMs": DURATION_SECONDS * 1000,
#     "resolution": RESOLUTION,
#     "stream": "secondary",
#     "accurateSeek": "true",
#     "download": "true",
#     "_ticket": playback_ticket,
# }

# print("\nDownload URL:")
# print(media_url + "?" + urlencode(params))

# response = requests.get(
#     media_url,
#     params=params,
#     stream=True,
#     timeout=300,
# )

# print("Media status:", response.status_code)
# print("Headers:", dict(response.headers))

# if response.status_code != 200:
#     print("Download failed.")
#     print(response.text)
#     exit()

# downloaded = 0

# with open(OUTPUT_FILE, "wb") as f:
#     for chunk in response.iter_content(chunk_size=1024 * 1024):
#         if chunk:
#             f.write(chunk)
#             downloaded += len(chunk)
#             print("Downloaded bytes:", downloaded)

# response.close()

# print("\nDone")
# print("Saved to:", OUTPUT_FILE)
# print("Final size:", os.path.getsize(OUTPUT_FILE))

# if os.path.getsize(OUTPUT_FILE) == 0:
#     print("File is empty. Timestamp likely has no recorded footage.")