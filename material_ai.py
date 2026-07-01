import os
import re
import pandas as pd
import json
from datetime import datetime
from wsgiref import headers
from zoneinfo import ZoneInfo
from urllib.parse import urlencode
import requests
from dotenv import load_dotenv
import time
load_dotenv()


CLOUD_BASE_URL = os.environ.get("WAVE_CLOUD_BASE_URL", "https://sync.wavevms.com")
SYSTEM_ID = os.environ.get("WAVE_SYSTEM_ID", "edbde632-d846-49f5-8fc8-7fdaaa2504e7")
BASE_URL = f"https://{SYSTEM_ID}.relay.vmsproxy.com"
USERNAME = os.environ.get("WAVE_USERNAME")
PASSWORD = os.environ.get("WAVE_PASSWORD")
CAMERA_ID = os.environ.get("WAVE_CAMERA_ID")
SERVER_GUID = os.environ.get("WAVE_SERVER_GUID")
TIMEZONE = os.environ.get("APP_TIMEZONE", "America/Chicago")
DURATION_SECONDS = int(os.environ.get("WAVE_FOOTAGE_DURATION_SECONDS", 8))
OUTPUT_FILE = f"truck_video.webm"
RESOLUTION = "1024x452"


def estimate_max_bytes(bitrate_mbps, duration_seconds, safety_factor=2.5):
    return int((bitrate_mbps * 1_000_000 / 8) * duration_seconds * safety_factor)

def ticket_time_to_ms(ticket_time: str) -> int:
    dt = datetime.strptime(ticket_time, "%Y-%m-%dT%H:%M:%S")
    dt = dt.replace(tzinfo=ZoneInfo(TIMEZONE))
    return int(dt.timestamp() * 1000)

def get_cloud_access_token():
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
    if r.status_code != 200:
        print(r.text)
        return None
    return r

def check_relay_url(access_token):
    print("Checking relay URL...")
    r = requests.get(
        f"{BASE_URL}/rest/v4/login/sessions/{access_token}",
        timeout=30,
        allow_redirects=True,
    )
    if r.status_code != 200:
        print("Cannot connect to relay.")
        exit()
    return r

def check_wave_connection(access_token, BASE_URL_REDIRECT_ROOT):
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    print("Checking WAVE connection...")
    r = requests.get(
        f"{BASE_URL_REDIRECT_ROOT}/rest/v4/servers",
        headers = headers,
        timeout=30,
    )
    if r.status_code != 200:
        print("Cannot connect to WAVE.")
        return None
    return r

def check_camera(access_token, BASE_URL_REDIRECT_ROOT):
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    r = requests.get(
        f"{BASE_URL_REDIRECT_ROOT}/rest/v4/devices",
        headers=headers,
        timeout=30,
    )
    if r.status_code != 200:
        print("Could not get devices.")
        print(r.text)
        return None

    if CAMERA_ID not in r.text:
        print("Camera not found.")
        return None
    return r

def check_device_details(access_token, BASE_URL_REDIRECT_ROOT):
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    r = requests.get(
        f"{BASE_URL_REDIRECT_ROOT}/rest/v4/devices/{CAMERA_ID}",
        headers=headers,
        timeout=30,
    )
    if r.status_code != 200:
        print("Could not get device details.")
        print(r.text)
        return None

    if CAMERA_ID not in r.text:
        print("Camera not found.")
        return None
    
    return r

def create_playback_ticket(access_token, BASE_URL_REDIRECT_ROOT):
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    r = requests.post(
        f"{BASE_URL_REDIRECT_ROOT}/rest/v3/login/tickets",
        headers=headers,
        json={},
        timeout=30,
    )

    if r.status_code != 200:
        print("Could not create playback ticket.")
        print(r.text)
        return None

    playback_ticket = r.json()["token"]
    return playback_ticket

    position_ms = ticket_time_to_ms(TICKET_TIME)

def check_footage_at_timestamp(BASE_URL_REDIRECT_ROOT, playback_ticket, ticket_time):
    position_ms = ticket_time_to_ms(ticket_time)
    end_position_ms = position_ms + (DURATION_SECONDS * 1000)
    footage = requests.get(
        f"{BASE_URL_REDIRECT_ROOT}/rest/v4/devices/{CAMERA_ID}/footage",
        params={
            "startTimeMs": position_ms,
            "endTimeMs": end_position_ms,
            "periodType": "recording",
            "preciseBounds": "true",
            "_ticket": playback_ticket,
        },
        timeout=30,
    )
    if footage.status_code != 200:
        print("Could not check footage.")
        print(footage.text)
        return None
    return footage

def download_playback_stream(BASE_URL_REDIRECT_ROOT, playback_ticket,ticket_time):
    position_ms = ticket_time_to_ms(ticket_time)

    media_url = f"{BASE_URL_REDIRECT_ROOT}/rest/v3/devices/{CAMERA_ID}/media.mp4"

    params = {
        "resolution": RESOLUTION,
        "positionMs": position_ms,
        "_ticket": playback_ticket,
        "Server-Guid": SERVER_GUID,
    }

    response = requests.get(
        media_url,
        params=params,
        stream=True,
        timeout=120,
    )

    if response.status_code != 200:
        print("Download failed.")
        print(response.text)
        return None

    downloaded = 0
    start_time = time.time()
    OUTPUT_FILE_NAME = f"truck_video_{ticket_time.replace(':', '-')}.mp4"

    with open(OUTPUT_FILE_NAME, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)

            if time.time() - start_time >= DURATION_SECONDS:
                break

    response.close()
    return OUTPUT_FILE_NAME



def download_playback_stream_v2(BASE_URL_REDIRECT_ROOT, playback_ticket,ticket_time):
    position_ms = ticket_time_to_ms(ticket_time)
    end_position_ms = position_ms + (DURATION_SECONDS * 1000)

    media_url = f"{BASE_URL_REDIRECT_ROOT}/rest/v3/devices/{CAMERA_ID}/media.mp4"
    params = {
        "resolution": RESOLUTION,
        "positionMs": position_ms,
        "accurateSeek":"false",
        "stream":"secondary",
        "endPositionMs": end_position_ms,
        "_ticket": playback_ticket,
        "download": "true",
    }
    response = requests.get(
        media_url,
        params=params,
        stream=True,
        timeout=1000,
    )
    print("############################################################# Status:", response.status_code)
    print("############################################################# URL:", response.url)
    print("############################################################# Headers:", dict(response.headers))
    if response.status_code != 200:
        print("Download failed.")
        print(response.text)
        return None
    downloaded = 0
    output_file_name = f"truck_video_{ticket_time.replace(':', '-')}_V2.mp4"
    with open(output_file_name, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
    response.close()
    if downloaded == 0:
        print("File is empty. Timestamp likely has no recorded footage.")
        return None
    print("Saved:", output_file_name)
    print("Bytes:", downloaded)
    return output_file_name



def download_playback_stream_v3(BASE_URL_REDIRECT_ROOT, playback_ticket,ticket_time, output_file_name):
    position_ms = ticket_time_to_ms(ticket_time)

    media_url = f"{BASE_URL_REDIRECT_ROOT}/rest/v3/devices/{CAMERA_ID}/media.mpjpeg"

    params = {
        "positionMs": position_ms,
        "stream": "primary",
        "resolution": RESOLUTION,
        "accurateSeek": "true",
        "_ticket": playback_ticket,
    }

    response = requests.get(
        media_url,
        params=params,
        stream=True,
        timeout=120,
    )

    if response.status_code != 200:
        print("Snapshot request failed")
        print(response.text)
        return None
    
    buffer = b""

    for chunk in response.iter_content(chunk_size=4096):
        if chunk:
            buffer += chunk

            start = buffer.find(b"\xff\xd8")
            end = buffer.find(b"\xff\xd9")

            if start != -1 and end != -1 and end > start:
                jpg_data = buffer[start:end + 2]

                with open(output_file_name, "wb") as f:
                    f.write(jpg_data)

                response.close()
                return output_file_name

    response.close()
    print("No JPEG frame found")
    return None


if __name__ == "__main__":
    df = pd.read_csv("latest_100.csv")

    TICKET_TIMES = df["created_at"].tolist()
    TICKET_NUMBERS = df["ticket_number"].tolist()
    MATERIAL_NAMES = df["material_name_snapshot"].tolist()
    total_times = len(TICKET_TIMES)
    print(f"Total ticket times to process: {total_times}")
    access_token_response = get_cloud_access_token()
    if access_token_response is None:
        exit()

    access_token = access_token_response.json()["access_token"]
    if not access_token.startswith("nxcdb-"):
        access_token = "nxcdb-" + access_token

    relay_response = check_relay_url(access_token)
    BASE_URL_REDIRECT_ROOT = relay_response.url.split("/rest/v4/login/sessions/")[0]

    wave_response = check_wave_connection(access_token, BASE_URL_REDIRECT_ROOT)
    if wave_response is None:
        exit()

    camera_response = check_camera(access_token, BASE_URL_REDIRECT_ROOT)
    if camera_response is None:
        exit()
    device_details_response = check_device_details(access_token, BASE_URL_REDIRECT_ROOT)
    if device_details_response is None:
        exit()
    print(DURATION_SECONDS)


    for i, TICKET_TIME in enumerate(TICKET_TIMES):
        counter = i + 1
        TKT_NUMBER = TICKET_NUMBERS[i]
        MATERIAL_NAME = MATERIAL_NAMES[i]
        MATERIAL_NAME = re.sub(r'[<>:"/\\|?*]', "_", MATERIAL_NAME)
        folder = os.path.join("Materials", MATERIAL_NAME)
        os.makedirs(folder, exist_ok=True)

        playback_ticket = create_playback_ticket(access_token, BASE_URL_REDIRECT_ROOT)
        if playback_ticket is None:
            print(f"File {counter}/{total_times} could not create playback ticket.")
            continue

        footage_response = check_footage_at_timestamp(BASE_URL_REDIRECT_ROOT, playback_ticket, TICKET_TIME)
        if footage_response is None:
            print(f"File {counter}/{total_times} could not check footage.")
            continue

        playback_ticket = create_playback_ticket(access_token, BASE_URL_REDIRECT_ROOT)
        if playback_ticket is None:
            print(f"File {counter}/{total_times} could not create playback ticket.")
            continue
        Output_file_name = os.path.join(
            folder,
            f"Truck_image_{TICKET_TIME.replace(':', '-')}_{TKT_NUMBER}.jpeg"
        )
        downloaded_file = download_playback_stream_v3(BASE_URL_REDIRECT_ROOT, playback_ticket, TICKET_TIME, Output_file_name)
        if downloaded_file is None:
            print(f"File {counter}/{total_times} download failed.")
            continue

        if os.path.getsize(downloaded_file) == 0:
            print(f"File {counter}/{total_times} is empty. Timestamp likely has no recorded footage.")
            continue

        print(f"File {counter}/{total_times} saved to:", downloaded_file)
