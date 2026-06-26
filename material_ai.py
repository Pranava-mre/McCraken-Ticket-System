


# import time
# from datetime import datetime
# import requests


# # ==========================
# # CHANGE THESE VALUES
# # ==========================

# BEARER_TOKEN = "nxcdb-eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImRiNGE2NzNkLTlkNjMtNGMzYS1hODliLWM3Yzc5MTc3ZTFkYyJ9.eyJleHAiOjE3ODI1NzI2NTMsInN1YmpUeXAiOiJVc2VyIiwicHdkVGltZSI6MTc4MjMxMzQzOSwic2lkIjoiZDY2YTliNTYtMGRkNS00Yjg4LWEzZjAtNTE0M2RlYjI5YjA1IiwidHlwIjoiYWNjZXNzVG9rZW4iLCJhdWQiOiJodHRwczovL3N5bmMud2F2ZXZtcy5jb20vIGNsb3VkU3lzdGVtSWQ9KiIsImlhdCI6MTc4MjQ4NjI1Mywic3ViIjoiYndvb2RzQHNvcy1sbGMubmV0IiwiY2xpZW50X2lkIjoiIiwiaXNzIjoiY2RiIn0.oAWEglRNLyStE93UOd4lnuW7BxpytMODwenZjTgbfLmAy4Yf_5OJQ4k4v73PNJHIygSniDGHhmIWGhbeQ-fWhWfDqwdbIjqOaor2z8eeWO9iThfvTOKcr8JWaQ7ctTudvOvizsUxs5MbCTUarwvffyNMK9aFkE2eqE2jOFHea0fyQ4caudZSsj_eE1g06fazy1Umn20CxSizbG5NSyHqaTW2EIoB8Lb3Lve84VI0-Xx9KyAS7h8_V5qnBVFcxmAJ0U9gNutvmQt912lUTKaGFQ2VChbWHaSfE4Otg_Yd_RJV7oGEnyrRCFiWWOGgfS0Ct4aCumI33NasxW6FIkOKHw"
# BASE_URL = "https://edbde632-d846-49f5-8fc8-7fdaaa2504e7.relay-us-chi-2-prod-dp.vmsproxy.com"


# CAMERA_ID = "4f68efaf-da80-b7f7-94af-f895e6c42e5e"

# SERVER_GUID = "fafd22b6-2eb6-a286-1746-922a1d62e77c"

# TICKET_TIME = "2026-06-26T08:32:08"

# DURATION_SECONDS = 8

# OUTPUT_FILE = "truck_video.webm"

# RESOLUTION = "1024x452"


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

# position_ms = ticket_time_to_ms(TICKET_TIME)

# media_url = f"{BASE_URL}/rest/v3/devices/{CAMERA_ID}/media.webm"

# params = {
#     "resolution": RESOLUTION,
#     "positionMs": position_ms,
#     "_ticket": playback_ticket,
#     "Server-Guid": SERVER_GUID,
# }

# print("Downloading playback stream...")
# print("Start time:", TICKET_TIME)
# print("Position ms:", position_ms)

# response = requests.get(
#     media_url,
#     params=params,
#     stream=True,
#     timeout=120
# )

# print("HTTP Status:", response.status_code)
# print("Content-Type:", response.headers.get("Content-Type"))

# if response.status_code != 200:
#     print("Download failed.")
#     print(response.text)
#     exit()

# downloaded = 0
# start_time = time.time()

# with open(OUTPUT_FILE, "wb") as f:
#     for chunk in response.iter_content(chunk_size=1024):
#         if chunk:
#             f.write(chunk)
#             downloaded += len(chunk)

#         if time.time() - start_time >= DURATION_SECONDS:
#             break

# response.close()

# print("Download completed.")
# print("Downloaded bytes:", downloaded)
# print("Saved to:", OUTPUT_FILE)

# if downloaded == 0:
#     print("Warning: file is empty. Check timestamp and archive availability.")

import requests

# ==========================
# CHANGE THESE VALUES
# ==========================

BASE_URL = "https://edbde632-d846-49f5-8fc8-7fdaaa2504e7.relay-us-chi-2-prod-dp.vmsproxy.com"

USERNAME = "bwoods@sos-llc.net"

PASSWORD = "Walk36toolfun!"

# ==========================
# POSSIBLE LOGIN ENDPOINTS
# ==========================

login_endpoints = [
    "/rest/v3/login/sessions",
    "/rest/v2/login/sessions",
    "/rest/v1/login/sessions",
]

for endpoint in login_endpoints:

    print("=" * 60)
    print("Trying:", endpoint)

    try:

        response = requests.post(
            BASE_URL + endpoint,
            json={
                "username": USERNAME,
                "password": PASSWORD
            },
            timeout=30
        )

        print("Status Code:", response.status_code)

        try:
            print("Response:")
            print(response.json())
        except:
            print(response.text)

        if response.status_code in [200, 201]:
            print("\n✅ LOGIN SUCCESSFUL")

            data = response.json()

            print("\nReturned Keys:")

            for key in data.keys():
                print("  ", key)

            if "token" in data:
                print("\nBearer Token:")
                print(data["token"])

            break

    except Exception as e:

        print("Error:", e)

print("\nFinished.")