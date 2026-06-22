
from flask import Flask, request, Response, render_template_string
from datetime import datetime, timezone
import base64
import json
import math
import os
import re
import threading

app = Flask(__name__)

latest_tags = {}
tag_history = {}
lock = threading.Lock()

RFID_WEBHOOK_DEBUG = os.getenv("RFID_WEBHOOK_DEBUG", "1").strip().lower() in {"1", "true", "yes", "on"}
RFID_WEBHOOK_DEBUG_MAX_BODY = int(os.getenv("RFID_WEBHOOK_DEBUG_MAX_BODY", "4000").strip() or "4000")
RFID_HISTORY_LIMIT = max(3, int(os.getenv("RFID_HISTORY_LIMIT", "12").strip() or "12"))
RFID_RSSI_EWMA_ALPHA = min(1.0, max(0.05, float(os.getenv("RFID_RSSI_EWMA_ALPHA", "0.35").strip() or "0.35")))
RFID_REFERENCE_RSSI_1M_DBM = float(os.getenv("RFID_REFERENCE_RSSI_1M_DBM", "-45").strip() or "-45")
RFID_PATH_LOSS_EXPONENT = min(6.0, max(1.2, float(os.getenv("RFID_PATH_LOSS_EXPONENT", "2.2").strip() or "2.2")))
RFID_MOTION_SLOPE_DB_PER_SEC = max(0.05, float(os.getenv("RFID_MOTION_SLOPE_DB_PER_SEC", "1.0").strip() or "1.0"))
last_webhook_debug = {}


def _header_value(name):
    value = request.headers.get(name)
    if value is None:
        return None
    if name.lower() == "authorization":
        return "<redacted>"
    return value


def _event_debug_summary(event, index):
    if not isinstance(event, dict):
        return {
            "index": index,
            "valid": False,
            "reason": "event_not_object",
        }

    tag_event = event.get("tagInventoryEvent") or {}
    epc_raw = ""
    if isinstance(tag_event, dict):
        epc_raw = tag_event.get("epcHex") or tag_event.get("epc") or ""

    return {
        "index": index,
        "valid": True,
        "eventType": event.get("eventType"),
        "timestamp": event.get("timestamp"),
        "event_keys": sorted(list(event.keys())),
        "tag_keys": sorted(list(tag_event.keys())) if isinstance(tag_event, dict) else [],
        "epc_raw": epc_raw,
        "epc_normalized": normalize_epc(epc_raw),
        "antennaPort": tag_event.get("antennaPort") if isinstance(tag_event, dict) else None,
        "peakRssiCdbm": tag_event.get("peakRssiCdbm") if isinstance(tag_event, dict) else None,
        "frequency": tag_event.get("frequency") if isinstance(tag_event, dict) else None,
        "transmitPowerCdbm": tag_event.get("transmitPowerCdbm") if isinstance(tag_event, dict) else None,
    }


def normalize_epc(value):
    text = str(value or "").strip()
    if not text:
        return ""

    if text.lower().startswith("0x"):
        text = text[2:]

    text_no_sep = text.replace(" ", "").replace("-", "")

    if re.fullmatch(r"[0-9A-Fa-f]+", text_no_sep):
        return text_no_sep.upper()

    try:
        decoded = base64.b64decode(text_no_sep, validate=True)
        if decoded:
            return decoded.hex().upper()
    except Exception:
        pass

    return text_no_sep.upper()


def rssi_cdbm_to_dbm(value):
    if value is None:
        return None
    return float(value) / 100.0


def parse_iso_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    # Some RFID payloads use nanosecond precision; datetime only accepts microseconds.
    if "." in text:
        match = re.match(r"^(.*?\.)([0-9]+)([+-].*)$", text)
        if match:
            left, frac, tz = match.groups()
            text = f"{left}{frac[:6]}{tz}"

    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def antenna_to_angle_deg(antenna_port):
    try:
        port = int(antenna_port)
    except (TypeError, ValueError):
        return None

    port_map = {
        1: 315,
        2: 45,
        3: 135,
        4: 225,
    }
    if port in port_map:
        return port_map[port]

    return ((port - 1) * 90) % 360


def estimate_distance_meters(rssi_dbm):
    if rssi_dbm is None:
        return None

    exponent = (RFID_REFERENCE_RSSI_1M_DBM - float(rssi_dbm)) / (10.0 * RFID_PATH_LOSS_EXPONENT)
    distance_m = math.pow(10.0, exponent)
    return max(0.3, min(30.0, distance_m))


def estimate_motion(history_samples):
    if len(history_samples) < 3:
        return "stable", 0.25, 0.0

    first = history_samples[0]
    last = history_samples[-1]
    dt = (last["ts"] - first["ts"]).total_seconds()
    if dt <= 0:
        return "stable", 0.25, 0.0

    slope_db_per_sec = (last["rssi_ewma_dbm"] - first["rssi_ewma_dbm"]) / dt
    if slope_db_per_sec >= RFID_MOTION_SLOPE_DB_PER_SEC:
        motion = "approaching"
    elif slope_db_per_sec <= -RFID_MOTION_SLOPE_DB_PER_SEC:
        motion = "receding"
    else:
        motion = "stable"

    strength = min(1.0, abs(slope_db_per_sec) / (RFID_MOTION_SLOPE_DB_PER_SEC * 3.0))
    sample_bonus = min(1.0, len(history_samples) / float(RFID_HISTORY_LIMIT))
    confidence = max(0.2, min(1.0, 0.5 * strength + 0.5 * sample_bonus))
    return motion, confidence, slope_db_per_sec


def parse_events(body):
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        return [body]
    return []


@app.post("/webhook/rfid")
def rfid_webhook():
    debug_enabled = RFID_WEBHOOK_DEBUG or str(request.args.get("debug") or "").strip().lower() in {"1", "true", "yes", "on"}
    raw_body = request.get_data(as_text=True) or ""
    body = request.get_json(silent=True)

    events = parse_events(body) if body is not None else []

    if debug_enabled:
        payload_preview = raw_body
        if len(payload_preview) > RFID_WEBHOOK_DEBUG_MAX_BODY:
            payload_preview = payload_preview[:RFID_WEBHOOK_DEBUG_MAX_BODY] + "...<truncated>"

        debug_payload = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "method": request.method,
            "path": request.path,
            "remote_addr": request.remote_addr,
            "content_type": request.content_type,
            "content_length": request.content_length,
            "query_params": request.args.to_dict(flat=False),
            "headers": {
                "Host": _header_value("Host"),
                "Content-Type": _header_value("Content-Type"),
                "Content-Length": _header_value("Content-Length"),
                "Authorization": _header_value("Authorization"),
                "X-Forwarded-For": _header_value("X-Forwarded-For"),
                "X-Forwarded-Host": _header_value("X-Forwarded-Host"),
                "X-Forwarded-Proto": _header_value("X-Forwarded-Proto"),
            },
            "raw_body_preview": payload_preview,
            "json_parsed": body is not None,
            "json_type": type(body).__name__ if body is not None else None,
            "event_count": len(events),
            "events_preview": [_event_debug_summary(event, i) for i, event in enumerate(events[:5])],
        }

        with lock:
            last_webhook_debug.clear()
            last_webhook_debug.update(debug_payload)

        print("=" * 80)
        print("RFID webhook debug")
        print(json.dumps(debug_payload, indent=2))
        print("=" * 80)

    if body is None:
        return {"ok": False, "error": "Invalid JSON"}, 400

    now_dt = datetime.now(timezone.utc)

    with lock:
        for event in events:
            if not isinstance(event, dict):
                continue

            tag_event = event.get("tagInventoryEvent") or {}
            if not isinstance(tag_event, dict):
                continue

            epc_raw = tag_event.get("epcHex") or tag_event.get("epc")
            epc = normalize_epc(epc_raw)

            if not epc:
                continue

            rssi_dbm = rssi_cdbm_to_dbm(tag_event.get("peakRssiCdbm"))
            event_dt = parse_iso_datetime(tag_event.get("lastSeenTime") or event.get("timestamp")) or now_dt

            previous = latest_tags.get(epc) or {}
            prev_ewma = previous.get("rssi_ewma_dbm")
            if rssi_dbm is None:
                rssi_ewma_dbm = prev_ewma
            elif prev_ewma is None:
                rssi_ewma_dbm = rssi_dbm
            else:
                rssi_ewma_dbm = (RFID_RSSI_EWMA_ALPHA * rssi_dbm) + ((1.0 - RFID_RSSI_EWMA_ALPHA) * prev_ewma)

            if rssi_ewma_dbm is None:
                continue

            history = tag_history.get(epc, [])
            history.append(
                {
                    "ts": event_dt,
                    "rssi_ewma_dbm": float(rssi_ewma_dbm),
                    "phase_angle": tag_event.get("phaseAngle"),
                }
            )
            if len(history) > RFID_HISTORY_LIMIT:
                history = history[-RFID_HISTORY_LIMIT:]
            tag_history[epc] = history

            motion, motion_confidence, motion_slope_db_per_sec = estimate_motion(history)
            distance_m = estimate_distance_meters(rssi_ewma_dbm)

            latest_tags[epc] = {
                "epc": epc,
                "antenna_port": tag_event.get("antennaPort"),
                "rssi_dbm": rssi_dbm,
                "rssi_ewma_dbm": round(float(rssi_ewma_dbm), 2),
                "frequency": tag_event.get("frequency"),
                "transmit_power_dbm": rssi_cdbm_to_dbm(tag_event.get("transmitPowerCdbm")),
                "timestamp": event_dt.isoformat(),
                "last_seen_time": tag_event.get("lastSeenTime") or event_dt.isoformat(),
                "phase_angle": tag_event.get("phaseAngle"),
                "distance_m": round(distance_m, 2) if distance_m is not None else None,
                "motion": motion,
                "motion_confidence": round(float(motion_confidence), 2),
                "motion_slope_db_per_sec": round(float(motion_slope_db_per_sec), 3),
                "direction_angle_deg": antenna_to_angle_deg(tag_event.get("antennaPort")),
                "history_samples": len(history),
            }

    return {"ok": True, "received": len(events)}, 200


@app.get("/webhook/rfid/debug/last")
def webhook_debug_last():
    with lock:
        payload = dict(last_webhook_debug)
    return {"ok": True, "debug": payload}, 200


@app.get("/events")
def events():
    def stream():
        while True:
            with lock:
                tags = list(latest_tags.values())

            tags = sorted(tags, key=lambda x: x.get("timestamp") or "", reverse=True)[:10]

            yield f"data: {json.dumps(tags)}\n\n"

            import time
            time.sleep(1)

    return Response(stream(), mimetype="text/event-stream")


@app.get("/")
def home():
    return render_template_string(
        """
<!doctype html>
<html>
<head>
    <title>RFID Radar Demo</title>
    <style>
        body {
            margin: 0;
            background: #05070a;
            color: white;
            font-family: Arial, sans-serif;
            overflow: hidden;
        }

        #container {
            display: flex;
            height: 100vh;
        }

        #radarWrap {
            flex: 1;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        #panel {
            width: 360px;
            background: #101620;
            padding: 20px;
            overflow-y: auto;
            border-left: 1px solid #263244;
        }

        canvas {
            background: #020403;
            border-radius: 50%;
            box-shadow: 0 0 30px rgba(0, 255, 120, 0.25);
        }

        .tag {
            padding: 10px;
            margin-bottom: 10px;
            background: #192231;
            border-radius: 8px;
            font-size: 13px;
        }

        .near {
            color: #9cffb0;
        }

        .medium {
            color: #ffd36a;
        }

        .far {
            color: #ff8f8f;
        }
    </style>
</head>
<body>
<div id="container">
    <div id="radarWrap">
        <canvas id="radar" width="700" height="700"></canvas>
    </div>
    <div id="panel">
        <h2>RFID Radar Demo</h2>
        <p>Shows top 10 most recent tag reads.</p>
        <div id="tagList"></div>
    </div>
</div>

<script>
const canvas = document.getElementById("radar");
const ctx = canvas.getContext("2d");
const centerX = canvas.width / 2;
const centerY = canvas.height / 2;
let tags = [];
let sweepAngle = 0;

function classify(rssi) {
    if (rssi === null || rssi === undefined) return "far";
    if (rssi >= -40) return "near";
    if (rssi >= -55) return "medium";
    return "far";
}

function tagColor(level) {
    if (level === "near") return "#66ff88";
    if (level === "medium") return "#ffd36a";
    return "#ff6666";
}

function motionLabel(motion) {
    if (motion === "approaching") return "Approaching";
    if (motion === "receding") return "Receding";
    return "Stable";
}

function angleFromTag(tag, index) {
    if (tag.direction_angle_deg !== null && tag.direction_angle_deg !== undefined) {
        return (tag.direction_angle_deg * Math.PI) / 180;
    }
    return ((index * 47 + 20) * Math.PI) / 180;
}

function radiusFromDistance(distanceM) {
    if (distanceM === null || distanceM === undefined) return 250;
    const clamped = Math.max(0.3, Math.min(30, Number(distanceM)));
    const minR = 60;
    const maxR = 310;
    const minD = 0.3;
    const maxD = 30;
    const t = (Math.log(clamped) - Math.log(minD)) / (Math.log(maxD) - Math.log(minD));
    return minR + t * (maxR - minR);
}

function drawRadar() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    ctx.strokeStyle = "rgba(0, 255, 120, 0.25)";
    ctx.lineWidth = 1;

    for (let r = 100; r <= 300; r += 100) {
        ctx.beginPath();
        ctx.arc(centerX, centerY, r, 0, Math.PI * 2);
        ctx.stroke();
    }

    for (let a = 0; a < 360; a += 30) {
        const rad = a * Math.PI / 180;
        ctx.beginPath();
        ctx.moveTo(centerX, centerY);
        ctx.lineTo(centerX + Math.cos(rad) * 320, centerY + Math.sin(rad) * 320);
        ctx.stroke();
    }

    const sweepRad = sweepAngle * Math.PI / 180;
    ctx.strokeStyle = "rgba(0, 255, 120, 0.8)";
    ctx.lineWidth = 3;
    ctx.beginPath();
    ctx.moveTo(centerX, centerY);
    ctx.lineTo(centerX + Math.cos(sweepRad) * 320, centerY + Math.sin(sweepRad) * 320);
    ctx.stroke();

    tags.forEach((tag, index) => {
        const level = classify(tag.rssi_ewma_dbm ?? tag.rssi_dbm);
        const color = tagColor(level);

        const angle = angleFromTag(tag, index);
        const radius = radiusFromDistance(tag.distance_m);

        const x = centerX + Math.cos(angle) * radius;
        const y = centerY + Math.sin(angle) * radius;

        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(x, y, 9, 0, Math.PI * 2);
        ctx.fill();

        ctx.fillStyle = "white";
        ctx.font = "12px Arial";
        ctx.fillText(tag.epc.slice(-6), x + 12, y + 4);
    });

    sweepAngle = (sweepAngle + 2) % 360;
    requestAnimationFrame(drawRadar);
}

function updatePanel() {
    const div = document.getElementById("tagList");
    div.innerHTML = "";

    tags.forEach(tag => {
        const level = classify(tag.rssi_ewma_dbm ?? tag.rssi_dbm);
        const item = document.createElement("div");
        item.className = "tag";
        const motion = motionLabel(tag.motion);
        const confidencePct = Math.round((tag.motion_confidence || 0) * 100);

        item.innerHTML = `
            <div><b>EPC:</b> ${tag.epc}</div>
            <div><b>Antenna:</b> ${tag.antenna_port}</div>
            <div><b>RSSI:</b> <span class="${level}">${tag.rssi_dbm} dBm</span></div>
            <div><b>Smoothed RSSI:</b> ${tag.rssi_ewma_dbm} dBm</div>
            <div><b>Power:</b> ${tag.transmit_power_dbm} dBm</div>
            <div><b>Frequency:</b> ${tag.frequency}</div>
            <div><b>Phase:</b> ${tag.phase_angle}</div>
            <div><b>Distance (est):</b> ${tag.distance_m} m</div>
            <div><b>Motion:</b> ${motion} (${confidencePct}% conf)</div>
            <div><b>Direction Angle:</b> ${tag.direction_angle_deg}</div>
            <div><b>Nearness:</b> ${level}</div>
        `;

        div.appendChild(item);
    });
}

const source = new EventSource("/events");
source.onmessage = function(event) {
    tags = JSON.parse(event.data);
    updatePanel();
};

drawRadar();
</script>
</body>
</html>
        """
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
