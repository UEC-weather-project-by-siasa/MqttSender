from __future__ import annotations

import random
import os
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4

from flask import Flask, jsonify, render_template, request
import paho.mqtt.client as mqtt


app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

DEFAULT_SENSORS: dict[str, dict[str, Any]] = {
    "temperature": {"label": "Temperature", "unit": "C", "min": 24.0, "max": 35.0, "decimals": 1, "cluster": 92},
    "humidity": {"label": "Humidity", "unit": "%", "min": 45.0, "max": 85.0, "decimals": 1, "cluster": 88},
    "pressure": {"label": "Pressure", "unit": "hPa", "min": 1002.0, "max": 1018.0, "decimals": 1, "cluster": 95},
    "wind_speed": {"label": "Wind Speed", "unit": "m/s", "min": 0.0, "max": 4.0, "decimals": 1, "cluster": 90},
    "wind_direction": {"label": "Wind Direction", "unit": "deg", "min": 0.0, "max": 359.0, "decimals": 1, "cluster": 50},
}


@dataclass
class JobState:
    id: str
    kind: str
    status: str = "running"
    sent: int = 0
    failed: int = 0
    total: int | None = None
    message: str = ""
    last_payload: dict[str, Any] | None = None
    stop_event: threading.Event | None = None


jobs: dict[str, JobState] = {}
jobs_lock = threading.Lock()
mqtt_lock = threading.Lock()
mqtt_logs: deque[dict[str, Any]] = deque(maxlen=160)
mqtt_state: dict[str, Any] = {
    "connected": False,
    "status": "disconnected",
    "host": "",
    "port": "",
    "device_id": "",
    "topic": "",
    "last_event": "",
    "last_at": "",
}


def now_label() -> str:
    return datetime.now().strftime("%H:%M:%S")


def add_mqtt_log(level: str, message: str, **meta: Any) -> None:
    entry = {
        "at": now_label(),
        "level": level,
        "message": message,
        "meta": meta,
    }
    with mqtt_lock:
        mqtt_logs.appendleft(entry)
        mqtt_state["last_event"] = message
        mqtt_state["last_at"] = entry["at"]


def set_mqtt_state(status: str, connected: bool, config: dict[str, Any] | None = None, topic: str = "") -> None:
    with mqtt_lock:
        mqtt_state["status"] = status
        mqtt_state["connected"] = connected
        if config:
            mqtt_state["host"] = config.get("host", "")
            mqtt_state["port"] = config.get("port", "")
            mqtt_state["device_id"] = config.get("device_id", "")
            mqtt_state["topic"] = topic or config.get("topic") or f"device/{config.get('device_id', '')}/data"


def mqtt_client(client_id: str, username: str, password: str, will_topic: str) -> mqtt.Client:
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    except AttributeError:
        client = mqtt.Client(client_id=client_id)

    if username or password:
        client.username_pw_set(username or None, password or None)
    client.will_set(will_topic, payload="offline", qos=1, retain=True)

    def on_disconnect(client: mqtt.Client, userdata: Any, *args: Any) -> None:
        reason_code = args[1] if len(args) > 1 else (args[0] if args else "")
        set_mqtt_state("disconnected", False)
        add_mqtt_log("info", "MQTT disconnected", reason=str(reason_code))

    client.on_disconnect = on_disconnect
    return client


def connect_client(config: dict[str, Any]) -> mqtt.Client:
    device_id = config["device_id"]
    topic = config.get("topic") or f"device/{device_id}/data"
    set_mqtt_state("connecting", False, config, topic)
    add_mqtt_log("info", "Connecting MQTT", host=config["host"], port=config["port"], device_id=device_id)
    client = mqtt_client(
        client_id=f"{device_id}-sim-{uuid4().hex[:8]}",
        username=config.get("username") or device_id,
        password=config.get("password") or config.get("device_key", ""),
        will_topic=f"device/{device_id}/status",
    )
    client.connect(config["host"], int(config["port"]), keepalive=60)
    client.loop_start()
    status_topic = f"device/{device_id}/status"
    client.publish(status_topic, '{"status":"online"}', qos=1, retain=True).wait_for_publish(timeout=5)
    set_mqtt_state("connected", True, config, topic)
    add_mqtt_log("success", "MQTT connected", status_topic=status_topic)
    return client


def publish_payloads(config: dict[str, Any], payloads: list[dict[str, Any]]) -> int:
    client = connect_client(config)
    sent = 0
    topic = config.get("topic") or f"device/{config['device_id']}/data"
    qos = int(config.get("qos", 0))
    retain = bool(config.get("retain", False))
    try:
        for payload in payloads:
            result = client.publish(topic, payload=str_json(payload), qos=qos, retain=retain)
            result.wait_for_publish(timeout=5)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish failed with code {result.rc}")
            sent += 1
            add_mqtt_log("success", "Published packet", topic=topic, ts=payload.get("ts"), sensors=len(payload.get("sensors", {})))
    finally:
        client.loop_stop()
        client.disconnect()
        set_mqtt_state("disconnected", False, config, topic)
    return sent


def str_json(payload: dict[str, Any]) -> str:
    import json

    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def parse_datetime_ms(value: str) -> int:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.astimezone()
    return int(dt.timestamp() * 1000)


def random_value(sensor: dict[str, Any]) -> float | int:
    value = random.uniform(float(sensor["min"]), float(sensor["max"]))
    decimals = int(sensor.get("decimals", 1))
    rounded = round(value, decimals)
    return int(rounded) if decimals == 0 else rounded


def format_sensor_value(value: float, sensor: dict[str, Any]) -> float | int:
    decimals = int(sensor.get("decimals", 1))
    rounded = round(value, decimals)
    return int(rounded) if decimals == 0 else rounded


def fixed_or_random(sensor: dict[str, Any], previous_values: dict[str, float | int] | None = None) -> float | int:
    if sensor.get("mode") == "fixed" and sensor.get("value") not in (None, ""):
        return format_sensor_value(float(sensor["value"]), sensor)

    key = sensor["key"]
    min_value = float(sensor["min"])
    max_value = float(sensor["max"])
    cluster = max(0.0, min(100.0, float(sensor.get("cluster", 85))))
    if not previous_values or key not in previous_values or cluster <= 0:
        return random_value(sensor)

    span = max_value - min_value
    max_delta = span * ((100.0 - cluster) / 100.0)
    if max_delta <= 0:
        return format_sensor_value(float(previous_values[key]), sensor)

    value = float(previous_values[key]) + random.uniform(-max_delta, max_delta)
    return format_sensor_value(max(min_value, min(max_value, value)), sensor)


def build_payload(
    config: dict[str, Any],
    sensors: list[dict[str, Any]],
    ts_ms: int | None = None,
    previous_values: dict[str, float | int] | None = None,
) -> dict[str, Any]:
    selected = {}
    for sensor in sensors:
        if not sensor.get("enabled", True):
            continue
        selected[sensor["key"]] = fixed_or_random(sensor, previous_values)
    if previous_values is not None:
        previous_values.clear()
        previous_values.update(selected)
    return {
        "device_id": config["device_id"],
        "net_mode": config.get("net_mode", "WiFi"),
        "ts": ts_ms if ts_ms is not None else int(time.time() * 1000),
        "sensors": selected,
    }


def validate_common(data: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    config = data.get("config") or {}
    sensors = data.get("sensors") or []
    required = ["host", "port", "device_id"]
    missing = [key for key in required if not str(config.get(key, "")).strip()]
    if missing:
        raise ValueError(f"Missing config: {', '.join(missing)}")
    if not sensors or not any(sensor.get("enabled", True) for sensor in sensors):
        raise ValueError("Enable at least one sensor")
    config["port"] = int(config.get("port", 1883))
    return config, sensors


def save_job(job: JobState) -> None:
    with jobs_lock:
        jobs[job.id] = job


def update_job(job_id: str, **changes: Any) -> None:
    with jobs_lock:
        job = jobs[job_id]
        for key, value in changes.items():
            setattr(job, key, value)


def run_stream(job_id: str, config: dict[str, Any], sensors: list[dict[str, Any]], interval: float) -> None:
    client = None
    topic = config.get("topic") or f"device/{config['device_id']}/data"
    qos = int(config.get("qos", 0))
    retain = bool(config.get("retain", False))
    previous_values: dict[str, float | int] = {}
    try:
        client = connect_client(config)
        while True:
            with jobs_lock:
                job = jobs[job_id]
                should_stop = job.stop_event.is_set() if job.stop_event else True
            if should_stop:
                update_job(job_id, status="stopped", message="Stopped by user")
                break

            payload = build_payload(config, sensors, previous_values=previous_values)
            result = client.publish(topic, payload=str_json(payload), qos=qos, retain=retain)
            result.wait_for_publish(timeout=5)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish failed with code {result.rc}")
            with jobs_lock:
                jobs[job_id].sent += 1
                jobs[job_id].last_payload = payload
            add_mqtt_log("success", "Published stream packet", topic=topic, ts=payload.get("ts"), sensors=len(payload.get("sensors", {})))
            time.sleep(max(0.2, interval))
    except Exception as exc:
        set_mqtt_state("error", False, config, topic)
        add_mqtt_log("error", "Stream failed", error=str(exc))
        update_job(job_id, status="failed", failed=1, message=str(exc))
    finally:
        if client:
            client.loop_stop()
            client.disconnect()
            set_mqtt_state("disconnected", False, config, topic)


def run_backfill(
    job_id: str,
    config: dict[str, Any],
    sensors: list[dict[str, Any]],
    start_ms: int,
    end_ms: int,
    step_sec: int,
    batch_size: int,
    batch_delay: float,
) -> None:
    total = max(0, ((end_ms - start_ms) // (step_sec * 1000)) + 1)
    update_job(job_id, total=total)
    client = None
    topic = config.get("topic") or f"device/{config['device_id']}/data"
    qos = int(config.get("qos", 0))
    retain = bool(config.get("retain", False))
    previous_values: dict[str, float | int] = {}
    try:
        client = connect_client(config)
        current = start_ms
        while current <= end_ms:
            sent_in_batch = 0
            first_ts = current
            last_payload = None
            for _ in range(batch_size):
                with jobs_lock:
                    job = jobs[job_id]
                    should_stop = job.stop_event.is_set() if job.stop_event else True
                if should_stop:
                    update_job(job_id, status="stopped", message="Stopped by user")
                    return
                if current > end_ms:
                    break

                payload = build_payload(config, sensors, ts_ms=current, previous_values=previous_values)
                result = client.publish(topic, payload=str_json(payload), qos=qos, retain=retain)
                result.wait_for_publish(timeout=5)
                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    raise RuntimeError(f"MQTT publish failed with code {result.rc}")
                sent_in_batch += 1
                last_payload = payload
                current += step_sec * 1000

            if sent_in_batch:
                with jobs_lock:
                    jobs[job_id].sent += sent_in_batch
                    jobs[job_id].last_payload = last_payload
                add_mqtt_log(
                    "success",
                    "Published backfill batch",
                    topic=topic,
                    packets=sent_in_batch,
                    first_ts=first_ts,
                    last_ts=last_payload.get("ts") if last_payload else first_ts,
                )
            if current <= end_ms and batch_delay > 0:
                time.sleep(batch_delay)
        update_job(job_id, status="done", message="Backfill complete")
        add_mqtt_log("success", "Backfill complete", total=total)
    except Exception as exc:
        set_mqtt_state("error", False, config, topic)
        add_mqtt_log("error", "Backfill failed", error=str(exc))
        update_job(job_id, status="failed", failed=1, message=str(exc))
    finally:
        if client:
            client.loop_stop()
            client.disconnect()
            set_mqtt_state("disconnected", False, config, topic)


@app.get("/")
def index() -> str:
    return render_template("index.html", default_sensors=DEFAULT_SENSORS)


@app.get("/api/defaults")
def defaults() -> Any:
    return jsonify({"sensors": DEFAULT_SENSORS})


@app.get("/api/mqtt-state")
def mqtt_status() -> Any:
    with mqtt_lock:
        return jsonify({"ok": True, "state": dict(mqtt_state), "logs": list(mqtt_logs)})


@app.post("/api/send-now")
def send_now() -> Any:
    try:
        config, sensors = validate_common(request.get_json(force=True))
        payload = build_payload(config, sensors)
        publish_payloads(config, [payload])
        return jsonify({"ok": True, "topic": config.get("topic") or f"device/{config['device_id']}/data", "payload": payload})
    except Exception as exc:
        add_mqtt_log("error", "Send failed", error=str(exc))
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/stream/start")
def stream_start() -> Any:
    try:
        data = request.get_json(force=True)
        config, sensors = validate_common(data)
        interval = float(data.get("interval", 5))
        job = JobState(id=uuid4().hex, kind="stream", stop_event=threading.Event())
        save_job(job)
        thread = threading.Thread(target=run_stream, args=(job.id, config, sensors, interval), daemon=True)
        thread.start()
        return jsonify({"ok": True, "job_id": job.id})
    except Exception as exc:
        add_mqtt_log("error", "Stream start failed", error=str(exc))
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/backfill/start")
def backfill_start() -> Any:
    try:
        data = request.get_json(force=True)
        config, sensors = validate_common(data)
        start_ms = parse_datetime_ms(data["start"])
        end_ms = parse_datetime_ms(data["end"])
        step_sec = int(data.get("step_sec", 60))
        if end_ms < start_ms:
            raise ValueError("End time must be after start time")
        if step_sec < 1:
            raise ValueError("Step must be at least 1 second")
        batch_size = int(data.get("batch_size", 10))
        batch_delay = float(data.get("batch_delay", 1))
        if batch_size < 1:
            raise ValueError("Packets per batch must be at least 1")
        if batch_size > 1000:
            raise ValueError("Packets per batch must be 1000 or less")
        if batch_delay < 0:
            raise ValueError("Batch delay cannot be negative")

        job = JobState(id=uuid4().hex, kind="backfill", stop_event=threading.Event())
        save_job(job)
        thread = threading.Thread(
            target=run_backfill,
            args=(job.id, config, sensors, start_ms, end_ms, step_sec, batch_size, batch_delay),
            daemon=True,
        )
        thread.start()
        return jsonify({"ok": True, "job_id": job.id})
    except Exception as exc:
        add_mqtt_log("error", "Backfill start failed", error=str(exc))
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/job/<job_id>/stop")
def stop_job(job_id: str) -> Any:
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Job not found"}), 404
        if job.stop_event:
            job.stop_event.set()
    return jsonify({"ok": True})


@app.get("/api/job/<job_id>")
def job_status(job_id: str) -> Any:
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Job not found"}), 404
        return jsonify(
            {
                "ok": True,
                "job": {
                    "id": job.id,
                    "kind": job.kind,
                    "status": job.status,
                    "sent": job.sent,
                    "failed": job.failed,
                    "total": job.total,
                    "message": job.message,
                    "last_payload": job.last_payload,
                },
            }
        )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5001"))
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True, use_reloader=False)
