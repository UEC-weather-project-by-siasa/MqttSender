# MQTT IoT Simulator

Python web app for simulating an ESP32/IoT device that publishes MQTT payloads to a broker.

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open `http://127.0.0.1:5001`

## Payload

Default publish topic:

```text
device/{device_id}/data
```

JSON payload:

```json
{
  "device_id": "weather-001",
  "net_mode": "WiFi",
  "ts": 1710000000000,
  "sensors": {
    "temperature": 29.4,
    "humidity": 68.2,
    "pressure": 1010,
    "wind_speed": 3.6,
    "wind_direction": 180
  }
}
```

When connected, the simulator publishes retained status to `device/{device_id}/status` as `{"status":"online"}` and sets the MQTT last will to `offline`, matching the sample firmware behavior.
