# MQTT IoT Simulator

เว็บแอพ Python สำหรับจำลอง ESP32/IoT device ส่ง MQTT payload ไปที่ broker

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

เปิดเว็บที่ `http://127.0.0.1:5000`

## Payload

ค่าเริ่มต้นส่งไปที่ topic:

```text
device/{device_id}/data
```

รูปแบบ JSON:

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

ตอนเชื่อมต่อจะส่ง retained status ไปที่ `device/{device_id}/status` เป็น `{"status":"online"}` และตั้ง last will เป็น `offline` เหมือน firmware ตัวอย่าง
