const sensorTable = document.querySelector("#sensorTable");
const form = document.querySelector("#simForm");
const output = document.querySelector("#output");
const jobStatus = document.querySelector("#jobStatus");
const sentCounter = document.querySelector("#sentCounter");
const topicPreview = document.querySelector("#topicPreview");
const lastEvent = document.querySelector("#lastEvent");
const mqttDot = document.querySelector("#mqttDot");
const logDot = document.querySelector("#logDot");
const mqttStatusText = document.querySelector("#mqttStatusText");
const logStatusText = document.querySelector("#logStatusText");
const logHost = document.querySelector("#logHost");
const logList = document.querySelector("#logList");

let sensors = Object.entries(window.DEFAULT_SENSORS).map(([key, spec]) => ({
  key,
  label: spec.label,
  unit: spec.unit,
  min: spec.min,
  max: spec.max,
  decimals: spec.decimals,
  cluster: spec.cluster ?? 85,
  enabled: true,
  mode: "random",
  value: "",
}));
let activeJobId = null;
let pollTimer = null;

function renderSensors() {
  sensorTable.innerHTML = "";
  sensors.forEach((sensor, index) => {
    const row = document.createElement("div");
    row.className = "sensor-row";
    row.innerHTML = `
      <label class="toggle"><input type="checkbox" ${sensor.enabled ? "checked" : ""} data-field="enabled"> <span></span></label>
      <input value="${sensor.key}" data-field="key" aria-label="sensor key" placeholder="temperature">
      <input value="${sensor.label}" data-field="label" aria-label="sensor label" placeholder="Temperature">
      <input type="number" step="any" value="${sensor.min}" data-field="min" aria-label="min">
      <input type="number" step="any" value="${sensor.max}" data-field="max" aria-label="max">
      <input type="number" min="0" max="100" value="${sensor.cluster}" data-field="cluster" aria-label="cluster percent" title="Higher values keep random values closer to the previous reading">
      <select data-field="mode" aria-label="mode">
        <option value="random" ${sensor.mode === "random" ? "selected" : ""}>random</option>
        <option value="fixed" ${sensor.mode === "fixed" ? "selected" : ""}>fixed</option>
      </select>
      <input type="number" step="any" value="${sensor.value}" data-field="value" placeholder="fixed">
      <input type="number" min="0" max="4" value="${sensor.decimals}" data-field="decimals" aria-label="decimals">
      <button class="icon-btn" type="button" data-remove="${index}" title="remove">×</button>
    `;
    row.querySelectorAll("[data-field]").forEach((input) => {
      input.addEventListener("input", () => updateSensor(index, input));
      input.addEventListener("change", () => updateSensor(index, input));
    });
    row.querySelector("[data-remove]").addEventListener("click", () => {
      sensors.splice(index, 1);
      renderSensors();
    });
    sensorTable.appendChild(row);
  });
}

function updateSensor(index, input) {
  const field = input.dataset.field;
  if (field === "enabled") {
    sensors[index][field] = input.checked;
  } else if (["min", "max", "decimals", "cluster"].includes(field)) {
    sensors[index][field] = Number(input.value);
  } else {
    sensors[index][field] = input.value;
  }
}

function collectPayloadBase() {
  const data = new FormData(form);
  return {
    config: {
      host: data.get("host"),
      port: Number(data.get("port") || 1883),
      device_id: data.get("device_id"),
      device_key: data.get("device_key"),
      username: data.get("username"),
      password: data.get("device_key"),
      net_mode: data.get("net_mode"),
      topic: data.get("topic"),
      qos: Number(data.get("qos") || 0),
      retain: data.get("retain") === "on",
    },
    sensors: sensors.map((sensor) => ({ ...sensor })),
  };
}

async function postJson(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const json = await res.json();
  if (!res.ok || json.ok === false) throw new Error(json.error || "Request failed");
  return json;
}

function show(data) {
  output.textContent = JSON.stringify(data, null, 2);
}

function setStatus(text, mood = "ready") {
  jobStatus.textContent = text;
  jobStatus.dataset.mood = mood;
}

function currentTopic() {
  const data = new FormData(form);
  const deviceId = data.get("device_id") || "weather-001";
  return data.get("topic") || `device/${deviceId}/data`;
}

function refreshTopicPreview() {
  topicPreview.textContent = currentTopic();
}

function setConnectionVisual(state) {
  const mood = state.connected ? "connected" : state.status === "error" ? "error" : state.status || "disconnected";
  mqttDot.dataset.mood = mood;
  logDot.dataset.mood = mood;
  const text = state.connected ? "Connected" : state.status === "connecting" ? "Connecting" : state.status === "error" ? "Error" : "Disconnected";
  mqttStatusText.textContent = text;
  logStatusText.textContent = text;
  logHost.textContent = state.host ? `${state.host}:${state.port} · ${state.device_id || "device"}` : "No broker";
  lastEvent.textContent = state.last_event || "Waiting";
}

function renderLogs(logs) {
  if (!logs.length) {
    logList.innerHTML = `<div class="empty-log">No MQTT events yet</div>`;
    return;
  }
  logList.innerHTML = logs
    .map((entry) => {
      const meta = entry.meta && Object.keys(entry.meta).length
        ? `<small>${Object.entries(entry.meta).map(([key, value]) => `${key}: ${value}`).join(" · ")}</small>`
        : "";
      return `
        <div class="log-row" data-level="${entry.level}">
          <time>${entry.at}</time>
          <div>
            <strong>${entry.message}</strong>
            ${meta}
          </div>
        </div>
      `;
    })
    .join("");
}

async function refreshMqttState() {
  try {
    const json = await fetch("/api/mqtt-state").then((res) => res.json());
    if (!json.ok) return;
    setConnectionVisual(json.state);
    renderLogs(json.logs || []);
  } catch (error) {
    setConnectionVisual({ status: "error", connected: false, last_event: error.message });
  }
}

function startPolling(jobId) {
  activeJobId = jobId;
  clearInterval(pollTimer);
  pollTimer = setInterval(async () => {
    try {
      const json = await fetch(`/api/job/${jobId}`).then((res) => res.json());
      if (!json.ok) return;
      const job = json.job;
      setStatus(`${job.kind}: ${job.status}`, job.status);
      sentCounter.textContent = `${job.sent}${job.total ? ` / ${job.total}` : ""}`;
      show(job);
      refreshMqttState();
      if (["done", "failed", "stopped"].includes(job.status)) {
        clearInterval(pollTimer);
        activeJobId = null;
      }
    } catch (error) {
      setStatus(error.message, "failed");
    }
  }, 700);
}

document.querySelector("#addSensor").addEventListener("click", () => {
  sensors.push({
    key: `sensor_${sensors.length + 1}`,
    label: "Custom Sensor",
    unit: "",
    min: 0,
    max: 100,
    decimals: 1,
    cluster: 85,
    enabled: true,
    mode: "random",
    value: "",
  });
  renderSensors();
});

document.querySelector("#sendNow").addEventListener("click", async () => {
  try {
    setStatus("Sending...", "running");
    const json = await postJson("/api/send-now", collectPayloadBase());
    setStatus("Sent", "done");
    sentCounter.textContent = "1";
    show(json);
    refreshMqttState();
  } catch (error) {
    setStatus("Failed", "failed");
    show({ ok: false, error: error.message });
  }
});

document.querySelector("#startStream").addEventListener("click", async () => {
  try {
    const body = collectPayloadBase();
    body.interval = Number(document.querySelector("#streamInterval").value || 5);
    const json = await postJson("/api/stream/start", body);
    setStatus("stream: running", "running");
    startPolling(json.job_id);
  } catch (error) {
    setStatus("Failed", "failed");
    show({ ok: false, error: error.message });
  }
});

async function stopActiveJob() {
  if (!activeJobId) return;
  await postJson(`/api/job/${activeJobId}/stop`, {});
  refreshMqttState();
}

document.querySelector("#stopJob").addEventListener("click", stopActiveJob);
document.querySelector("#stopBackfill").addEventListener("click", stopActiveJob);

document.querySelector("#startBackfill").addEventListener("click", async () => {
  try {
    const body = collectPayloadBase();
    body.start = document.querySelector("#backfillStart").value;
    body.end = document.querySelector("#backfillEnd").value;
    body.step_sec = Number(document.querySelector("#backfillStep").value || 60);
    body.batch_size = Number(document.querySelector("#backfillBatchSize").value || 10);
    body.batch_delay = Number(document.querySelector("#backfillBatchDelay").value || 1);
    const json = await postJson("/api/backfill/start", body);
    setStatus("backfill: running", "running");
    startPolling(json.job_id);
  } catch (error) {
    setStatus("Failed", "failed");
    show({ ok: false, error: error.message });
  }
});

document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((item) => item.classList.remove("active"));
    document.querySelectorAll(".tab-view").forEach((item) => item.classList.remove("active"));
    tab.classList.add("active");
    document.querySelector(`#tab-${tab.dataset.tab}`).classList.add("active");
  });
});

function setDefaultBackfill() {
  const end = new Date();
  const start = new Date(end.getTime() - 60 * 60 * 1000);
  const toLocal = (date) => {
    const shifted = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
    return shifted.toISOString().slice(0, 16);
  };
  document.querySelector("#backfillStart").value = toLocal(start);
  document.querySelector("#backfillEnd").value = toLocal(end);
}

renderSensors();
setDefaultBackfill();
refreshTopicPreview();
refreshMqttState();
form.addEventListener("input", refreshTopicPreview);
form.addEventListener("change", refreshTopicPreview);
document.querySelector("#refreshLogs").addEventListener("click", refreshMqttState);
setInterval(() => {
  if (!activeJobId) refreshMqttState();
}, 15000);
