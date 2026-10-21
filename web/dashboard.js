(() => {
  "use strict";
  const $ = (id) => document.getElementById(id);
  const map = L.map("map", { zoomControl: true, preferCanvas: true }).setView([20, 0], 2);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19, attribution: "&copy; OpenStreetMap contributors"
  }).addTo(map);
  const cellLayer = L.layerGroup().addTo(map);
  const cellLayers = new Map();
  let selectedCellId = null;
  let hasFittedCells = false;
  let retryDelay = 1000;
  let socket = null;
  let reconnectTimer = null;
  let toastTimer = null;
  let latestSnapshot = null;

  function showToast(message) {
    const toast = $("toast");
    toast.textContent = message;
    toast.classList.add("visible");
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => toast.classList.remove("visible"), 3200);
  }

  function esc(value) {
    return String(value ?? "—").replace(/[&<>"']/g, (char) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    })[char]);
  }

  function stamp(value) {
    if (!value) return "Unknown";
    const date = new Date(value);
    return Number.isNaN(date.valueOf()) ? "Unknown" : date.toLocaleString();
  }

  function riskText(cell) {
    const score = cell.risk?.score;
    if (score === null || score === undefined) return cell.risk?.status || "unavailable";
    if (score >= latestSnapshot.data.risk_thresholds.entry) return `High · ${(score * 100).toFixed(0)}%`;
    if (score >= latestSnapshot.data.risk_thresholds.clear) return `Elevated · ${(score * 100).toFixed(0)}%`;
    return `Below clear · ${(score * 100).toFixed(0)}%`;
  }

  function cellStyle(cell) {
    if (cell.freshness === "stale") return { color: "#7d898c", weight: 2, dashArray: "5 4", fillColor: "#657276", fillOpacity: 0.62 };
    const score = cell.risk?.score;
    let color = "#86adbf";
    if (typeof score === "number") {
      if (score >= latestSnapshot.data.risk_thresholds.entry) color = "#ff655f";
      else if (score >= latestSnapshot.data.risk_thresholds.clear) color = "#e8b65b";
      else color = "#75c59e";
    }
    return { color, weight: 1.5, fillColor: color, fillOpacity: Math.max(0.30, 0.76 * (cell.confidence ?? 0.45)) };
  }

  function boundsFor(cell) {
    const lat = cell.center.latitude;
    const lon = cell.center.longitude;
    const half = cell.cell_size_meters / 2;
    const metersPerLatDegree = 111320;
    const metersPerLonDegree = Math.max(1, metersPerLatDegree * Math.cos(lat * Math.PI / 180));
    const dLat = half / metersPerLatDegree;
    const dLon = half / metersPerLonDegree;
    return [[lat - dLat, lon - dLon], [lat + dLat, lon + dLon]];
  }

  function popup(cell) {
    return `<div class="popup-title">Cell ${esc(cell.id)}</div><div>${esc(riskText(cell))}</div>` +
      `<div class="popup-muted">${esc(cell.freshness)} · confidence ${cell.confidence == null ? "unknown" : `${(cell.confidence * 100).toFixed(0)}%`}</div>` +
      `<div class="popup-muted">Last seen ${esc(stamp(cell.last_seen_at))}</div>`;
  }

  function renderSnapshot(message) {
    if (!message || message.type !== "snapshot" || !message.data) return;
    latestSnapshot = message;
    const data = message.data;
    $("updated-at").textContent = `Updated ${stamp(data.generated_at)}`;
    $("cell-count").textContent = `${data.cells.length} ${data.cells.length === 1 ? "cell" : "cells"}`;
    $("grid-size").textContent = `${data.cell_size_meters} m cells`;
    $("telemetry-source").textContent = data.sources.telemetry;
    $("model-source").textContent = data.sources.model;
    $("weather-source").textContent = data.sources.weather;
    renderCells(data.cells);
    renderSelected(data.cells.find((cell) => cell.id === selectedCellId) || null);
    renderAlerts(data.alerts);
    renderHealth(data.health);
  }

  function renderCells(cells) {
    const currentIds = new Set(cells.map((cell) => cell.id));
    for (const [id, layer] of cellLayers.entries()) {
      if (!currentIds.has(id)) { cellLayer.removeLayer(layer); cellLayers.delete(id); }
    }
    for (const cell of cells) {
      const style = cellStyle(cell);
      let layer = cellLayers.get(cell.id);
      if (!layer) {
        layer = L.rectangle(boundsFor(cell), style).addTo(cellLayer);
        layer.on("click", () => {
          selectedCellId = cell.id;
          renderSelected(latestSnapshot?.data.cells.find((item) => item.id === cell.id) || null);
        });
        cellLayers.set(cell.id, layer);
      } else {
        layer.setBounds(boundsFor(cell));
        layer.setStyle(style);
      }
      layer.bindPopup(popup(cell));
    }
    if (!hasFittedCells && cells.length) {
      const group = L.featureGroup([...cellLayers.values()]);
      map.fitBounds(group.getBounds().pad(0.35), { maxZoom: 17 });
      hasFittedCells = true;
    }
  }

  function renderSelected(cell) {
    if (!cell) {
      $("selected-title").textContent = "Select a cell";
      $("selected-freshness").textContent = "NO SELECTION";
      $("selected-freshness").className = "pill neutral";
      $("selected-content").innerHTML = '<p class="empty-state">Choose a grid cell to inspect its latest readings and status.</p>';
      return;
    }
    selectedCellId = cell.id;
    $("selected-title").textContent = `Cell ${cell.id}`;
    const freshness = $("selected-freshness");
    freshness.textContent = cell.freshness.toUpperCase();
    freshness.className = `pill ${cell.freshness === "stale" ? "stale" : "fresh"}`;
    const sensorCards = cell.sensor_readings.map((reading) => `<div class="sensor-card">
      <span class="sensor-name">Channel ${reading.channel} · quantized</span>
      <span class="sensor-value">${reading.quantized_value == null ? "Unknown" : esc(reading.quantized_value)}</span>
      <span class="sensor-time">Seen ${esc(stamp(reading.timestamp))}</span>
      <span class="sensor-time">Confidence ${reading.confidence == null ? "unknown" : `${(reading.confidence * 100).toFixed(0)}%`}</span>
    </div>`).join("");
    const weather = cell.weather;
    const alert = cell.alert;
    $("selected-content").innerHTML = `<div class="cell-subtitle">Last frame ${esc(stamp(cell.last_seen_at))} · confidence ${cell.confidence == null ? "unknown" : `${(cell.confidence * 100).toFixed(0)}%`}</div>
      <div class="sensor-grid">${sensorCards}</div>
      <div class="details-list">
        <span>SVM output</span><span>${esc(riskText(cell))}</span>
        <span>Model source</span><span>${esc(cell.risk.source)}</span>
        <span>Risk evaluator state</span><span>${esc(cell.risk.status)}</span>
        <span>Weather temperature</span><span>${weather.temperature_c == null ? "Unknown" : `${esc(weather.temperature_c)} °C`}</span>
        <span>Weather wind</span><span>${weather.wind_speed_mps == null ? "Unknown" : `${esc(weather.wind_speed_mps)} m/s · ${esc(weather.wind_direction_degrees)}°`}</span>
        <span>Weather feature timestamps</span><span>Temp ${esc(stamp(weather.feature_timestamps?.temperature))}<br>Wind ${esc(stamp(weather.feature_timestamps?.wind_speed))}</span>
        <span>Weather freshness / source</span><span>${esc(weather.status)} · ${esc(weather.source)}</span>
        <span>Alert state</span><span>${!alert ? "No alert record" : !alert.active ? `Resolved ${esc(stamp(alert.resolved_at))}` : alert.acknowledged ? "Acknowledged · hazard active" : "Unacknowledged · hazard active"}</span>
        ${alert ? `<span>Alert ID / escalation</span><span>${esc(alert.id)} · level ${esc(alert.escalation_level)}</span><span>Last notification</span><span>${esc(stamp(alert.last_notified_at))}</span>` : ""}
      </div>`;
  }

  function renderAlerts(alerts) {
    $("alert-count").textContent = String(alerts.length);
    if (!alerts.length) {
      $("alerts-list").innerHTML = '<p class="empty-state">No active alerts reported.</p>';
      return;
    }
    const durableAck = Boolean(latestSnapshot?.data.health.acknowledgement_store.durable);
    $("alerts-list").innerHTML = alerts.map((alert) => `<article class="alert-card ${alert.acknowledged ? "acknowledged" : ""}">
      <div class="alert-top"><span class="alert-title">${alert.acknowledged ? "Acknowledged" : "Action required"} · cell ${esc(alert.cell_id)}</span><span class="alert-state">Level ${esc(alert.escalation_level)}</span></div>
      <div class="alert-meta">${esc(alert.id)}<br>Raised ${esc(stamp(alert.raised_at))} · last notified ${esc(stamp(alert.last_notified_at))}</div>
      <div class="alert-actions"><span class="alert-meta">${alert.acknowledged ? `Ack by ${esc(alert.acknowledged_by)} · hazard remains active` : "Hazard remains active until risk clears"}</span>
        ${alert.acknowledged ? "" : durableAck ? `<button class="button" data-ack="${esc(alert.id)}">Acknowledge</button>` : `<span class="alert-state">PostgreSQL required</span>`}</div>
    </article>`).join("");
    document.querySelectorAll("[data-ack]").forEach((button) => button.addEventListener("click", () => acknowledge(button.dataset.ack, button)));
  }

  async function acknowledge(alertId, button) {
    const name = window.prompt("Operator name for this acknowledgement:");
    if (!name || !name.trim()) return;
    button.disabled = true;
    try {
      const response = await fetch(`/api/v1/alerts/${encodeURIComponent(alertId)}/acknowledge`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ acknowledged_by: name.trim() })
      });
      if (!response.ok) {
        const detail = await response.json().catch(() => ({}));
        throw new Error(detail.detail || `HTTP ${response.status}`);
      }
      showToast("Acknowledgement committed. Hazard remains active until cleared.");
      await loadSnapshot();
    } catch (error) {
      showToast(`Acknowledgement not confirmed: ${error.message}`);
      button.disabled = false;
    }
  }

  function renderHealth(health) {
    const packets = health.udp_packet_stats;
    const db = health.telemetry_database;
    const ack = health.acknowledgement_store;
    const items = [
      ["API", health.api, "Snapshot from in-memory grid"],
      ["UDP packets", packets ? `${packets.accepted ?? "—"} accepted · ${packets.rejected ?? "—"} rejected` : "Unavailable", packets ? `${packets.listener_status ?? "listener unknown"} · sent ${packets.sent ?? "—"} · drop ${packets.dropped ?? "—"} · duplicate ${packets.duplicated ?? "—"}` : "No packet counter connected"],
      ["History database", db ? (db.database_connected === true ? "Connected" : db.database_connected === false ? "Disconnected" : "Unknown") : "Not configured", db ? `queue ${db.queue_depth}/${db.queue_capacity} · failures ${db.write_failures}` : "No persistence worker"],
      ["Alert acknowledgement store", ack.durable ? "PostgreSQL configured" : "Not durable", ack.database_connected_last_result == null ? "No acknowledgement write yet" : ack.database_connected_last_result ? "Last commit succeeded" : "Last commit failed"],
    ];
    $("health-content").innerHTML = items.map(([label, value, sub]) => `<div class="health-item"><span class="health-label">${esc(label)}</span><span class="health-value">${esc(value)}</span><span class="health-sub">${esc(sub)}</span></div>`).join("");
  }

  async function loadSnapshot() {
    const response = await fetch("/api/v1/map/snapshot", { cache: "no-store" });
    if (!response.ok) throw new Error(`snapshot HTTP ${response.status}`);
    renderSnapshot(await response.json());
  }

  function setConnection(connected) {
    $("connection-dot").className = `dot ${connected ? "online" : "offline"}`;
    $("connection-label").textContent = connected ? "Live updates" : "Reconnecting";
  }

  async function reconnect() {
    setConnection(false);
    try { await loadSnapshot(); } catch (_error) { /* retry below; keep last map visible */ }
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectSocket, retryDelay);
    retryDelay = Math.min(15000, retryDelay * 2);
  }

  function connectSocket() {
    const scheme = location.protocol === "https:" ? "wss:" : "ws:";
    socket = new WebSocket(`${scheme}//${location.host}/ws/map`);
    socket.onopen = () => { retryDelay = 1000; setConnection(true); };
    socket.onmessage = (event) => {
      try { renderSnapshot(JSON.parse(event.data)); } catch (_error) { /* ignore malformed update and keep last state */ }
    };
    socket.onerror = () => socket.close();
    socket.onclose = reconnect;
  }

  loadSnapshot().catch(() => setConnection(false)).finally(connectSocket);
})();

