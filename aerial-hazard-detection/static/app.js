
const meta = {
    "MQ-3": ["Alcohol vapour", "mg/L"],
    "MQ-4": ["Methane", "ppm"],
    "MQ-7": ["Carbon monoxide", "ppm"],
    "MQ-8": ["Hydrogen", "ppm"],
    "MQ-9": ["CO / combustible response", "relative units"],
    "MQ-135": ["Air quality response", "relative units"]
};

const $ = id => document.getElementById(id);

let map, layer, liveGridLayer, chart;

function cls(r) {
    return r === "HIGH" ? "high" :
        r === "WARNING" ? "warning" : "normal";
}

function render(r) {
    $("risk").textContent = r.risk;

    $("risk").style.color =
        r.risk === "HIGH" ? "#d84747" :
        r.risk === "WARNING" ? "#d99a25" : "#16976b";

    $("score").textContent = r.score;
    $("meter").style.width = r.score + "%";

    $("meter").style.background =
        r.risk === "HIGH" ? "#d84747" :
        r.risk === "WARNING" ? "#d99a25" : "#16976b";

    $("updated").textContent =
        "Updated " + new Date(r.timestamp).toLocaleTimeString();

    $("banner").className =
        r.risk === "NORMAL" ? "banner hide" : "banner";

    $("banner").textContent =
        r.risk === "NORMAL"
            ? ""
            : r.risk + " DEMO SCENARIO — model score " +
              r.score +
              "%. Simulated values only; do not use for real safety decisions.";

    $("sensors").innerHTML = r.sensor_details.map(s => `
        <article class="sensor ${cls(r.risk)}">
            <div class="sensortop">
                <div>
                    <b>${s.id}</b>
                    <div class="gas">${s.gas}</div>
                </div>
                <span class="tag ${cls(r.risk)}">${r.risk}</span>
            </div>
            <div class="reading">
                ${Number(s.value).toLocaleString()}
                <small>${s.unit}</small>
            </div>
            <div class="sensorfoot">● Simulated channel</div>
        </article>
    `).join("");
}

async function refresh(force = false) {
    try {
        const response = await fetch(
            "/api/readings/latest" +
            (force ? "?scenario=hazard" : ""),
            { cache: "no-store" }
        );

        if (!response.ok) {
            throw new Error("Latest reading request failed");
        }

        const r = await response.json();
        render(r);

        const statusResponse = await fetch("/api/status", {
            cache: "no-store"
        });

        if (!statusResponse.ok) {
            throw new Error("Status request failed");
        }

        const status = await statusResponse.json();
        $("count").textContent = status.reading_count;

        await Promise.all([history(), alerts()]);
    } catch (e) {
        $("updated").textContent =
            "Connection error — check Flask server";

        console.error("Dashboard refresh error:", e);
    }
}

async function history() {
    const response = await fetch("/api/readings/history", {
        cache: "no-store"
    });

    if (!response.ok) {
        throw new Error("History request failed");
    }

    const rows = await response.json();
    const key = $("sensor-choice").value;

    chart.data.labels = rows.map(
        x => new Date(x.timestamp).toLocaleTimeString()
    );

    chart.data.datasets[0].data = rows.map(x => x[key]);
    chart.data.datasets[0].label = key.toUpperCase();

    chart.update();
}

async function alerts() {
    const response = await fetch("/api/alerts", {
        cache: "no-store"
    });

    if (!response.ok) {
        throw new Error("Alerts request failed");
    }

    const rows = await response.json();

    $("alerts").innerHTML = rows.length
        ? rows.map(x => `
            <tr>
                <td>${new Date(x.timestamp).toLocaleTimeString()}</td>
                <td>
                    <span class="status ${x.risk}">${x.risk}</span>
                </td>
                <td>${x.score}%</td>
                <td>${x.mq4}</td>
                <td>${x.mq7}</td>
                <td>${x.mq8}</td>
            </tr>
        `).join("")
        : '<tr><td colspan="6">No alerts yet. Monitoring is active.</td></tr>';
}

map = L.map("map").setView([18.5204, 73.8567], 16);

L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap"
}).addTo(map);

layer = L.layerGroup().addTo(map);
liveGridLayer = L.markerClusterGroup({
    maxClusterRadius: 34,
    spiderfyOnMaxZoom: true,
    showCoverageOnHover: false,
    zoomToBoundsOnClick: true,
    disableClusteringAtZoom: 18,
    animate: false
}).addTo(map);

async function updateLiveGrid() {
    try {
        const response = await fetch("/api/grid/live", {
            cache: "no-store"
        });

        if (!response.ok) {
            throw new Error(
                "Live grid request failed: " + response.status
            );
        }

        const cells = await response.json();

        liveGridLayer.clearLayers();

        const bounds = [];

        cells.forEach(cell => {
            if (cell.latitude == null || cell.longitude == null) {
                return;
            }

            const lat = Number(cell.latitude);
            const lon = Number(cell.longitude);

            if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
                return;
            }

            const score = cell.risk_score == null
                ? NaN
                : Number(cell.risk_score);

            const receivedAt = cell.last_received
                ? new Date(cell.last_received)
                : null;

            const ageSeconds = receivedAt &&
                Number.isFinite(receivedAt.getTime())
                ? Math.max(
                    0,
                    (Date.now() - receivedAt.getTime()) / 1000
                )
                : NaN;

            const stale =
                !Number.isFinite(ageSeconds) || ageSeconds > 15;

            let risk = String(
                cell.risk_level || "UNKNOWN"
            ).toUpperCase();

            if (Number.isFinite(score)) {
                if (score >= 75) {
                    risk = "HIGH";
                } else if (score >= 40) {
                    risk = "WARNING";
                } else {
                    risk = "NORMAL";
                }
            }

            let color = "#64748b";

            if (risk === "HIGH") {
                color = "#dc2626";
            } else if (risk === "WARNING") {
                color = "#f59e0b";
            } else if (risk === "NORMAL") {
                color = "#16a34a";
            }

            const marker = L.circleMarker([lat, lon], {
                radius: 7,
                color: stale ? "#475569" : "#ffffff",
                weight: stale ? 2 : 1.5,
                fillColor: color,
                fillOpacity: stale ? 0.45 : 0.9,
                dashArray: stale ? "3 3" : null
            });

            const scoreText = Number.isFinite(score)
                ? score.toFixed(1) + "%"
                : "Unknown";

            const confidence = cell.confidence == null
                ? NaN
                : Number(cell.confidence);

            const confidenceText = Number.isFinite(confidence)
                ? (confidence * 100).toFixed(1) + "%"
                : "Unknown";

            const ageText = Number.isFinite(ageSeconds)
                ? Math.round(ageSeconds) + " seconds"
                : "Unknown";

            marker.bindPopup(
                "<b>Grid cell:</b> " + cell.cell_id +
                "<br><b>Last predicted risk:</b> " + risk +
                "<br><b>Risk score:</b> " + scoreText +
                "<br><b>Confidence:</b> " + confidenceText +
                "<br><b>Reading age:</b> " + ageText +
                "<br><b>Data freshness:</b> " +
                (stale
                    ? "STALE — update unavailable"
                    : "FRESH") +
                "<br><small>Simulated sensor data — " +
                "not for real safety decisions</small>"
            );

            marker.addTo(liveGridLayer);
            bounds.push([lat, lon]);
        });

        if (bounds.length > 0 && !map._liveGridPositioned) {
            map.fitBounds(bounds, {
                padding: [30, 30],
                maxZoom: 16
            });

            map._liveGridPositioned = true;
        }
    } catch (error) {
        console.error("Live grid error:", error);
    }
}

chart = new Chart($("chart"), {
    type: "line",
    data: {
        labels: [],
        datasets: [{
            label: "Sensor reading",
            data: [],
            borderColor: "#5275e8",
            backgroundColor: "rgba(82,117,232,.1)",
            fill: true,
            tension: 0.3,
            pointRadius: 2
        }]
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: false
            }
        },
        scales: {
            x: {
                grid: {
                    display: false
                },
                ticks: {
                    maxTicksLimit: 8
                }
            },
            y: {
                beginAtZero: true
            }
        }
    }
});

$("simulate").addEventListener("click", () => refresh(true));
$("sensor-choice").addEventListener("change", () => {
    history().catch(error => console.error("History error:", error));
});

refresh();
updateLiveGrid();

setInterval(() => refresh(false), 5000);
setInterval(updateLiveGrid, 5000);


async function loadPublicDataset() {
    const response = await fetch("/api/public-dataset/summary", { cache: "no-store" });
    if (!response.ok) throw new Error("Could not load public dataset summary");
    const data = await response.json();
    $("public-rows").textContent = Number(data.rows).toLocaleString();
    $("public-accuracy").textContent = data.accuracy == null
        ? "Not evaluated" : (Number(data.accuracy) * 100).toFixed(2) + "%";
    $("public-test-rows").textContent = data.test_rows
        ? Number(data.test_rows).toLocaleString() + " held-out samples" : "Run evaluation script for metrics";
    $("public-classes").innerHTML = Object.entries(data.class_counts).map(([label, count]) => `
        <div class="class-count"><span>${label}</span><strong>${Number(count).toLocaleString()}</strong></div>
    `).join("");
    $("public-sample-rows").innerHTML = data.sample.map(row => `
        <tr><td>${row.serial}</td><td>${row.gas}</td>
        ${["MQ2","MQ3","MQ5","MQ6","MQ7","MQ8","MQ135"].map(key => `<td>${row.readings[key]}</td>`).join("")}</tr>
    `).join("");
}

$("public-predict-form").addEventListener("submit", async event => {
    event.preventDefault();
    const form = event.currentTarget;
    const submit = form.querySelector("button[type=submit]");
    const output = $("public-prediction");
    const payload = Object.fromEntries(new FormData(form).entries());
    for (const key of Object.keys(payload)) payload[key] = Number(payload[key]);
    submit.disabled = true;
    output.textContent = "Classifying sample…";
    try {
        const response = await fetch("/api/public-dataset/predict", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(payload)
        });
        const result = await response.json();
        if (!response.ok) throw new Error(result.error || "Prediction failed");
        const probabilities = Object.entries(result.probabilities || {})
            .sort((a, b) => b[1] - a[1])
            .map(([label, percent]) => `${label}: ${percent.toFixed(2)}%`)
            .join(" · ");
        output.innerHTML = `<strong>Predicted dataset class: ${result.prediction}</strong>
            <div>${probabilities || "Class probabilities unavailable."}</div>
            <small>${result.notice}</small>`;
    } catch (error) {
        output.textContent = error.message;
    } finally {
        submit.disabled = false;
    }
});

loadPublicDataset().catch(error => {
    $("public-classes").textContent = "Dataset summary unavailable: " + error.message;
    console.error("Public dataset load error:", error);
});
