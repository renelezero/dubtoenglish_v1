(function () {
  "use strict";

  function esc(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function ago(iso) {
    const s = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
    if (s < 60) return s + "s ago";
    if (s < 3600) return Math.floor(s / 60) + "m ago";
    return Math.floor(s / 3600) + "h ago";
  }

  // ---- Elements ----
  const connIndicator = document.getElementById("connIndicator");
  const connText = document.getElementById("connText");
  const feedContent = document.getElementById("feedContent");
  const feedCount = document.getElementById("feedCount");
  const briefingContent = document.getElementById("briefingContent");
  const briefingMeta = document.getElementById("briefingMeta");
  const btnVoice = document.getElementById("btnVoice");
  const statTotal = document.getElementById("statTotal");
  const statSources = document.getElementById("statSources");
  const statTopLocation = document.getElementById("statTopLocation");
  const statCritical = document.getElementById("statCritical");

  // ---- Map ----
  const map = L.map("map", {
    center: [28, 42],
    zoom: 4,
    zoomControl: false,
    attributionControl: false,
  });

  L.control.zoom({ position: "bottomright" }).addTo(map);

  L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
    maxZoom: 18,
  }).addTo(map);

  const SEVERITY_COLORS = {
    critical: "#ef4444",
    high: "#f59e0b",
    medium: "#3b82f6",
    low: "#6b7280",
  };

  const markers = L.layerGroup().addTo(map);

  function addMapPin(ev) {
    const locs = ev.locations || [];
    for (const loc of locs) {
      if (loc.lat == null || loc.lng == null) continue;
      const color = SEVERITY_COLORS[ev.severity] || SEVERITY_COLORS.low;
      const marker = L.circleMarker([loc.lat, loc.lng], {
        radius: ev.severity === "critical" ? 8 : ev.severity === "high" ? 6 : 4,
        fillColor: color,
        color: color,
        weight: 1,
        opacity: 0.9,
        fillOpacity: 0.6,
      });
      marker.bindPopup(
        '<div class="map-popup">' +
          '<div class="popup-severity ' + ev.severity + '">' + ev.severity.toUpperCase() + "</div>" +
          "<strong>" + esc(ev.headline_en || "") + "</strong>" +
          "<p>" + esc(ev.summary_en || "") + "</p>" +
          '<div class="popup-source">' + esc(ev.source_name || "") + " &middot; " + esc(loc.name || "") + "</div>" +
        "</div>"
      );
      markers.addLayer(marker);
    }
  }

  // ---- Feed ----
  const MAX_FEED = 80;
  let feedItems = 0;

  function clearEmpty(el) {
    const e = el.querySelector(".empty-state");
    if (e) e.remove();
  }

  function addFeedItem(ev) {
    clearEmpty(feedContent);
    const el = document.createElement("div");
    el.className = "feed-item sev-" + (ev.severity || "low");
    el.innerHTML =
      '<div class="feed-top">' +
        '<span class="feed-source">' + esc(ev.source_name || "") + "</span>" +
        '<span class="feed-time">' + ago(ev.timestamp) + "</span>" +
      "</div>" +
      '<div class="feed-headline">' + esc(ev.headline_en || "") + "</div>" +
      (ev.summary_en ? '<div class="feed-summary">' + esc(ev.summary_en) + "</div>" : "");

    feedContent.prepend(el);
    feedItems++;
    feedCount.textContent = feedItems;

    while (feedContent.children.length > MAX_FEED) {
      feedContent.removeChild(feedContent.lastChild);
    }
  }

  // ---- Briefing ----
  const MAX_BRIEFING = 50;

  function addBriefingEntry(entry) {
    clearEmpty(briefingContent);
    const el = document.createElement("div");
    el.className = "briefing-entry";

    const ts = new Date(entry.timestamp).toLocaleTimeString([], {
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    });

    el.innerHTML =
      '<div class="briefing-time">' + ts + "</div>" +
      '<div class="briefing-text">' + esc(entry.text) + "</div>";

    briefingContent.prepend(el);

    while (briefingContent.children.length > MAX_BRIEFING) {
      briefingContent.removeChild(briefingContent.lastChild);
    }
    briefingMeta.textContent = "Updated " + ts;
  }

  // ---- Audio ----
  let voiceEnabled = false;
  let currentAudio = null;
  let audioQueue = [];

  btnVoice.addEventListener("click", () => {
    voiceEnabled = !voiceEnabled;
    btnVoice.classList.toggle("active", voiceEnabled);
    btnVoice.innerHTML = voiceEnabled
      ? '<span class="voice-icon">&#9834;</span> On'
      : '<span class="voice-icon">&#9834;</span> Voice';
    if (!voiceEnabled) flushAudio();
  });

  function enqueueAudio(b64) {
    if (!voiceEnabled) return;
    audioQueue.push(b64);
    while (audioQueue.length > 2) audioQueue.shift();
    if (!currentAudio) playNext();
  }

  function playNext() {
    if (!audioQueue.length) { currentAudio = null; return; }
    const b64 = audioQueue.shift();
    const a = new Audio("data:audio/mp3;base64," + b64);
    currentAudio = a;
    a.addEventListener("ended", playNext);
    a.addEventListener("error", playNext);
    a.play().catch(playNext);
  }

  function flushAudio() {
    audioQueue = [];
    if (currentAudio) {
      currentAudio.pause();
      currentAudio.removeEventListener("ended", playNext);
      currentAudio.removeEventListener("error", playNext);
      currentAudio = null;
    }
  }

  // ---- Stats ----
  function updateStats(stats) {
    statTotal.textContent = stats.total || 0;
    statSources.textContent = Object.keys(stats.sources || {}).length;
    const locs = stats.locations || {};
    const topLoc = Object.keys(locs).sort((a, b) => locs[b] - locs[a])[0];
    statTopLocation.textContent = topLoc || "—";
    statCritical.textContent = (stats.severities || {}).critical || 0;
  }

  // ---- Timeline chart ----
  const ctx = document.getElementById("timelineChart").getContext("2d");
  const bins = new Array(12).fill(0);
  const chart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: bins.map(() => ""),
      datasets: [{
        data: bins,
        backgroundColor: "rgba(59,130,246,0.5)",
        borderColor: "rgba(59,130,246,0.8)",
        borderWidth: 1,
        borderRadius: 2,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
      scales: {
        x: { display: false },
        y: { display: false, beginAtZero: true },
      },
      animation: { duration: 300 },
    },
  });

  function pushTimelineBin() {
    bins.shift();
    bins.push(0);
    chart.data.datasets[0].data = bins;
    chart.update("none");
  }

  setInterval(pushTimelineBin, 5000);

  function bumpTimeline() {
    bins[bins.length - 1]++;
    chart.data.datasets[0].data = bins;
    chart.update("none");
  }

  // ---- WebSocket ----
  function connect() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(proto + "//" + location.host + "/ws/feed");

    ws.addEventListener("open", () => {
      connIndicator.className = "status-indicator live";
      connText.textContent = "Live";
    });

    ws.addEventListener("message", (e) => {
      let msg;
      try { msg = JSON.parse(e.data); } catch { return; }

      switch (msg.type) {
        case "event":
          if (msg.event) {
            addFeedItem(msg.event);
            addMapPin(msg.event);
            bumpTimeline();
          }
          break;

        case "briefing":
          if (msg.entry) addBriefingEntry(msg.entry);
          if (msg.audio) enqueueAudio(msg.audio);
          break;

        case "stats":
          if (msg.stats) updateStats(msg.stats);
          break;
      }
    });

    ws.addEventListener("close", () => {
      connIndicator.className = "status-indicator offline";
      connText.textContent = "Reconnecting...";
      setTimeout(connect, 3000);
    });

    ws.addEventListener("error", () => {
      connIndicator.className = "status-indicator error";
      connText.textContent = "Error";
    });
  }

  // ---- Load existing events on page load ----
  async function loadHistory() {
    try {
      const res = await fetch("/api/events?hours=1");
      const data = await res.json();
      if (data.events) {
        for (const ev of data.events) {
          addFeedItem(ev);
          addMapPin(ev);
        }
      }
    } catch (err) {
      console.error("Failed to load history:", err);
    }
  }

  loadHistory();
  connect();
})();
