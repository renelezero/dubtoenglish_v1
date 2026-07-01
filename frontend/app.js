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
  const sevFilters = document.getElementById("sevFilters");
  const srcFilters = document.getElementById("srcFilters");

  // ---- Filter state ----
  let filterSev = "all";
  let filterSrc = "all";
  const knownSources = new Set();

  function matchesFilter(ev) {
    if (filterSev !== "all" && ev.severity !== filterSev) return false;
    if (filterSrc !== "all" && ev.source_name !== filterSrc) return false;
    return true;
  }

  sevFilters.addEventListener("click", (e) => {
    const btn = e.target.closest(".filter-pill");
    if (!btn) return;
    filterSev = btn.dataset.sev;
    sevFilters.querySelectorAll(".filter-pill").forEach((b) => b.classList.toggle("active", b.dataset.sev === filterSev));
    applyFilters();
  });

  srcFilters.addEventListener("click", (e) => {
    const btn = e.target.closest(".filter-pill");
    if (!btn) return;
    filterSrc = btn.dataset.src;
    srcFilters.querySelectorAll(".filter-pill").forEach((b) => b.classList.toggle("active", b.dataset.src === filterSrc));
    applyFilters();
  });

  function addSourcePill(name) {
    if (knownSources.has(name)) return;
    knownSources.add(name);
    const btn = document.createElement("button");
    btn.className = "filter-pill";
    btn.dataset.src = name;
    btn.textContent = name;
    srcFilters.appendChild(btn);
  }

  function applyFilters() {
    // Feed items
    feedContent.querySelectorAll(".feed-item").forEach((el) => {
      const sev = el.dataset.severity;
      const src = el.dataset.source;
      const show = (filterSev === "all" || sev === filterSev) && (filterSrc === "all" || src === filterSrc);
      el.style.display = show ? "" : "none";
    });
    // Map pins
    for (const pin of pinList) {
      const show = (filterSev === "all" || pin.severity === filterSev) && (filterSrc === "all" || pin.source === filterSrc);
      if (show && !markers.hasLayer(pin.marker)) markers.addLayer(pin.marker);
      if (!show && markers.hasLayer(pin.marker)) markers.removeLayer(pin.marker);
    }
  }

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
  const pinList = [];

  const BASE_RADIUS = { critical: 14, high: 11, medium: 9, low: 7 };
  const MIN_RADIUS = 3;
  const DECAY_MS = 10 * 60 * 1000;

  function addMapPin(ev) {
    const locs = ev.locations || [];
    const born = ev.timestamp ? new Date(ev.timestamp).getTime() : Date.now();
    for (const loc of locs) {
      if (loc.lat == null || loc.lng == null) continue;
      const color = SEVERITY_COLORS[ev.severity] || SEVERITY_COLORS.low;
      const startR = BASE_RADIUS[ev.severity] || BASE_RADIUS.low;
      const marker = L.circleMarker([loc.lat, loc.lng], {
        radius: startR,
        fillColor: color,
        color: color,
        weight: 1.5,
        opacity: 0.95,
        fillOpacity: 0.7,
      });
      marker.bindPopup(
        '<div class="map-popup">' +
          '<div class="popup-severity ' + ev.severity + '">' + (ev.severity || "").toUpperCase() + "</div>" +
          "<strong>" + esc(ev.headline_en || "") + "</strong>" +
          "<p>" + esc(ev.summary_en || "") + "</p>" +
          '<div class="popup-source">' + esc(ev.source_name || "") + " &middot; " + esc(loc.name || "") + "</div>" +
        "</div>"
      );
      markers.addLayer(marker);
      pinList.push({ marker, born, startR, severity: ev.severity || "low", source: ev.source_name || "" });
    }
  }

  setInterval(() => {
    const now = Date.now();
    for (const pin of pinList) {
      const age = now - pin.born;
      const t = Math.min(age / DECAY_MS, 1);
      const r = pin.startR - (pin.startR - MIN_RADIUS) * t;
      const opacity = 0.95 - 0.55 * t;
      pin.marker.setRadius(r);
      pin.marker.setStyle({ fillOpacity: opacity * 0.7, opacity: opacity });
    }
  }, 5000);

  // ---- Feed ----
  const MAX_FEED = 200;
  let feedItems = 0;

  function clearEmpty(el) {
    const e = el.querySelector(".empty-state");
    if (e) e.remove();
  }

  function addFeedItem(ev, prepend) {
    clearEmpty(feedContent);
    addSourcePill(ev.source_name || "Unknown");

    const el = document.createElement("div");
    el.className = "feed-item sev-" + (ev.severity || "low");
    el.dataset.severity = ev.severity || "low";
    el.dataset.source = ev.source_name || "";

    const origin = ev.origin === "live" ? '<span class="feed-live">LIVE</span> ' : "";

    el.innerHTML =
      '<div class="feed-top">' +
        '<span class="feed-source">' + origin + esc(ev.source_name || "") + "</span>" +
        '<span class="feed-time">' + ago(ev.timestamp) + "</span>" +
      "</div>" +
      '<div class="feed-headline">' + esc(ev.headline_en || "") + "</div>" +
      (ev.summary_en ? '<div class="feed-summary">' + esc(ev.summary_en) + "</div>" : "");

    if (!matchesFilter(ev)) el.style.display = "none";

    if (prepend !== false) {
      feedContent.prepend(el);
    } else {
      feedContent.appendChild(el);
    }
    feedItems++;
    feedCount.textContent = feedItems;

    while (feedContent.children.length > MAX_FEED) {
      feedContent.removeChild(feedContent.lastChild);
    }
  }

  // ---- Briefing ----
  const MAX_BRIEFING = 50;

  function addBriefingEntry(entry, prepend) {
    clearEmpty(briefingContent);
    const el = document.createElement("div");
    el.className = "briefing-entry";

    const ts = new Date(entry.timestamp).toLocaleTimeString([], {
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    });

    el.innerHTML =
      '<div class="briefing-time">' + ts + "</div>" +
      '<div class="briefing-text">' + esc(entry.text) + "</div>";

    if (prepend !== false) {
      briefingContent.prepend(el);
    } else {
      briefingContent.appendChild(el);
    }

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
    statTopLocation.textContent = topLoc || "\u2014";
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

  // ---- Actor Network (vis-network) ----
  const DIM_COLORS = {
    political: "#3b82f6",
    military: "#ef4444",
    economic: "#10b981",
    religious: "#a855f7",
  };
  const TYPE_FALLBACK = "#94a3b8";
  const REL_COLORS = {
    hostile: "#ef4444",
    cooperative: "#22c55e",
    neutral: "#64748b",
  };

  let networkInstance = null;
  let networkLoaded = false;
  const networkEl = document.getElementById("networkGraph");
  const networkEmpty = document.getElementById("networkEmpty");
  const netDimension = document.getElementById("netDimension");

  function nodeColor(node) {
    const dims = node.dimensions || [];
    if (dims.length) return DIM_COLORS[dims[0]] || TYPE_FALLBACK;
    return TYPE_FALLBACK;
  }

  function buildNetwork(data) {
    const dimFilter = netDimension.value;
    let nodes = data.nodes || [];
    if (dimFilter) {
      nodes = nodes.filter((n) => (n.dimensions || []).includes(dimFilter));
    }
    const keep = new Set(nodes.map((n) => n.id));
    const edges = (data.edges || []).filter(
      (e) => keep.has(e.source) && keep.has(e.target)
    );

    if (!nodes.length) {
      networkEmpty.style.display = "";
      networkEl.style.display = "none";
      if (networkInstance) networkInstance.setData({ nodes: [], edges: [] });
      return;
    }
    networkEmpty.style.display = "none";
    networkEl.style.display = "";

    const maxInf = Math.max(...nodes.map((n) => n.influence || 0), 1);
    const visNodes = nodes.map((n) => {
      const size = 10 + 34 * ((n.influence || 0) / maxInf);
      const dims = (n.dimensions || []).join(", ") || "—";
      return {
        id: n.id,
        label: n.label,
        value: n.influence || 1,
        size: size,
        color: { background: nodeColor(n), border: "#0b1220" },
        font: { color: "#e5e7eb", size: 13, face: "Inter" },
        title:
          n.label +
          " (" + n.type + ")\nDimensions: " + dims +
          "\nInfluence: " + (n.influence || 0) +
          " | Mentions: " + n.mentions +
          " | Connections: " + n.degree,
      };
    });

    const visEdges = edges.map((e) => ({
      from: e.source,
      to: e.target,
      arrows: "to",
      width: Math.min(1 + e.weight, 8),
      color: { color: REL_COLORS[e.class] || REL_COLORS.neutral, opacity: 0.7 },
      label: e.type,
      font: { color: "#94a3b8", size: 10, strokeWidth: 0, align: "middle" },
      title: e.snippet || e.type,
      smooth: { type: "continuous" },
    }));

    const payload = { nodes: visNodes, edges: visEdges };

    if (!networkInstance) {
      networkInstance = new vis.Network(networkEl, payload, {
        physics: {
          stabilization: { iterations: 150 },
          barnesHut: { gravitationalConstant: -8000, springLength: 140 },
        },
        interaction: { hover: true, tooltipDelay: 120 },
        nodes: { shape: "dot", borderWidth: 2 },
        edges: { selectionWidth: 2 },
      });
    } else {
      networkInstance.setData(payload);
    }
  }

  async function loadNetwork() {
    try {
      const res = await fetch("/api/network?hours=336&limit=120");
      const data = await res.json();
      networkLoaded = true;
      buildNetwork(data);
    } catch (err) {
      console.error("Network load failed:", err);
    }
  }

  netDimension.addEventListener("change", loadNetwork);
  document.getElementById("netRefresh").addEventListener("click", loadNetwork);

  // ---- View tabs (Map / Network) ----
  const viewTabs = document.querySelectorAll(".view-tab");
  const netControls = document.getElementById("netControls");
  viewTabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      const view = tab.dataset.view;
      viewTabs.forEach((t) => t.classList.toggle("active", t === tab));
      document.querySelectorAll(".view-pane").forEach((p) => {
        p.classList.toggle("active", p.id === view);
      });
      netControls.style.display = view === "network" ? "" : "none";
      if (view === "map" && map) {
        setTimeout(() => map.invalidateSize(), 50);
      }
      if (view === "network") {
        loadNetwork();
      }
    });
  });

  // ---- Ask the Analyst ----
  const analystForm = document.getElementById("analystForm");
  const analystInput = document.getElementById("analystInput");
  const analystSend = document.getElementById("analystSend");

  function addAnalystEntry(question, answer, pending) {
    clearEmpty(briefingContent);
    const el = document.createElement("div");
    el.className = "briefing-entry analyst-entry";
    el.innerHTML =
      '<div class="analyst-q">Q: ' + esc(question) + "</div>" +
      '<div class="analyst-a">' +
        (pending ? '<span class="analyst-thinking">Analyzing…</span>' : esc(answer)) +
      "</div>";
    briefingContent.prepend(el);
    return el;
  }

  analystForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const q = analystInput.value.trim();
    if (!q) return;
    analystInput.value = "";
    analystSend.disabled = true;
    const el = addAnalystEntry(q, "", true);
    try {
      const res = await fetch("/api/analyst", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: q, hours: 336 }),
      });
      const data = await res.json();
      el.querySelector(".analyst-a").textContent = data.answer || "(no answer)";
    } catch (err) {
      el.querySelector(".analyst-a").textContent = "Request failed.";
    } finally {
      analystSend.disabled = false;
    }
  });

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
        case "history":
          if (msg.events && !historyLoaded) {
            loadEvents(msg.events);
          }
          break;

        case "briefing_history":
          if (msg.entries) {
            briefingContent.innerHTML = "";
            for (const entry of msg.entries) {
              addBriefingEntry(entry, false);
            }
          }
          break;

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

        case "graph_update":
          if (document.getElementById("network").classList.contains("active")) {
            loadNetwork();
          }
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

  let historyLoaded = false;

  function loadEvents(events) {
    feedContent.innerHTML = "";
    feedItems = 0;
    markers.clearLayers();
    pinList.length = 0;
    const sorted = events.slice().sort((a, b) =>
      new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
    );
    for (const ev of sorted) {
      addFeedItem(ev, false);
      addMapPin(ev);
    }
    historyLoaded = true;
  }

  async function loadHistory() {
    try {
      const res = await fetch("/api/events?hours=6");
      const data = await res.json();
      if (data.events && data.events.length > 0) {
        loadEvents(data.events);
      }
    } catch (err) {
      console.error("History load failed:", err);
    }
    try {
      const res = await fetch("/api/summaries?hours=6");
      const data = await res.json();
      if (data.entries && data.entries.length > 0) {
        briefingContent.innerHTML = "";
        const sorted = data.entries.slice().sort((a, b) =>
          new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
        );
        for (const entry of sorted) {
          addBriefingEntry(entry, false);
        }
      }
    } catch (err) {
      console.error("Briefing history load failed:", err);
    }
  }

  loadHistory();
  connect();
})();
