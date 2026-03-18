(function () {
  "use strict";

  function escapeHtml(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
  }

  function timeStr(isoOrNow) {
    const d = isoOrNow ? new Date(isoOrNow) : new Date();
    return d.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function typeLabel(type) {
    if (type.startsWith("lookback")) return "LOOKBACK";
    return "UPDATE";
  }

  function typeCls(type) {
    if (type.startsWith("lookback")) return "tag-lookback";
    return "tag-update";
  }

  // ---- Elements ----

  const connIndicator = document.getElementById("connIndicator");
  const connText = document.getElementById("connText");

  const liveContent = document.getElementById("liveContent");
  const liveMeta = document.getElementById("liveMeta");

  const lookbackContent = document.getElementById("lookbackContent");
  const lookbackMeta = document.getElementById("lookbackMeta");
  const btnLookback = document.getElementById("btnLookback");
  const tabs = document.querySelectorAll(".window-tabs .tab");
  const listenButtons = document.querySelectorAll(".btn-listen");

  let activeWindow = "3h";
  let activeListenStream = null;

  // ---- Audio queue (single-voice anchor) ----

  const MAX_QUEUE = 3;
  let audioQueue = [];
  let isPlaying = false;
  let currentAudio = null;

  function enqueueAudio(b64) {
    audioQueue.push(b64);
    while (audioQueue.length > MAX_QUEUE) audioQueue.shift();
    if (!isPlaying) playNext();
  }

  function playNext() {
    if (audioQueue.length === 0) {
      isPlaying = false;
      currentAudio = null;
      return;
    }
    isPlaying = true;
    const b64 = audioQueue.shift();
    const audio = new Audio("data:audio/mp3;base64," + b64);
    currentAudio = audio;
    audio.addEventListener("ended", playNext);
    audio.addEventListener("error", playNext);
    audio.play().catch(playNext);
  }

  function flushAudio() {
    audioQueue = [];
    if (currentAudio) {
      currentAudio.pause();
      currentAudio.removeEventListener("ended", playNext);
      currentAudio.removeEventListener("error", playNext);
      currentAudio.currentTime = 0;
      currentAudio = null;
    }
    isPlaying = false;
  }

  // ---- Listen toggle ----

  function setListenStream(streamId) {
    if (activeListenStream === streamId) {
      activeListenStream = null;
    } else {
      activeListenStream = streamId;
    }
    flushAudio();
    updateListenUI();
  }

  function updateListenUI() {
    listenButtons.forEach((btn) => {
      const sid = btn.dataset.streamId;
      const cell = btn.closest(".video-cell");
      const panel = btn.closest(".summary-panel");
      if (sid === activeListenStream) {
        btn.classList.add("active");
        btn.innerHTML = '<span class="listen-icon">&#9834;</span> Listening';
        if (cell) cell.classList.add("listening");
        if (panel) panel.classList.add("listening");
      } else {
        btn.classList.remove("active");
        btn.innerHTML = '<span class="listen-icon">&#9834;</span> Listen';
        if (cell) cell.classList.remove("listening");
        if (panel) panel.classList.remove("listening");
      }
    });
  }

  listenButtons.forEach((btn) => {
    btn.addEventListener("click", () => setListenStream(btn.dataset.streamId));
  });

  // ---- Stream status helpers ----

  function setStreamStatus(streamId, state, text) {
    const cell = document.querySelector(`.video-cell[data-stream-id="${streamId}"]`);
    if (!cell) return;
    const indicator = cell.querySelector(".status-indicator");
    const label = cell.querySelector(".status-text");
    indicator.className = "status-indicator " + state;
    label.textContent = text;
  }

  // ---- Per-stream feed management ----

  const MAX_FEED_ENTRIES = 50;
  const pendingEntries = {};

  function getFeedEl(streamId) {
    return document.querySelector(`.cell-feed[data-feed-for="${streamId}"]`);
  }

  function clearEmptyState(el) {
    const empty = el.querySelector(".empty-state");
    if (empty) empty.remove();
  }

  function addArabicToFeed(streamId, arabicText) {
    const feedEl = getFeedEl(streamId);
    if (!feedEl) return;
    clearEmptyState(feedEl);

    const entry = document.createElement("div");
    entry.className = "feed-entry";
    entry.innerHTML =
      `<div class="feed-arabic">${escapeHtml(arabicText)}</div>` +
      `<div class="feed-english waiting">...</div>` +
      `<div class="feed-time">${timeStr()}</div>`;
    feedEl.appendChild(entry);
    feedEl.scrollTop = feedEl.scrollHeight;
    pendingEntries[streamId] = entry;
  }

  function fillEnglishInFeed(streamId, englishText) {
    const feedEl = getFeedEl(streamId);
    if (!feedEl) return;

    const entry = pendingEntries[streamId];
    if (entry) {
      const el = entry.querySelector(".feed-english");
      el.textContent = englishText;
      el.classList.remove("waiting");
      delete pendingEntries[streamId];
    } else {
      clearEmptyState(feedEl);
      const newEntry = document.createElement("div");
      newEntry.className = "feed-entry";
      newEntry.innerHTML =
        `<div class="feed-english">${escapeHtml(englishText)}</div>` +
        `<div class="feed-time">${timeStr()}</div>`;
      feedEl.appendChild(newEntry);
    }
    feedEl.scrollTop = feedEl.scrollHeight;

    while (feedEl.children.length > MAX_FEED_ENTRIES) {
      feedEl.removeChild(feedEl.firstChild);
    }
  }

  // ---- Markdown-ish rendering ----

  function renderSummaryText(text) {
    const lines = text.split("\n");
    let html = "";
    let inList = false;

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) {
        if (inList) { html += "</ul>"; inList = false; }
        continue;
      }
      if (trimmed.startsWith("- ") || trimmed.startsWith("* ")) {
        if (!inList) { html += "<ul>"; inList = true; }
        html += `<li>${escapeHtml(trimmed.slice(2))}</li>`;
      } else if (trimmed.startsWith("## ")) {
        if (inList) { html += "</ul>"; inList = false; }
        html += `<h4>${escapeHtml(trimmed.slice(3))}</h4>`;
      } else if (trimmed.startsWith("# ")) {
        if (inList) { html += "</ul>"; inList = false; }
        html += `<h3>${escapeHtml(trimmed.slice(2))}</h3>`;
      } else if (trimmed.startsWith("**") && trimmed.endsWith("**")) {
        if (inList) { html += "</ul>"; inList = false; }
        html += `<p class="highlight"><strong>${escapeHtml(trimmed.slice(2, -2))}</strong></p>`;
      } else {
        if (inList) { html += "</ul>"; inList = false; }
        html += `<p>${escapeHtml(trimmed)}</p>`;
      }
    }
    if (inList) html += "</ul>";
    return html;
  }

  // ---- Live feed log (left panel) ----

  const MAX_LOG_ENTRIES = 100;

  function appendSummaryEntry(entry) {
    clearEmptyState(liveContent);

    const el = document.createElement("div");
    el.className = "summary-log-entry";

    const tag = typeLabel(entry.type);
    const cls = typeCls(entry.type);
    const ts = timeStr(entry.timestamp);

    el.innerHTML =
      `<div class="log-header"><span class="log-tag ${cls}">${tag}</span><span class="log-time">${ts}</span></div>` +
      `<div class="log-body">${renderSummaryText(entry.text)}</div>`;

    liveContent.appendChild(el);
    liveContent.scrollTop = liveContent.scrollHeight;

    while (liveContent.children.length > MAX_LOG_ENTRIES) {
      liveContent.removeChild(liveContent.firstChild);
    }
  }

  async function loadSummaryHistory() {
    try {
      const res = await fetch("/api/summaries?hours=6");
      const data = await res.json();
      if (data.entries && data.entries.length > 0) {
        liveContent.innerHTML = "";
        for (const entry of data.entries) {
          appendSummaryEntry(entry);
        }
        liveMeta.textContent = `${data.entries.length} log entries | ${timeStr()}`;
      }
    } catch (err) {
      console.error("Failed to load summary history:", err);
    }
  }

  // ---- Lookback panel (right panel) ----

  const lookbackCache = {};

  function renderLookback() {
    const data = lookbackCache[activeWindow];
    if (!data) {
      lookbackContent.innerHTML =
        '<div class="empty-state">No lookback data yet for ' + activeWindow + "...</div>";
      lookbackMeta.textContent = "Auto-refreshes";
      return;
    }
    lookbackContent.innerHTML = renderSummaryText(data.text);
    lookbackMeta.textContent =
      activeWindow + " lookback | " + data.transcript_count + " transcripts | " + data.time;
  }

  // ---- WebSocket ----

  function connect() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${proto}//${location.host}/ws/feed`);

    ws.addEventListener("open", () => {
      connIndicator.className = "status-indicator live";
      connText.textContent = "Connected — streams processing on server";
    });

    ws.addEventListener("message", (event) => {
      let msg;
      try { msg = JSON.parse(event.data); } catch { return; }

      switch (msg.type) {
        case "status":
          if (msg.message === "Live") {
            setStreamStatus(msg.stream_id, "live", "Live");
          } else if (msg.message.includes("Error")) {
            setStreamStatus(msg.stream_id, "error", msg.message);
          } else {
            setStreamStatus(msg.stream_id, "connecting", msg.message);
          }
          break;

        case "arabic":
          addArabicToFeed(msg.stream_id, msg.text);
          break;

        case "translation":
          fillEnglishInFeed(msg.stream_id, msg.text);
          if (msg.audio && activeListenStream === msg.stream_id) {
            enqueueAudio(msg.audio);
          }
          break;

        case "translation_audio":
          if (msg.audio && activeListenStream === msg.stream_id) {
            enqueueAudio(msg.audio);
          }
          break;

        case "summary_entry":
          if (msg.entry) {
            appendSummaryEntry(msg.entry);
            liveMeta.textContent = "Last update " + timeStr(msg.entry.timestamp);
          }
          if (msg.audio && activeListenStream === "live-updates") {
            enqueueAudio(msg.audio);
          }
          break;

        case "lookback":
          lookbackCache[msg.window] = {
            text: msg.text,
            audio: msg.audio,
            transcript_count: msg.transcript_count,
            time: timeStr(),
          };
          if (msg.window === activeWindow) renderLookback();
          if (msg.audio && activeListenStream === "lookback") {
            flushAudio();
            enqueueAudio(msg.audio);
          }
          break;

        case "error":
          setStreamStatus(msg.stream_id, "error", msg.message);
          break;
      }
    });

    ws.addEventListener("close", () => {
      connIndicator.className = "status-indicator offline";
      connText.textContent = "Disconnected — reconnecting...";
      setTimeout(connect, 3000);
    });

    ws.addEventListener("error", () => {
      connIndicator.className = "status-indicator error";
      connText.textContent = "Connection error";
    });
  }

  // ---- Lookback controls ----

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      tabs.forEach((t) => t.classList.remove("active"));
      tab.classList.add("active");
      activeWindow = tab.dataset.window;
      renderLookback();
    });
  });

  btnLookback.addEventListener("click", async () => {
    lookbackContent.innerHTML =
      '<div class="summary-loading">Generating ' + activeWindow + " lookback...</div>";

    try {
      const res = await fetch("/api/summary?window=" + activeWindow);
      const data = await res.json();

      if (!data.summary) {
        lookbackContent.innerHTML =
          '<div class="empty-state">' + escapeHtml(data.message || "No data.") + "</div>";
      } else {
        lookbackCache[activeWindow] = {
          text: data.summary,
          audio: data.audio,
          transcript_count: data.transcript_count,
          time: timeStr(),
        };
        renderLookback();

        if (data.audio && activeListenStream === "lookback") {
          flushAudio();
          enqueueAudio(data.audio);
        }
      }
    } catch (err) {
      lookbackContent.innerHTML =
        '<div class="empty-state">Error: ' + escapeHtml(err.message) + "</div>";
    }
  });

  // ---- Init ----
  loadSummaryHistory();
  connect();
})();
