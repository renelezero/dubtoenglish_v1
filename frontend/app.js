(function () {
  "use strict";

  // ---- Helpers ----

  function extractVideoId(url) {
    try {
      const u = new URL(url);
      if (u.hostname.includes("youtu.be")) return u.pathname.slice(1);
      return u.searchParams.get("v") || "";
    } catch {
      return "";
    }
  }

  function escapeHtml(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
  }

  function timestamp() {
    return new Date().toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  // ---- Stream instance ----

  class StreamInstance {
    constructor(panelEl) {
      this.panel = panelEl;
      this.id = panelEl.dataset.streamId;
      this.hardcodedUrl = panelEl.dataset.url || "";

      this.iframe = panelEl.querySelector("iframe");
      this.feed = panelEl.querySelector(".subtitle-feed");
      this.statusIndicator = panelEl.querySelector(".status-indicator");
      this.statusText = panelEl.querySelector(".status-text");
      this.latencyDisplay = panelEl.querySelector(".latency-display");
      this.btnClear = panelEl.querySelector(".btn-clear");
      this.customUrlInput = panelEl.querySelector(".custom-url");

      this.ws = null;
      this.audioQueue = [];
      this.isPlayingAudio = false;
      this.pendingEntry = null;
      this.chunkStartTime = null;

      this.btnClear.addEventListener("click", () => this.clearFeed());
    }

    getUrl() {
      if (this.customUrlInput) {
        return this.customUrlInput.value.trim() || this.hardcodedUrl;
      }
      return this.hardcodedUrl;
    }

    setStatus(state, text) {
      this.statusIndicator.className = "status-indicator " + state;
      this.statusText.textContent = text;
    }

    clearFeed() {
      this.feed.innerHTML =
        '<div class="empty-state">Waiting to start\u2026</div>';
      this.pendingEntry = null;
    }

    removeEmptyState() {
      const empty = this.feed.querySelector(".empty-state");
      if (empty) empty.remove();
    }

    addArabicEntry(arabicText) {
      this.removeEmptyState();
      const entry = document.createElement("div");
      entry.className = "sub-entry";
      entry.innerHTML =
        `<div class="arabic">${escapeHtml(arabicText)}</div>` +
        `<div class="english waiting">\u2026</div>` +
        `<div class="timestamp">${timestamp()}</div>`;
      this.feed.appendChild(entry);
      this.feed.scrollTop = this.feed.scrollHeight;
      this.pendingEntry = entry;
    }

    fillTranslation(englishText) {
      if (this.pendingEntry) {
        const el = this.pendingEntry.querySelector(".english");
        el.textContent = englishText;
        el.classList.remove("waiting");
        this.pendingEntry = null;
      } else {
        this.removeEmptyState();
        const entry = document.createElement("div");
        entry.className = "sub-entry";
        entry.innerHTML =
          `<div class="english">${escapeHtml(englishText)}</div>` +
          `<div class="timestamp">${timestamp()}</div>`;
        this.feed.appendChild(entry);
      }
      this.feed.scrollTop = this.feed.scrollHeight;
    }

    // ---- Audio ----

    enqueueAudio(base64Mp3) {
      this.audioQueue.push(base64Mp3);
      if (!this.isPlayingAudio) this.playNext();
    }

    playNext() {
      if (this.audioQueue.length === 0) {
        this.isPlayingAudio = false;
        return;
      }
      this.isPlayingAudio = true;
      const b64 = this.audioQueue.shift();
      const audio = new Audio("data:audio/mp3;base64," + b64);
      audio.addEventListener("ended", () => this.playNext());
      audio.addEventListener("error", () => this.playNext());
      audio.play().catch(() => this.playNext());
    }

    flushAudio() {
      this.audioQueue = [];
      this.isPlayingAudio = false;
    }

    // ---- Embed ----

    showEmbed(videoId) {
      this.iframe.src = `https://www.youtube.com/embed/${videoId}?autoplay=1&mute=1`;
    }

    hideEmbed() {
      this.iframe.src = "";
    }

    // ---- WebSocket ----

    start(enableTts) {
      const url = this.getUrl();
      if (!url) return;

      const videoId = extractVideoId(url);
      if (videoId) this.showEmbed(videoId);

      this.clearFeed();
      this.removeEmptyState();
      this.setStatus("connecting", "Connecting\u2026");

      const proto = location.protocol === "https:" ? "wss:" : "ws:";
      const wsUrl = `${proto}//${location.host}/ws/dub`;
      this.ws = new WebSocket(wsUrl);

      this.ws.addEventListener("open", () => {
        this.setStatus("connecting", "Connected \u2014 initializing\u2026");
        this.ws.send(JSON.stringify({ url, tts: enableTts }));
        this.chunkStartTime = Date.now();
      });

      this.ws.addEventListener("message", (event) => {
        let msg;
        try {
          msg = JSON.parse(event.data);
        } catch {
          return;
        }

        switch (msg.type) {
          case "status":
            this.setStatus("connecting", msg.message);
            if (msg.message.toLowerCase().includes("listening")) {
              this.setStatus("live", "Live");
            }
            break;

          case "arabic":
            this.chunkStartTime = Date.now();
            this.addArabicEntry(msg.text);
            break;

          case "translation":
            this.fillTranslation(msg.text);
            if (msg.audio && document.getElementById("globalAudio").checked) {
              this.enqueueAudio(msg.audio);
            }
            if (this.chunkStartTime) {
              const latency = (
                (Date.now() - this.chunkStartTime) /
                1000
              ).toFixed(1);
              this.latencyDisplay.textContent = `~${latency}s`;
              this.chunkStartTime = null;
            }
            break;

          case "error":
            this.setStatus("error", msg.message);
            break;
        }
      });

      this.ws.addEventListener("close", () => {
        this.setStatus("offline", "Disconnected");
      });

      this.ws.addEventListener("error", () => {
        this.setStatus("error", "Connection error");
      });
    }

    stop() {
      if (this.ws) {
        this.ws.close();
        this.ws = null;
      }
      this.flushAudio();
      this.hideEmbed();
      this.setStatus("offline", "Stopped");
    }
  }

  // ---- Init all streams ----

  const panels = document.querySelectorAll(".stream-panel");
  const streams = Array.from(panels).map((p) => new StreamInstance(p));

  const btnStartAll = document.getElementById("btnStartAll");
  const btnStopAll = document.getElementById("btnStopAll");
  const globalAudio = document.getElementById("globalAudio");

  btnStartAll.addEventListener("click", () => {
    const enableTts = globalAudio.checked;
    streams.forEach((s) => {
      if (s.getUrl()) s.start(enableTts);
    });
    btnStartAll.disabled = true;
    btnStopAll.disabled = false;
  });

  btnStopAll.addEventListener("click", () => {
    streams.forEach((s) => s.stop());
    btnStartAll.disabled = false;
    btnStopAll.disabled = true;
  });

  // Allow Enter key on custom URL input to trigger start
  const customInput = document.querySelector(".custom-url");
  if (customInput) {
    customInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !btnStartAll.disabled) {
        btnStartAll.click();
      }
    });
  }
})();
