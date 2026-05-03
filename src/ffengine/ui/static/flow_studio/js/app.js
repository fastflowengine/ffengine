const STUDIO_BASE_CANDIDATES = (() => {
  const pathname = (window.location.pathname || "").toLowerCase();
  const candidates = [];
  if (pathname.startsWith("/plugin/flow-studio")) {
    candidates.push("/plugin/flow-studio");
  }
  if (pathname.startsWith("/flow-studio")) {
    candidates.push("/flow-studio");
  }
  candidates.push("/flow-studio", "/plugin/flow-studio");
  return Array.from(new Set(candidates));
})();
let studioBase = STUDIO_BASE_CANDIDATES[0] || "/flow-studio";

function studioUrl(path) {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${studioBase}${normalizedPath}`;
}

async function studioFetch(path, options) {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  const tried = new Set();
  const candidates = [studioBase, ...STUDIO_BASE_CANDIDATES];
  let lastResponse = null;
  const expectsJson = normalizedPath.startsWith("/api/");
  for (const candidate of candidates) {
    if (!candidate || tried.has(candidate)) continue;
    tried.add(candidate);
    const response = await fetch(`${candidate}${normalizedPath}`, options);
    const contentType = String(response.headers.get("content-type") || "").toLowerCase();
    const isJson = contentType.includes("application/json");
    const validApiPayload = !expectsJson || isJson || response.status >= 400;
    if (response.status !== 404 && validApiPayload) {
      studioBase = candidate;
      return response;
    }
    lastResponse = response;
  }
  return lastResponse || fetch(studioUrl(normalizedPath), options);
}
    const THEME_CACHE_KEY = "flow_studio_airflow_theme_css_v1";

    function setThemeSource(source) {
      document.documentElement.setAttribute("data-theme-source", source);
      const debug = el("theme_source_debug");
      if (debug) {
        debug.textContent = "";
      }
    }

    function syncThemeTokensFromDocument(doc) {
      try {
        const root = (doc && doc.documentElement) ? doc.documentElement : document.documentElement;
        const body = (doc && doc.body) ? doc.body : document.body;
        const rootStyle = window.getComputedStyle(root);
        const st = window.getComputedStyle(body);
        const bodyVars = window.getComputedStyle(body);
        const targetRoot = document.documentElement;

        let isExplicitlyDark = false;
        const themeAttr = (root.getAttribute("data-theme") || "").trim();
        const colorModeAttr = (root.getAttribute("data-color-mode") || "").trim();
        if (themeAttr) {
          targetRoot.setAttribute("data-theme", themeAttr);
          if (themeAttr === "dark") isExplicitlyDark = true;
        } else {
          targetRoot.removeAttribute("data-theme");
        }
        if (colorModeAttr) {
          targetRoot.setAttribute("data-color-mode", colorModeAttr);
          if (colorModeAttr === "dark") isExplicitlyDark = true;
        } else {
          targetRoot.removeAttribute("data-color-mode");
        }

        const hasDarkClass = root.classList.contains("chakra-ui-dark")
          || body.classList.contains("chakra-ui-dark")
          || root.classList.contains("dark")
          || body.classList.contains("dark");
        const hasLightClass = root.classList.contains("chakra-ui-light")
          || body.classList.contains("chakra-ui-light")
          || root.classList.contains("light")
          || body.classList.contains("light");

        targetRoot.classList.toggle("chakra-ui-dark", hasDarkClass);
        targetRoot.classList.toggle("chakra-ui-light", hasLightClass);
        if (hasDarkClass) {
          isExplicitlyDark = true;
        }

        if ((root.style && root.style.colorScheme === "dark") || st.colorScheme === "dark") {
          isExplicitlyDark = true;
        }

        const isVeryLightRgb = (value) => {
          const m = value && value.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
          if (!m) return false;
          const r = Number(m[1]);
          const g = Number(m[2]);
          const b = Number(m[3]);
          const luma = 0.2126 * r + 0.7152 * g + 0.0722 * b;
          return luma > 180;
        };

        const isDarkRgb = (value) => {
          const m = value && value.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
          if (!m) return false;
          const r = Number(m[1]);
          const g = Number(m[2]);
          const b = Number(m[3]);
          const luma = 0.2126 * r + 0.7152 * g + 0.0722 * b;
          return luma < 128;
        };

        const isTransparent = (value) => {
          if (!value) return true;
          const v = value.toLowerCase();
          return v === "transparent" || (v.includes("rgba(") && v.includes(", 0)"));
        };

        const font = st.fontFamily;
        const textColor = st.color;
        const backgroundColor = st.backgroundColor;

        const getToken = (...names) => {
          for (const name of names) {
            const v1 = (rootStyle.getPropertyValue(name) || "").trim();
            if (v1) return v1;
            const v2 = (bodyVars.getPropertyValue(name) || "").trim();
            if (v2) return v2;
          }
          return "";
        };

        if (font) {
          targetRoot.style.setProperty("--font-family-base", font);
        }
        const airflowBg = getToken("--chakra-colors-chakra-body-bg", "--chakra-colors-bg-panel", "--chakra-colors-bg-base", "--color-bg-main", "--bs-body-bg");
        const airflowCard = getToken("--chakra-colors-chakra-subtle-bg", "--chakra-colors-bg-surface", "--color-bg-1", "--bs-secondary-bg");
        const airflowLine = getToken("--chakra-colors-chakra-border-color", "--chakra-colors-border-default", "--chakra-colors-border", "--color-border", "--bs-border-color");
        const airflowText = getToken("--chakra-colors-chakra-body-text", "--chakra-colors-text-default", "--chakra-colors-text", "--color-text", "--bs-body-color");
        const airflowMuted = getToken("--chakra-colors-chakra-subtle-text", "--chakra-colors-text-muted", "--color-text-secondary", "--bs-secondary-color");

        const rootBg = window.getComputedStyle(root).backgroundColor;
        const potentialDark = [backgroundColor, rootBg, airflowBg].find((c) => c && !isTransparent(c));
        if (potentialDark && isDarkRgb(potentialDark)) {
          isExplicitlyDark = true;
        }
        targetRoot.classList.toggle("force-dark-mode", isExplicitlyDark);

        if (airflowBg) {
          targetRoot.style.setProperty("--bg", airflowBg);
        } else if (backgroundColor && !isTransparent(backgroundColor)) {
          targetRoot.style.setProperty("--bg", backgroundColor);
        }
        if (airflowCard) {
          targetRoot.style.setProperty("--card", airflowCard);
        }
        if (airflowLine) {
          targetRoot.style.setProperty("--line", airflowLine);
        }
        if (airflowText) {
          targetRoot.style.setProperty("--text", airflowText);
        } else if (textColor && !isVeryLightRgb(textColor)) {
          targetRoot.style.setProperty("--text", textColor);
        } else {
          targetRoot.style.setProperty("--text", isExplicitlyDark ? "#f8fafc" : "#0f172a");
        }
        if (airflowMuted) {
          targetRoot.style.setProperty("--muted", airflowMuted);
        } else {
          targetRoot.style.setProperty("--muted", isExplicitlyDark ? "#94a3b8" : "#64748b");
        }
        // Copy critical Airflow/Chakra/Bootstrap CSS variables so controls inherit runtime theme.
        for (let i = 0; i < rootStyle.length; i += 1) {
          const key = rootStyle[i];
          if (!key) continue;
          if (key.startsWith("--bs-") || key.startsWith("--chakra-") || key.startsWith("--color-")) {
            const value = rootStyle.getPropertyValue(key);
            if (value) targetRoot.style.setProperty(key, value.trim());
          }
        }
      } catch (_err) {
        // no-op
      }
    }

    function showThemeNotice(message) {
      const box = el("theme_notice");
      box.classList.remove("hidden");
      box.textContent = message;
    }

    function clearThemeNotice() {
      const box = el("theme_notice");
      box.classList.add("hidden");
      box.textContent = "";
    }

    function copyParentThemeAssets() {
      try {
        if (window.parent === window) return 0;
        const parentDoc = window.parent.document;
        const existingLinks = new Set(
          Array.from(document.querySelectorAll('link[rel="stylesheet"][href]')).map((x) => x.href)
        );
        const existingStyleKeys = new Set(
          Array.from(document.querySelectorAll("style[data-airflow-parent-style-key]"))
            .map((x) => x.getAttribute("data-airflow-parent-style-key"))
        );
        let copied = 0;

        const parentLinks = parentDoc.querySelectorAll('link[rel="stylesheet"][href]');
        for (const link of parentLinks) {
          const href = link.href;
          if (!href || existingLinks.has(href)) continue;
          const clone = document.createElement("link");
          clone.rel = "stylesheet";
          clone.href = href;
          document.head.appendChild(clone);
          existingLinks.add(href);
          copied += 1;
        }

        const parentStyles = parentDoc.querySelectorAll("style");
        for (let i = 0; i < parentStyles.length; i += 1) {
          const cssText = parentStyles[i].textContent || "";
          if (!cssText.trim()) continue;
          // Avoid copying Airflow app runtime global rules (opacity, app layout, etc.).
          // Only copy styles that define theme tokens.
          const looksLikeThemeTokenBlock = (
            cssText.includes("--color-bg-main")
            || cssText.includes("--color-text")
            || cssText.includes("--bs-body-bg")
            || cssText.includes("--bs-body-color")
            || cssText.includes("--chakra-colors")
          );
          if (!looksLikeThemeTokenBlock) continue;
          const styleKey = `p_${i}_${cssText.length}_${cssText.slice(0, 32)}`;
          if (existingStyleKeys.has(styleKey)) continue;
          const clone = document.createElement("style");
          clone.setAttribute("data-airflow-parent-style-key", styleKey);
          clone.textContent = cssText;
          document.head.appendChild(clone);
          existingStyleKeys.add(styleKey);
          copied += 1;
        }
        return copied;
      } catch (_err) {
        return 0;
      }
    }

    function parseEntryScriptUrl(shellHtml) {
      try {
        const doc = new DOMParser().parseFromString(shellHtml, "text/html");
        const candidates = Array.from(doc.querySelectorAll('script[type="module"][src]'))
          .map((n) => n.getAttribute("src") || "")
          .filter(Boolean);
        if (!candidates.length) return "";
        const preferred = candidates.find((s) => s.includes("/static/assets/index-")) || candidates[0];
        return new URL(preferred, window.location.origin).toString();
      } catch (_err) {
        return "";
      }
    }

    function extractInjectedCssFromEntryScript(scriptText) {
      const match = scriptText.match(/document\.createTextNode\("((?:\\.|[^"\\])*)"\)/);
      if (!match || !match[1]) return "";
      try {
        return JSON.parse(`"${match[1]}"`);
      } catch (_err) {
        return "";
      }
    }

    function injectPluginCssText(cssText, marker) {
      if (!cssText || !cssText.trim()) return false;
      const existing = document.querySelector(`style[data-airflow-theme="${marker}"]`);
      if (existing) return true;
      const style = document.createElement("style");
      style.setAttribute("data-airflow-theme", marker);
      style.textContent = cssText;
      document.head.appendChild(style);
      return true;
    }

    async function loadThemeFromPluginEntryScript() {
      const cached = sessionStorage.getItem(THEME_CACHE_KEY) || "";
      if (cached && injectPluginCssText(cached, "plugin-script-cache")) {
        return "plugin-script-cache";
      }
      const shellResp = await fetch("/plugin/flow_studio");
      if (!shellResp.ok) {
        throw new Error(`plugin_shell_${shellResp.status}`);
      }
      const shellHtml = await shellResp.text();
      const entryUrl = parseEntryScriptUrl(shellHtml);
      if (!entryUrl) {
        throw new Error("plugin_entry_not_found");
      }
      const entryResp = await fetch(entryUrl);
      if (!entryResp.ok) {
        throw new Error(`plugin_entry_${entryResp.status}`);
      }
      const entryScript = await entryResp.text();
      const cssText = extractInjectedCssFromEntryScript(entryScript);
      if (!cssText) {
        throw new Error("plugin_css_extract_failed");
      }
      sessionStorage.setItem(THEME_CACHE_KEY, cssText);
      if (!injectPluginCssText(cssText, "plugin-script")) {
        throw new Error("plugin_css_inject_failed");
      }
      return "plugin-script";
    }

    // Known static path fallback. Returns true if any CSS link is attached.
    async function tryAttachAirflowCss() {
      const candidates = [
        "/static/dist/main.css",
        "/static/main.css",
        "/static/css/main.css",
        "/static/dist/assets/index.css",
      ];
      for (const href of candidates) {
        try {
          const res = await fetch(href, { method: "HEAD" });
          if (!res.ok) continue;
          const link = document.createElement("link");
          link.rel = "stylesheet";
          link.href = href;
          document.head.appendChild(link);
          return true;
        } catch (_err) {
          // try next candidate
        }
      }
      return false;
    }

    async function applyAirflowThemeAssets() {
      clearThemeNotice();
      const diagnostics = [];

      let parentSynced = false;
      try {
        if (window.parent && window.parent !== window && window.parent.document) {
          syncThemeTokensFromDocument(window.parent.document);
          parentSynced = true;
        }
      } catch (_err) {
        diagnostics.push("parent_cors_or_error");
      }

      const copied = copyParentThemeAssets();
      if (parentSynced || copied > 0) {
        if (!parentSynced) syncThemeTokensFromDocument(document);
        setThemeSource("parent");
        console.info(`[flow-studio-theme] source=parent copied_assets=${copied} direct_sync=${parentSynced}`);
        return;
      }
      diagnostics.push("parent_assets=0");

      try {
        const source = await loadThemeFromPluginEntryScript();
        syncThemeTokensFromDocument(document);
        setThemeSource(source);
        console.info(`[flow-studio-theme] source=${source}`);
        return;
      } catch (err) {
        diagnostics.push(`plugin_script=${String(err && err.message || err)}`);
      }

      const linked = await tryAttachAirflowCss();
      if (linked) {
        syncThemeTokensFromDocument(document);
        setThemeSource("known-static-link");
        console.info("[flow-studio-theme] source=known-static-link");
        return;
      }
      diagnostics.push("known_static_link=0");

      setThemeSource("fallback");
      showThemeNotice("Airflow theme assets could not be loaded; fallback theme is active.");
      console.warn(`[flow-studio-theme] source=fallback ${diagnostics.join(" | ")}`);
    }

    let currentUpdateDagId = "";
    let currentActiveRevisionId = "";
    let currentRevisionItems = [];
    let pendingTaskDeleteCard = null;
    let isBusy = false;
    const CUSTOM_TAG_MAX_COUNT = 10;
    const CUSTOM_TAG_MAX_LENGTH = 32;
    const SCHEDULER_FALLBACK_START_DATE = "2023-01-01T00:00:00";
    const SCHEDULER_DEFAULT_TIMEZONE = "UTC";
    const SCHEDULER_MODES = ["manual", "minutely", "hourly", "daily", "weekly", "monthly", "advanced"];
    const LOAD_METHOD_LABELS = Object.freeze({
      create_if_not_exists_or_truncate: "Create if missing, then truncate",
      append: "Append rows",
      replace: "Replace table data",
      upsert: "Upsert (insert/update)",
      delete_from_table: "Delete from table",
      drop_if_exists_and_create: "Drop and recreate",
      script: "Run script",
    });
    const DEPENDENCY_MODES = Object.freeze({
      PARALLEL: "parallel",
      WAIT_PREVIOUS: "wait_previous",
      CUSTOM: "custom",
    });
    const TASK_TYPES = Object.freeze({
      SOURCE_TARGET: "source_target",
      SCRIPT_RUN: "script_run",
      DAG: "dag",
    });
    const PARTITION_MODE_HINTS = Object.freeze({
      auto_numeric: "MIN/MAX based numeric partitioning. Best for integer/decimal columns.",
      auto_datetime: "MIN/MAX based datetime partitioning. Best for date/timestamp columns.",
      percentile: "Uses percentile boundaries. If unsupported, falls back to auto_numeric.",
      hash_mod: "Splits rows into modulo buckets (MOD/%). Good for evenly distributed keys.",
      distinct: "Builds IN groups from DISTINCT values. May be expensive on high cardinality.",
      explicit: "Manual WHERE fragments. Enter one partition filter per line.",
    });
    const PARTITION_COLUMN_REQUIRED_MODES = new Set([
      "auto_numeric",
      "auto_datetime",
      "percentile",
      "hash_mod",
      "distinct",
    ]);
    const PARTITION_PARTS_REQUIRED_MODES = new Set([
      "auto_numeric",
      "auto_datetime",
      "percentile",
      "hash_mod",
      "distinct",
    ]);
    let customTagsState = [];
    let schedulerModeState = "manual";
    let schedulerAppliedState = null;
    let dagDepsAppliedState = null;
    let dagDepsDraftState = null;
    let dagDepsOptionsState = [];
    let dagDepsReferencedByState = [];
    let pendingDeleteDagCleanupReferences = false;

    function el(id) { return document.getElementById(id); }

    function logDebug(message, payload) {
      if (typeof payload === "undefined") {
        console.debug(`[flow-studio] ${message}`);
        return;
      }
      console.debug(`[flow-studio] ${message}`, payload);
    }

    function normalizeCustomTag(rawValue) {
      return String(rawValue || "")
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_-]+/g, "_")
        .replace(/^[_-]+|[_-]+$/g, "");
    }

    function renderCustomTags() {
      const chipsWrap = el("custom_tags_chips");
      if (!chipsWrap) return;
      chipsWrap.innerHTML = "";
      for (const tag of customTagsState) {
        const chip = document.createElement("span");
        chip.className = "tag-chip";
        chip.textContent = tag;
        const remove = document.createElement("button");
        remove.type = "button";
        remove.className = "tag-chip-remove";
        remove.textContent = "x";
        remove.title = `Remove tag: ${tag}`;
        remove.disabled = !!isBusy;
        remove.onclick = () => {
          customTagsState = customTagsState.filter((item) => item !== tag);
          renderCustomTags();
        };
        chip.appendChild(remove);
        chipsWrap.appendChild(chip);
      }
    }

    function setCustomTags(rawTags) {
      const next = [];
      const seen = new Set();
      const items = Array.isArray(rawTags) ? rawTags : [];
      for (const raw of items) {
        const normalized = normalizeCustomTag(raw);
        if (!normalized) continue;
        if (normalized.length > CUSTOM_TAG_MAX_LENGTH) continue;
        if (seen.has(normalized)) continue;
        seen.add(normalized);
        next.push(normalized);
        if (next.length >= CUSTOM_TAG_MAX_COUNT) break;
      }
      customTagsState = next;
      renderCustomTags();
    }

    function addCustomTag(rawTag) {
      const normalized = normalizeCustomTag(rawTag);
      if (!normalized) return false;
      if (normalized.length > CUSTOM_TAG_MAX_LENGTH) {
        pushToast(`Tag too long (max ${CUSTOM_TAG_MAX_LENGTH} chars).`, "error", true);
        return false;
      }
      if (customTagsState.includes(normalized)) return false;
      if (customTagsState.length >= CUSTOM_TAG_MAX_COUNT) {
        pushToast(`Maximum ${CUSTOM_TAG_MAX_COUNT} tags allowed.`, "error", true);
        return false;
      }
      customTagsState = [...customTagsState, normalized];
      renderCustomTags();
      return true;
    }

    function flushCustomTagInput() {
      const input = el("custom_tags_input");
      if (!input) return;
      const value = String(input.value || "");
      if (!value.trim()) return;
      const parts = value.split(/[,\s]+/g);
      for (const part of parts) {
        addCustomTag(part);
      }
      input.value = "";
    }

    function pad2(value) {
      return String(value).padStart(2, "0");
    }

    function nowDateTimeLocalValue() {
      const now = new Date();
      return `${now.getFullYear()}-${pad2(now.getMonth() + 1)}-${pad2(now.getDate())}T${pad2(now.getHours())}:${pad2(now.getMinutes())}`;
    }

    function nowDateTimeIsoSecondsLocal() {
      const now = new Date();
      return `${now.getFullYear()}-${pad2(now.getMonth() + 1)}-${pad2(now.getDate())}T${pad2(now.getHours())}:${pad2(now.getMinutes())}:${pad2(now.getSeconds())}`;
    }

    function toDateTimeLocalValue(rawValue) {
      const text = String(rawValue || "").trim();
      if (!text) return nowDateTimeLocalValue();
      const normalized = text.replace(" ", "T");
      let parsed = new Date(normalized);
      if (Number.isNaN(parsed.getTime())) {
        parsed = new Date(`${normalized}Z`);
      }
      if (Number.isNaN(parsed.getTime())) {
        const fallback = normalized.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2})/);
        if (fallback) return `${fallback[1]}T${fallback[2]}:${fallback[3]}`;
        return nowDateTimeLocalValue();
      }
      return [
        `${parsed.getFullYear()}-${pad2(parsed.getMonth() + 1)}-${pad2(parsed.getDate())}`,
        `${pad2(parsed.getHours())}:${pad2(parsed.getMinutes())}`,
      ].join("T");
    }

    function normalizeStartDateForPayload(rawValue) {
      const text = String(rawValue || "").trim();
      if (!text) return SCHEDULER_FALLBACK_START_DATE;
      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(text)) return `${text}:00`;
      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/.test(text)) return text;
      return SCHEDULER_FALLBACK_START_DATE;
    }

    function isValidTimezone(value) {
      const tz = String(value || "").trim();
      if (!tz) return false;
      try {
        Intl.DateTimeFormat("en-US", { timeZone: tz });
        return true;
      } catch (_err) {
        return false;
      }
    }

    function resolveBrowserTimezone() {
      try {
        const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
        return isValidTimezone(tz) ? String(tz).trim() : "";
      } catch (_err) {
        return "";
      }
    }

    function fillNumericSelect(selectId, min, max, selectedValue) {
      const select = el(selectId);
      if (!select) return;
      const selected = Number.isFinite(Number(selectedValue)) ? Number(selectedValue) : min;
      select.innerHTML = "";
      for (let i = min; i <= max; i += 1) {
        const opt = document.createElement("option");
        opt.value = String(i);
        opt.textContent = pad2(i);
        if (i === selected) opt.selected = true;
        select.appendChild(opt);
      }
    }

    function resolveSchedulerModeFromCron(cronExpression) {
      const cron = String(cronExpression || "").trim();
      if (!cron) return "manual";
      const fields = cron.split(/\s+/g);
      if (fields.length !== 5) return "advanced";
      const [minute, hour, dayOfMonth, month, dayOfWeek] = fields;
      if (hour === "*" && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
        if (minute === "*") return "minutely";
        const minutelyMatch = minute.match(/^\*\/([1-9]\d?)$/);
        if (minutelyMatch) {
          const step = Number(minutelyMatch[1]);
          if (step >= 1 && step <= 59) return "minutely";
        }
      }
      if (hour === "*" && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") return "hourly";
      if (dayOfMonth === "*" && month === "*" && dayOfWeek === "*" && hour !== "*") return "daily";
      if (dayOfMonth === "*" && month === "*" && dayOfWeek !== "*" && hour !== "*") return "weekly";
      if (dayOfMonth !== "*" && month === "*" && dayOfWeek === "*" && hour !== "*") return "monthly";
      return "advanced";
    }

    function setSchedulerMode(mode) {
      const next = SCHEDULER_MODES.includes(mode) ? mode : "manual";
      schedulerModeState = next;
      for (const btn of document.querySelectorAll(".scheduler-tab-btn")) {
        btn.classList.toggle("active", btn.getAttribute("data-scheduler-tab") === next);
      }
      for (const panel of document.querySelectorAll(".scheduler-panel")) {
        panel.classList.toggle("active", panel.getAttribute("data-scheduler-panel") === next);
      }
      syncSchedulerPreview();
    }

    function buildCronFromSchedulerControls() {
      if (schedulerModeState === "manual") {
        return null;
      }
      if (schedulerModeState === "minutely") {
        const step = Number(el("scheduler_minutely_step")?.value || 1);
        const safe = Math.max(1, Math.min(59, step));
        return safe === 1 ? "* * * * *" : `*/${safe} * * * *`;
      }
      if (schedulerModeState === "hourly") {
        const minute = Number(el("scheduler_hourly_minute")?.value || 0);
        return `${Math.max(0, Math.min(59, minute))} * * * *`;
      }
      if (schedulerModeState === "daily") {
        const hour = Number(el("scheduler_daily_hour")?.value || 0);
        const minute = Number(el("scheduler_daily_minute")?.value || 0);
        return `${Math.max(0, Math.min(59, minute))} ${Math.max(0, Math.min(23, hour))} * * *`;
      }
      if (schedulerModeState === "weekly") {
        const day = Number(el("scheduler_weekly_day")?.value || 0);
        const hour = Number(el("scheduler_weekly_hour")?.value || 0);
        const minute = Number(el("scheduler_weekly_minute")?.value || 0);
        return `${Math.max(0, Math.min(59, minute))} ${Math.max(0, Math.min(23, hour))} * * ${Math.max(0, Math.min(6, day))}`;
      }
      if (schedulerModeState === "monthly") {
        const day = Number(el("scheduler_monthly_day")?.value || 1);
        const hour = Number(el("scheduler_monthly_hour")?.value || 0);
        const minute = Number(el("scheduler_monthly_minute")?.value || 0);
        return `${Math.max(0, Math.min(59, minute))} ${Math.max(0, Math.min(23, hour))} ${Math.max(1, Math.min(31, day))} * *`;
      }
      const advanced = String(el("scheduler_advanced_cron")?.value || "").trim();
      return advanced || null;
    }

    function applyFriendlyLoadMethodLabels(scopeNode) {
      const scope = scopeNode && typeof scopeNode.querySelectorAll === "function" ? scopeNode : document;
      for (const select of scope.querySelectorAll("select.load-method")) {
        for (const option of Array.from(select.options || [])) {
          const friendly = LOAD_METHOD_LABELS[String(option.value || "").trim()];
          if (friendly) option.textContent = friendly;
        }
      }
    }

    function normalizeSchedulerState(rawScheduler) {
      const scheduler = (rawScheduler && typeof rawScheduler === "object") ? rawScheduler : {};
      const cronExpression = String(scheduler.cron_expression || "").trim() || null;
      const timezoneValue = String(scheduler.timezone || "").trim();
      const timezone = isValidTimezone(timezoneValue) ? timezoneValue : SCHEDULER_DEFAULT_TIMEZONE;
      const active = typeof scheduler.active === "boolean" ? scheduler.active : true;
      const startDate = normalizeStartDateForPayload(String(scheduler.start_date || "").trim());
      return {
        cron_expression: cronExpression,
        timezone,
        active,
        start_date: startDate,
      };
    }

    function cloneSchedulerState(state) {
      return {
        cron_expression: state && state.cron_expression ? String(state.cron_expression) : null,
        timezone: String((state && state.timezone) || SCHEDULER_DEFAULT_TIMEZONE),
        active: !!(state && state.active),
        start_date: String((state && state.start_date) || SCHEDULER_FALLBACK_START_DATE),
      };
    }

    function schedulerDetailedSummaryTextFromState(state) {
      const scheduler = cloneSchedulerState(state || {});
      const cron = String(scheduler.cron_expression || "").trim();
      if (!cron) {
        return `Manual mode. Timezone: ${scheduler.timezone}. Active: ${scheduler.active ? "on" : "off"}. Start: ${scheduler.start_date}.`;
      }
      return `Cron: ${cron}. Timezone: ${scheduler.timezone}. Active: ${scheduler.active ? "on" : "off"}. Start: ${scheduler.start_date}.`;
    }

    function isSimpleCronNumber(value, minValue, maxValue) {
      const text = String(value || "").trim();
      if (!/^\d+$/.test(text)) return false;
      const numeric = Number(text);
      return Number.isInteger(numeric) && numeric >= minValue && numeric <= maxValue;
    }

    function schedulerWeekdayName(weekday) {
      const names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
      const numeric = Number(weekday);
      if (!Number.isInteger(numeric) || numeric < 0 || numeric > 6) return "";
      return names[numeric];
    }

    function schedulerCompactBaseSummaryFromState(state) {
      const scheduler = cloneSchedulerState(state || {});
      const cron = String(scheduler.cron_expression || "").trim();
      if (!cron) {
        return "Manual run only";
      }
      const fields = cron.split(/\s+/g);
      if (fields.length !== 5) {
        return "Runs on a custom schedule";
      }
      const [minute, hour, dayOfMonth, month, dayOfWeek] = fields;

      const everyMinutesMatch = minute.match(/^\*\/([1-9]\d?)$/);
      if (everyMinutesMatch && hour === "*" && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
        const step = Number(everyMinutesMatch[1]);
        if (step >= 1 && step <= 59) {
          return `Runs every ${step} minute${step === 1 ? "" : "s"}`;
        }
      }
      if (minute === "*" && hour === "*" && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
        return "Runs every minute";
      }

      const mode = resolveSchedulerModeFromCron(cron);
      if (mode === "minutely") {
        const minutelyMatch = minute.match(/^\*\/([1-9]\d?)$/);
        if (minute === "*") return "Runs every minute";
        if (!minutelyMatch) return "Runs on a custom schedule";
        const step = Number(minutelyMatch[1]);
        if (step < 1 || step > 59) return "Runs on a custom schedule";
        return `Runs every ${step} minute${step === 1 ? "" : "s"}`;
      }
      if (mode === "hourly") {
        if (!isSimpleCronNumber(minute, 0, 59)) return "Runs on a custom schedule";
        return `Runs every hour at minute ${pad2(Number(minute))}`;
      }
      if (mode === "daily") {
        if (!isSimpleCronNumber(hour, 0, 23) || !isSimpleCronNumber(minute, 0, 59)) return "Runs on a custom schedule";
        return `Runs daily at ${pad2(Number(hour))}:${pad2(Number(minute))}`;
      }
      if (mode === "weekly") {
        if (!isSimpleCronNumber(hour, 0, 23) || !isSimpleCronNumber(minute, 0, 59) || !isSimpleCronNumber(dayOfWeek, 0, 6)) {
          return "Runs on a custom schedule";
        }
        const dayName = schedulerWeekdayName(dayOfWeek);
        if (!dayName) return "Runs on a custom schedule";
        return `Runs weekly on ${dayName} at ${pad2(Number(hour))}:${pad2(Number(minute))}`;
      }
      if (mode === "monthly") {
        if (!isSimpleCronNumber(hour, 0, 23) || !isSimpleCronNumber(minute, 0, 59) || !isSimpleCronNumber(dayOfMonth, 1, 31)) {
          return "Runs on a custom schedule";
        }
        return `Runs monthly on day ${Number(dayOfMonth)} at ${pad2(Number(hour))}:${pad2(Number(minute))}`;
      }
      return "Runs on a custom schedule";
    }

    function schedulerCompactSummaryTextFromState(state) {
      const scheduler = cloneSchedulerState(state || {});
      const baseSummary = schedulerCompactBaseSummaryFromState(scheduler);
      if (!scheduler.active) {
        return `Paused - ${baseSummary}`;
      }
      return baseSummary;
    }

    function renderSchedulerCompactSummary() {
      const box = el("scheduler_compact_summary");
      if (!box) return;
      const summary = schedulerCompactSummaryTextFromState(schedulerAppliedState || {});
      box.textContent = summary;
      const panel = el("scheduler_compact_panel");
      if (panel) {
        panel.title = `Scheduler: ${summary}. Click to configure.`;
      }
    }

    function syncSchedulerPreview() {
      const cron = buildCronFromSchedulerControls();
      const preview = el("scheduler_cron_preview");
      const summary = el("scheduler_summary");
      const draft = {
        cron_expression: cron,
        timezone: String(el("scheduler_timezone")?.value || "").trim() || SCHEDULER_DEFAULT_TIMEZONE,
        active: !!el("scheduler_active")?.checked,
        start_date: normalizeStartDateForPayload(el("scheduler_start_date")?.value || ""),
      };
      if (preview) {
        preview.value = cron || "Manual";
      }
      if (summary) {
        summary.textContent = schedulerDetailedSummaryTextFromState(draft);
      }
    }

    async function loadTimezoneOptions(queryText = "", limit = 200) {
      const q = String(queryText || "").trim();
      const endpoint = `/api/timezones?q=${encodeURIComponent(q)}&limit=${encodeURIComponent(limit)}`;
      const r = await studioFetch(endpoint);
      const data = await parseJsonSafe(r);
      if (!r.ok || !data || !data.ok) return { default_timezone: "" };
      const items = Array.isArray(data.items) ? data.items : [];
      const datalist = el("scheduler_timezone_options");
      if (!datalist) return { default_timezone: String(data.default_timezone || "").trim() };
      datalist.innerHTML = "";
      for (const item of items) {
        const value = String(item || "").trim();
        if (!value) continue;
        const option = document.createElement("option");
        option.value = value;
        datalist.appendChild(option);
      }
      return { default_timezone: String(data.default_timezone || "").trim() };
    }

    function collectSchedulerFormPayload() {
      return normalizeSchedulerState({
        cron_expression: buildCronFromSchedulerControls(),
        timezone: String(el("scheduler_timezone")?.value || "").trim() || SCHEDULER_DEFAULT_TIMEZONE,
        active: !!el("scheduler_active")?.checked,
        start_date: normalizeStartDateForPayload(el("scheduler_start_date")?.value || ""),
      });
    }

    function setSchedulerFormFromState(rawScheduler) {
      const scheduler = normalizeSchedulerState(rawScheduler);
      const cronExpression = String(scheduler.cron_expression || "").trim();
      const mode = resolveSchedulerModeFromCron(cronExpression);

      el("scheduler_timezone").value = scheduler.timezone;
      el("scheduler_active").checked = !!scheduler.active;
      el("scheduler_start_date").value = toDateTimeLocalValue(scheduler.start_date || nowDateTimeIsoSecondsLocal());
      el("scheduler_advanced_cron").value = cronExpression;

      const fields = cronExpression ? cronExpression.split(/\s+/g) : [];
      if (fields.length === 5) {
        const [minute, hour, dayOfMonth, _month, dayOfWeek] = fields;
        if (mode === "minutely") {
          if (minute === "*") {
            el("scheduler_minutely_step").value = "1";
          } else {
            const minutelyMatch = minute.match(/^\*\/([1-9]\d?)$/);
            el("scheduler_minutely_step").value = String(Number((minutelyMatch && minutelyMatch[1]) || 1));
          }
        } else if (mode === "hourly") {
          el("scheduler_hourly_minute").value = String(Number(minute) || 0);
        } else if (mode === "daily") {
          el("scheduler_daily_hour").value = String(Number(hour) || 0);
          el("scheduler_daily_minute").value = String(Number(minute) || 0);
        } else if (mode === "weekly") {
          el("scheduler_weekly_day").value = String(Number(dayOfWeek) || 0);
          el("scheduler_weekly_hour").value = String(Number(hour) || 0);
          el("scheduler_weekly_minute").value = String(Number(minute) || 0);
        } else if (mode === "monthly") {
          el("scheduler_monthly_day").value = String(Number(dayOfMonth) || 1);
          el("scheduler_monthly_hour").value = String(Number(hour) || 0);
          el("scheduler_monthly_minute").value = String(Number(minute) || 0);
        }
      }
      setSchedulerMode(mode);
      syncSchedulerPreview();
    }

    function setSchedulerAppliedState(rawScheduler) {
      schedulerAppliedState = normalizeSchedulerState(rawScheduler);
      renderSchedulerCompactSummary();
    }

    function openSchedulerModal() {
      if (isBusy) return;
      const modal = el("scheduler_modal");
      if (!modal) return;
      setSchedulerFormFromState(schedulerAppliedState || {
        cron_expression: null,
        timezone: SCHEDULER_DEFAULT_TIMEZONE,
        active: true,
        start_date: nowDateTimeIsoSecondsLocal(),
      });
      modal.classList.add("open");
      modal.setAttribute("aria-hidden", "false");
      document.body.classList.add("scheduler-modal-open");
      // Always preload full timezone list on open; filtering-by-current-value
      // can collapse the datalist to a single option (e.g. Europe/Istanbul).
      loadTimezoneOptions("", 300);
      el("scheduler_timezone").focus();
    }

    function closeSchedulerModal() {
      const modal = el("scheduler_modal");
      if (!modal) return;
      modal.classList.remove("open");
      modal.setAttribute("aria-hidden", "true");
      document.body.classList.remove("scheduler-modal-open");
    }

    function applySchedulerModal() {
      const next = collectSchedulerFormPayload();
      setSchedulerAppliedState(next);
      closeSchedulerModal();
    }

    async function initializeSchedulerDefaultsForCreate() {
      const browserTimezone = resolveBrowserTimezone();
      const initialTimezone = browserTimezone || SCHEDULER_DEFAULT_TIMEZONE;
      setSchedulerAppliedState({
        cron_expression: null,
        timezone: initialTimezone,
        active: true,
        start_date: nowDateTimeIsoSecondsLocal(),
      });
      setSchedulerFormFromState(schedulerAppliedState);
      const tzData = await loadTimezoneOptions("", 300);
      const backendDefault = String((tzData && tzData.default_timezone) || "").trim();
      if (!browserTimezone && isValidTimezone(backendDefault)) {
        setSchedulerAppliedState({
          ...schedulerAppliedState,
          timezone: backendDefault,
        });
        setSchedulerFormFromState(schedulerAppliedState);
      }
    }

    function bindSchedulerControls() {
      fillNumericSelect("scheduler_minutely_step", 1, 59, 1);
      fillNumericSelect("scheduler_hourly_minute", 0, 59, 0);
      fillNumericSelect("scheduler_daily_hour", 0, 23, 0);
      fillNumericSelect("scheduler_daily_minute", 0, 59, 0);
      fillNumericSelect("scheduler_weekly_hour", 0, 23, 0);
      fillNumericSelect("scheduler_weekly_minute", 0, 59, 0);
      fillNumericSelect("scheduler_monthly_day", 1, 31, 1);
      fillNumericSelect("scheduler_monthly_hour", 0, 23, 0);
      fillNumericSelect("scheduler_monthly_minute", 0, 59, 0);
      for (const tab of document.querySelectorAll(".scheduler-tab-btn")) {
        tab.addEventListener("click", () => setSchedulerMode(tab.getAttribute("data-scheduler-tab") || "manual"));
      }
      for (const node of document.querySelectorAll(
        "#scheduler_minutely_step,#scheduler_hourly_minute,#scheduler_daily_hour,#scheduler_daily_minute,#scheduler_weekly_day,#scheduler_weekly_hour,#scheduler_weekly_minute,#scheduler_monthly_day,#scheduler_monthly_hour,#scheduler_monthly_minute,#scheduler_advanced_cron,#scheduler_timezone,#scheduler_start_date,#scheduler_active"
      )) {
        node.addEventListener("change", syncSchedulerPreview);
        node.addEventListener("input", syncSchedulerPreview);
      }
      const timezoneInput = el("scheduler_timezone");
      if (timezoneInput) {
        timezoneInput.addEventListener("focus", () => loadTimezoneOptions("", 300));
        timezoneInput.addEventListener("input", () => {
          clearTimeout(timezoneInput._ffTimezoneTimer);
          timezoneInput._ffTimezoneTimer = setTimeout(() => {
            loadTimezoneOptions(timezoneInput.value || "", 300);
          }, 180);
        });
      }
    }

    function normalizeDagDependencyIds(rawIds) {
      const items = Array.isArray(rawIds) ? rawIds : [];
      const out = [];
      const seen = new Set();
      for (const raw of items) {
        const dagId = String(raw || "").trim();
        if (!dagId || seen.has(dagId)) continue;
        seen.add(dagId);
        out.push(dagId);
      }
      return out;
    }

    function cloneDagDepsState(state) {
      const raw = state && typeof state === "object" ? state : {};
      return {
        upstream_dag_ids: normalizeDagDependencyIds(raw.upstream_dag_ids || []),
      };
    }

    function resolveDagDepsUpstreamIds(state) {
      const safeState = cloneDagDepsState(state);
      const optionIds = new Set(
        (Array.isArray(dagDepsOptionsState) ? dagDepsOptionsState : [])
          .map((item) => String(item && item.dag_id || "").trim())
          .filter(Boolean)
      );
      return normalizeDagDependencyIds(safeState.upstream_dag_ids)
        .filter((dagId) => optionIds.has(dagId));
    }

    function sortDagDependencyOptionsByDagId(items) {
      const rows = Array.isArray(items) ? [...items] : [];
      rows.sort((left, right) => {
        const leftDagIdRaw = String((left && left.dag_id) || "").trim();
        const rightDagIdRaw = String((right && right.dag_id) || "").trim();
        const leftDagId = leftDagIdRaw.toLowerCase();
        const rightDagId = rightDagIdRaw.toLowerCase();
        const byNormalized = leftDagId.localeCompare(rightDagId);
        if (byNormalized !== 0) return byNormalized;
        return leftDagIdRaw.localeCompare(rightDagIdRaw);
      });
      return rows;
    }

    function summarizeDagDepsCompact(state) {
      const selectedDagIds = resolveDagDepsUpstreamIds(state);
      if (!selectedDagIds.length) return "No upstream DAG";
      const labels = selectedDagIds.slice(0, 2);
      if (selectedDagIds.length <= 2) {
        return `Upstream: ${labels.join(", ")}`;
      }
      return `Upstream: ${labels.join(", ")} +${selectedDagIds.length - 2} more`;
    }

    function renderDagDepsCompactSummary() {
      const summaryNode = el("dag_deps_compact_summary");
      if (!summaryNode) return;
      const summaryText = summarizeDagDepsCompact(dagDepsAppliedState || {});
      summaryNode.textContent = summaryText;
      const panel = el("dag_deps_compact_panel");
      if (panel) panel.title = `DAG Dependencies: ${summaryText}. Click to configure.`;
    }

    function renderDagDepsModal() {
      const customWrap = el("dag_deps_custom_wrap");
      const customSelect = el("dag_deps_custom_select");
      const customChips = el("dag_deps_custom_chips");
      const summary = el("dag_deps_summary");
      const addButton = el("btn_add_dag_dependency");
      if (!customWrap || !customSelect || !customChips || !summary || !addButton) return;

      const draft = cloneDagDepsState(dagDepsDraftState || dagDepsAppliedState || {});
      customWrap.classList.remove("hidden");
      const selectedCustom = resolveDagDepsUpstreamIds(draft);
      customSelect.innerHTML = "";
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = dagDepsOptionsState.length ? "Select upstream DAG" : "No upstream DAG";
      customSelect.appendChild(placeholder);

      for (const item of dagDepsOptionsState) {
        const optionDagId = String(item && item.dag_id || "").trim();
        if (!optionDagId) continue;
        const opt = document.createElement("option");
        opt.value = optionDagId;
        opt.textContent = `${optionDagId} (${item.level || "-"} / ${item.flow || "-"})`;
        opt.disabled = selectedCustom.includes(optionDagId);
        customSelect.appendChild(opt);
      }

      customChips.innerHTML = "";
      for (const upstreamDagId of selectedCustom) {
        const chip = document.createElement("span");
        chip.className = "dependency-chip";
        chip.textContent = upstreamDagId;
        const remove = document.createElement("button");
        remove.type = "button";
        remove.className = "dependency-chip-remove";
        remove.textContent = "x";
        remove.title = `Remove upstream DAG: ${upstreamDagId}`;
        remove.disabled = !!isBusy;
        remove.addEventListener("click", () => {
          const nextState = cloneDagDepsState(dagDepsDraftState || {});
          nextState.upstream_dag_ids = normalizeDagDependencyIds(
            nextState.upstream_dag_ids.filter((item) => item !== upstreamDagId)
          );
          dagDepsDraftState = nextState;
          renderDagDepsModal();
        });
        chip.appendChild(remove);
        customChips.appendChild(chip);
      }

      summary.textContent = summarizeDagDepsCompact(draft);
      addButton.disabled = !!isBusy || !dagDepsOptionsState.length;
    }

    function setDagDepsAppliedStateFromUpstreamIds(upstreamDagIds) {
      const normalized = normalizeDagDependencyIds(upstreamDagIds || []);
      dagDepsAppliedState = {
        upstream_dag_ids: normalized,
      };
      renderDagDepsCompactSummary();
    }

    function reconcileDagDepsAppliedState() {
      if (!dagDepsAppliedState) {
        dagDepsAppliedState = {
          upstream_dag_ids: [],
        };
      }
      const nextState = cloneDagDepsState(dagDepsAppliedState);
      const optionIds = new Set(
        (Array.isArray(dagDepsOptionsState) ? dagDepsOptionsState : [])
          .map((item) => String(item && item.dag_id || "").trim())
          .filter(Boolean)
      );
      nextState.upstream_dag_ids = normalizeDagDependencyIds(nextState.upstream_dag_ids)
        .filter((dagId) => optionIds.has(dagId));
      dagDepsAppliedState = nextState;
      renderDagDepsCompactSummary();
    }

    async function loadDagDependencyOptions(rawDagId) {
      const project = String(el("project")?.value || "").trim();
      const domain = String(el("domain")?.value || "").trim();
      const level = String(el("level")?.value || "").trim();
      const flow = String(el("flow")?.value || "").trim();
      if (!project || !domain || !level || !flow) {
        dagDepsOptionsState = [];
        dagDepsReferencedByState = [];
        reconcileDagDepsAppliedState();
        refreshAllDagTaskOptions();
        return null;
      }

      const dagId = String(rawDagId || currentUpdateDagId || "").trim();
      const params = new URLSearchParams();
      params.set("project", project);
      params.set("domain", domain);
      params.set("level", level);
      params.set("flow", flow);
      if (dagId) params.set("dag_id", dagId);
      const response = await studioFetch(`/api/dag-options?${params.toString()}`);
      const data = await parseJsonSafe(response);
      if (!response.ok || !data || !data.ok) {
        dagDepsOptionsState = [];
        dagDepsReferencedByState = [];
        reconcileDagDepsAppliedState();
        refreshAllDagTaskOptions();
        return null;
      }

      dagDepsOptionsState = sortDagDependencyOptionsByDagId(data.items);
      dagDepsReferencedByState = Array.isArray(data.referenced_by) ? data.referenced_by : [];

      if (!dagDepsAppliedState) {
        setDagDepsAppliedStateFromUpstreamIds(data.current_upstream_dag_ids || []);
      } else {
        reconcileDagDepsAppliedState();
      }
      refreshAllDagTaskOptions();
      return data;
    }

    function refreshDagTaskOptions(card) {
      const selectNode = card && card.querySelector(".dag-task-dag-id");
      if (!selectNode) return;
      const currentValue = String(selectNode.value || "").trim();
      const pending = String(card.dataset.pendingDagTaskDagId || "").trim();
      const preferred = pending || currentValue;
      const options = Array.isArray(dagDepsOptionsState) ? dagDepsOptionsState : [];
      selectNode.innerHTML = "";
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = options.length ? "Select DAG" : "No DAG";
      selectNode.appendChild(placeholder);
      for (const item of options) {
        const dagId = String(item && item.dag_id || "").trim();
        if (!dagId) continue;
        const opt = document.createElement("option");
        opt.value = dagId;
        opt.textContent = `${dagId} (${item.level || "-"} / ${item.flow || "-"})`;
        selectNode.appendChild(opt);
      }
      if (preferred && options.some((item) => String(item && item.dag_id || "").trim() === preferred)) {
        selectNode.value = preferred;
      } else {
        selectNode.value = "";
      }
      delete card.dataset.pendingDagTaskDagId;
    }

    function refreshAllDagTaskOptions() {
      for (const card of getTaskCards()) {
        refreshDagTaskOptions(card);
      }
    }

    async function openDagDepsModal() {
      if (isBusy) return;
      const modal = el("dag_deps_modal");
      if (!modal) return;
      await loadDagDependencyOptions(currentUpdateDagId).catch((_err) => {});
      dagDepsDraftState = cloneDagDepsState(dagDepsAppliedState || {
        upstream_dag_ids: [],
      });
      renderDagDepsModal();
      modal.classList.add("open");
      modal.setAttribute("aria-hidden", "false");
      document.body.classList.add("dag-deps-modal-open");
    }

    function closeDagDepsModal() {
      const modal = el("dag_deps_modal");
      if (!modal) return;
      modal.classList.remove("open");
      modal.setAttribute("aria-hidden", "true");
      document.body.classList.remove("dag-deps-modal-open");
      dagDepsDraftState = null;
    }

    function applyDagDepsModal() {
      const draft = cloneDagDepsState(dagDepsDraftState || dagDepsAppliedState || {});
      dagDepsAppliedState = draft;
      renderDagDepsCompactSummary();
      closeDagDepsModal();
    }

    function collectDagDependenciesPayload() {
      const upstreamDagIds = resolveDagDepsUpstreamIds(dagDepsAppliedState || {});
      return {
        upstream_dag_ids: upstreamDagIds,
      };
    }

    function pushToast(message, variant = "success", persistent = false) {
      const container = el("toast_container");
      const normalizedMessage = normalizeApiDetail(message);
      if (!container || !normalizedMessage) return;
      const node = document.createElement("div");
      node.className = `toast ${variant === "error" ? "error" : "success"}`;
      const text = document.createElement("div");
      text.className = "toast-message";
      text.textContent = normalizedMessage;
      const close = document.createElement("button");
      close.type = "button";
      close.className = "toast-close";
      close.textContent = "x";
      close.setAttribute("aria-label", "Close");
      close.onclick = () => node.remove();
      node.appendChild(text);
      node.appendChild(close);
      container.appendChild(node);
      if (!persistent) {
        window.setTimeout(() => node.remove(), 3800);
      }
    }

    function normalizeApiDetail(detail) {
      if (detail == null) return "";
      if (typeof detail === "string") return detail.trim();
      if (Array.isArray(detail)) {
        const parts = detail
          .map((item) => {
            if (item == null) return "";
            if (typeof item === "string") return item.trim();
            if (typeof item === "object") {
              const msg = String(item.msg || item.message || "").trim();
              const locRaw = Array.isArray(item.loc) ? item.loc.join(".") : String(item.loc || "").trim();
              const loc = locRaw ? `${locRaw}: ` : "";
              const direct = `${loc}${msg}`.trim();
              if (direct) return direct;
              try {
                return JSON.stringify(item);
              } catch (_err) {
                return String(item);
              }
            }
            return String(item).trim();
          })
          .filter(Boolean);
        return parts.join(" | ");
      }
      if (typeof detail === "object") {
        const nestedDetail = detail.detail;
        if (nestedDetail != null && nestedDetail !== detail) {
          const nested = normalizeApiDetail(nestedDetail);
          if (nested) return nested;
        }
        const msg = String(detail.message || detail.msg || "").trim();
        if (msg) return msg;
        try {
          return JSON.stringify(detail);
        } catch (_err) {
          return String(detail);
        }
      }
      return String(detail).trim();
    }

    function apiErrorMessage(data, fallbackMessage) {
      const normalized = normalizeApiDetail(data && data.detail);
      return normalized || fallbackMessage;
    }

    function setOperationBusy(active, label) {
      isBusy = !!active;
      const progress = el("operation_progress");
      const progressLabel = el("operation_progress_label");
      if (progress) {
        progress.classList.toggle("hidden", !active);
        progress.setAttribute("aria-busy", active ? "true" : "false");
      }
      if (progressLabel) {
        progressLabel.textContent = active ? (label || "Operation in progress") : "";
      }
      for (const btn of document.querySelectorAll(".btn-create-dag, #btn_update_top, #btn_promote_revision, #btn_add_task, #btn_refresh_revisions, #btn_delete_dag, #btn_cancel_delete_dag, #btn_confirm_delete_dag, #btn_cancel_scheduler_modal, #btn_apply_scheduler_modal, #btn_cancel_dag_deps_modal, #btn_apply_dag_deps_modal, #btn_add_dag_dependency, #btn_cancel_task_delete, #btn_confirm_task_delete, .btn-delete-task, .task-type-chip")) {
        btn.disabled = !!active;
      }
      const schedulerCompactPanel = el("scheduler_compact_panel");
      if (schedulerCompactPanel) {
        schedulerCompactPanel.classList.toggle("disabled", !!active);
        schedulerCompactPanel.setAttribute("aria-disabled", active ? "true" : "false");
      }
      const dagDepsCompactPanel = el("dag_deps_compact_panel");
      if (dagDepsCompactPanel) {
        dagDepsCompactPanel.classList.toggle("disabled", !!active);
        dagDepsCompactPanel.setAttribute("aria-disabled", active ? "true" : "false");
      }
      const customTagInput = el("custom_tags_input");
      if (customTagInput) {
        customTagInput.disabled = !!active;
      }
      for (const node of document.querySelectorAll(".scheduler-control-input, .scheduler-tab-btn")) {
        node.disabled = !!active;
      }
      for (const node of document.querySelectorAll("#dag_deps_custom_select")) {
        node.disabled = !!active;
      }
      for (const node of document.querySelectorAll(".dependency-mode, .dependency-custom-select, .btn-add-dependency")) {
        node.disabled = !!active;
      }
      renderCustomTags();
      for (const card of getTaskCards()) {
        syncDependencyState(card);
      }
      syncDeleteDagConfirmState();
      syncTaskDeleteConfirmState();
      renderDagDepsModal();
    }

    function beginOperation(label) {
      if (isBusy) {
        pushToast("Another operation is already in progress.", "error", true);
        return false;
      }
      setOperationBusy(true, label);
      return true;
    }

    function endOperation() {
      setOperationBusy(false, "");
    }
    function setUpdateModeStatus(message, variant) {
      const box = el("update_mode_status");
      if (!box) return;
      box.classList.remove("hidden", "ok", "warn");
      if (variant === "ok") box.classList.add("ok");
      if (variant === "warn") box.classList.add("warn");
      box.textContent = message;
    }

    function setUpdateMode(active) {
      const top = el("update_actions_top");
      const bottomCreate = el("create_actions_bottom");
      const revisionPanel = el("revision_panel");
      const deleteButton = el("btn_delete_dag");
      if (active) {
        top.classList.remove("hidden");
        bottomCreate.classList.add("hidden");
        revisionPanel.classList.remove("hidden");
        if (deleteButton) deleteButton.classList.remove("hidden");
      } else {
        top.classList.add("hidden");
        bottomCreate.classList.remove("hidden");
        revisionPanel.classList.add("hidden");
        if (deleteButton) deleteButton.classList.add("hidden");
        closeDeleteDagModal();
        closeDagDepsModal();
        closeTaskDeleteModal();
        currentUpdateDagId = "";
        currentActiveRevisionId = "";
        currentRevisionItems = [];
        const sel = el("revision_select");
        if (sel) sel.innerHTML = '<option value="">No revision</option>';
        const meta = el("revision_meta");
        if (meta) meta.textContent = "";
      }
    }

    function resetStudioAfterDelete() {
      currentUpdateDagId = "";
      setCustomTags([]);
      dagDepsAppliedState = {
        upstream_dag_ids: [],
      };
      dagDepsDraftState = null;
      dagDepsReferencedByState = [];
      renderDagDepsCompactSummary();
      setSchedulerAppliedState({
        cron_expression: null,
        timezone: resolveBrowserTimezone() || SCHEDULER_DEFAULT_TIMEZONE,
        active: true,
        start_date: nowDateTimeIsoSecondsLocal(),
      });
      setSchedulerFormFromState(schedulerAppliedState);
      closeSchedulerModal();
      clearAndLoadTasks([{}]);
      setUpdateMode(false);
      loadDagDependencyOptions("").catch((_err) => {});
      try {
        const url = new URL(window.location.href);
        url.searchParams.delete("dag_id");
        window.history.replaceState({}, "", url.toString());
      } catch (_err) {
        // no-op
      }
    }

    function redirectToDagListAfterDelete(deletedDagId) {
      const dagId = String(deletedDagId || "").trim();
      try {
        const current = new URL(window.location.href);
        const marker = "/dags/";
        const path = current.pathname || "";
        const idx = path.indexOf(marker);
        const basePrefix = idx >= 0 ? path.slice(0, idx) : "";
        const target = new URL(`${basePrefix}/dags`, current.origin);
        target.searchParams.set("_ts", String(Date.now()));
        if (dagId) target.searchParams.set("deleted_dag_id", dagId);
        window.location.assign(target.toString());
      } catch (_err) {
        window.location.assign("/dags");
      }
    }

    function syncTaskDeleteConfirmState() {
      const confirmBtn = el("btn_confirm_task_delete");
      if (!confirmBtn) return;
      const canConfirm = !!pendingTaskDeleteCard && !isBusy;
      confirmBtn.disabled = !canConfirm;
      confirmBtn.setAttribute("aria-disabled", canConfirm ? "false" : "true");
    }

    function openTaskDeleteModal(taskCard) {
      const modal = el("delete_task_modal");
      if (!modal || !taskCard) return;
      pendingTaskDeleteCard = taskCard;
      modal.classList.add("open");
      modal.setAttribute("aria-hidden", "false");
      syncTaskDeleteConfirmState();
    }

    function closeTaskDeleteModal() {
      const modal = el("delete_task_modal");
      if (!modal) return;
      pendingTaskDeleteCard = null;
      modal.classList.remove("open");
      modal.setAttribute("aria-hidden", "true");
      syncTaskDeleteConfirmState();
    }

    function confirmTaskDelete() {
      const taskCard = pendingTaskDeleteCard;
      closeTaskDeleteModal();
      if (!taskCard) return;
      taskCard.remove();
      refreshTaskCardHeaders();
    }

    function requestTaskDelete(taskCard) {
      const cards = getTaskCards();
      if (!taskCard || cards.length <= 1 || isBusy) return;
      if (!hasIncomingDependencyForCard(taskCard)) {
        taskCard.remove();
        refreshTaskCardHeaders();
        return;
      }
      openTaskDeleteModal(taskCard);
    }

    function syncDeleteDagConfirmState() {
      const input = el("delete_dag_confirm_input");
      const expected = String(currentUpdateDagId || "").trim();
      const confirmBtn = el("btn_confirm_delete_dag");
      if (!input || !confirmBtn) return;
      const matches = !!expected && String(input.value || "").trim() === expected;
      confirmBtn.disabled = !matches || isBusy;
      confirmBtn.setAttribute("aria-disabled", matches && !isBusy ? "false" : "true");
    }

    async function openDeleteDagModal() {
      const dagId = String(currentUpdateDagId || "").trim();
      if (!dagId) {
        pushToast("Update mode must be active before delete.", "error", true);
        return;
      }
      const modal = el("delete_dag_modal");
      const expected = el("delete_dag_expected");
      const input = el("delete_dag_confirm_input");
      const referencesWarning = el("delete_dag_references_warning");
      if (!modal || !expected || !input || !referencesWarning) return;

      pendingDeleteDagCleanupReferences = false;
      referencesWarning.classList.add("hidden");
      referencesWarning.textContent = "";
      try {
        const optionsData = await loadDagDependencyOptions(dagId);
        const referencedBy = Array.isArray(optionsData && optionsData.referenced_by)
          ? optionsData.referenced_by
          : [];
        if (referencedBy.length) {
          pendingDeleteDagCleanupReferences = true;
          referencesWarning.textContent = `This DAG is referenced by ${referencedBy.length} DAG(s). Deleting it will remove those references.`;
          referencesWarning.classList.remove("hidden");
        }
      } catch (_err) {
        pendingDeleteDagCleanupReferences = false;
      }
      expected.textContent = dagId;
      input.value = "";
      modal.classList.add("open");
      modal.setAttribute("aria-hidden", "false");
      syncDeleteDagConfirmState();
      input.focus();
    }

    function closeDeleteDagModal() {
      const modal = el("delete_dag_modal");
      const input = el("delete_dag_confirm_input");
      const referencesWarning = el("delete_dag_references_warning");
      if (!modal) return;
      modal.classList.remove("open");
      modal.setAttribute("aria-hidden", "true");
      if (input) input.value = "";
      if (referencesWarning) {
        referencesWarning.classList.add("hidden");
        referencesWarning.textContent = "";
      }
      pendingDeleteDagCleanupReferences = false;
      syncDeleteDagConfirmState();
    }

    async function deleteCurrentDag() {
      const dagId = String(currentUpdateDagId || "").trim();
      if (!dagId) {
        pushToast("Update mode must be active before delete.", "error", true);
        return;
      }
      const input = el("delete_dag_confirm_input");
      if (!input || String(input.value || "").trim() !== dagId) {
        pushToast("Enter the exact DAG ID to confirm.", "error", true);
        return;
      }
      if (!beginOperation("Deleting DAG...")) {
        return;
      }
      try {
        const cleanupFlag = pendingDeleteDagCleanupReferences ? "&cleanup_references=true" : "";
        const data = await deleteJson(
          studioUrl(`/api/delete-dag?dag_id=${encodeURIComponent(dagId)}${cleanupFlag}`)
        );
        if (!data || !data.ok) {
          pushToast(apiErrorMessage(data, "DAG deletion failed."), "error", true);
          return;
        }
        closeDeleteDagModal();
        const deletedCount = Array.isArray(data.deleted_paths) ? data.deleted_paths.length : 0;
        pushToast(`DAG deleted: ${dagId} (${deletedCount} items)`, "success", false);
        const warnings = Array.isArray(data.warnings) ? data.warnings : [];
        for (const warning of warnings) {
          if (!warning) continue;
          logDebug("delete warning", warning);
        }
        resetStudioAfterDelete();
        redirectToDagListAfterDelete(dagId);
      } catch (err) {
        logDebug("delete dag error", err);
        pushToast("Unexpected error occurred during DAG deletion.", "error", true);
      } finally {
        endOperation();
      }
    }

    function renderRevisionMeta() {
      const sel = el("revision_select");
      const meta = el("revision_meta");
      if (!sel || !meta) return;
      const revisionId = String(sel.value || "").trim();
      if (!revisionId) {
        meta.textContent = currentActiveRevisionId
          ? `Active revision: ${currentActiveRevisionId}`
          : "Active revision snapshot not found in history.";
        meta.title = meta.textContent;
        return;
      }
      const item = currentRevisionItems.find((x) => String(x.revision_id || "") === revisionId);
      if (!item) {
        meta.textContent = "";
        meta.title = "";
        return;
      }
      const activeMark = currentActiveRevisionId && currentActiveRevisionId === revisionId ? " (active)" : "";
      meta.textContent = `${item.revision_id}${activeMark} - ${item.source || "unknown"} - ${item.created_at || "-"}`;
      meta.title = meta.textContent;
    }

    function renderRevisionOptions(items, activeRevisionId) {
      const sel = el("revision_select");
      if (!sel) return;
      currentRevisionItems = Array.isArray(items) ? items : [];
      currentActiveRevisionId = String(activeRevisionId || "").trim();
      sel.innerHTML = "";
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = currentRevisionItems.length ? "Select revision" : "No revision";
      sel.appendChild(placeholder);
      for (const item of currentRevisionItems) {
        const opt = document.createElement("option");
        const rid = String(item.revision_id || "").trim();
        const activeMark = currentActiveRevisionId && currentActiveRevisionId === rid ? " [active]" : "";
        opt.value = rid;
        opt.textContent = `${rid}${activeMark} - ${String(item.source || "unknown")} - ${String(item.created_at || "-")}`;
        sel.appendChild(opt);
      }
      if (currentActiveRevisionId) {
        sel.value = currentActiveRevisionId;
      }
      renderRevisionMeta();
    }

    async function loadRevisions(rawDagId) {
      const dagId = String(rawDagId || currentUpdateDagId || "").trim();
      if (!dagId) {
        renderRevisionOptions([], "");
        return null;
      }
      const r = await studioFetch(`/api/dag-revisions?dag_id=${encodeURIComponent(dagId)}`);
      const data = await r.json();
      logDebug("dag-revisions response", { status_code: r.status, ...data });
      if (!r.ok || !data.ok) {
        renderRevisionOptions([], "");
        return null;
      }
      renderRevisionOptions(data.items || [], data.active_revision_id || "");
      return data;
    }

    async function promoteSelectedRevision() {
      const dagId = String(currentUpdateDagId || "").trim();
      if (!dagId) {
        setUpdateModeStatus("Update mode must be active before promote.", "warn");
        pushToast("Update mode must be active before promote.", "error", true);
        return;
      }
      const sel = el("revision_select");
      const revisionId = String((sel && sel.value) || "").trim();
      if (!revisionId) {
        setUpdateModeStatus("Select a revision to promote.", "warn");
        pushToast("Select a revision to promote.", "error", true);
        return;
      }

      if (!beginOperation("Activating revision...")) {
        return;
      }
      try {
        const data = await postJson(
          studioUrl(`/api/dag-revisions/promote?dag_id=${encodeURIComponent(dagId)}&revision_id=${encodeURIComponent(revisionId)}`),
          {}
        );
        if (!data || !data.ok) {
          setUpdateModeStatus("Revision promote failed.", "warn");
          pushToast(apiErrorMessage(data, "Revision promote failed."), "error", true);
          return;
        }
        setUpdateModeStatus(`Revision activated: ${revisionId}`, "ok");
        pushToast(`Revision activated: ${revisionId}`, "success", false);
        await preloadByDagId(dagId);
      } catch (err) {
        logDebug("revision promote error", err);
        setUpdateModeStatus("Unexpected error occurred during revision promote.", "warn");
        pushToast("Unexpected error occurred during revision promote.", "error", true);
      } finally {
        endOperation();
      }
    }

    function parseJsonArray(raw) {
      const text = (raw || "").trim();
      if (!text) return [];
      try {
        const parsed = JSON.parse(text);
        return Array.isArray(parsed) ? parsed : [];
      } catch (_err) {
        return [];
      }
    }

    function rangesToMultilineText(raw) {
      if (!Array.isArray(raw)) return "";
      return raw
        .map((item) => {
          if (typeof item === "string") return item.trim();
          try {
            return JSON.stringify(item);
          } catch (_err) {
            return String(item || "").trim();
          }
        })
        .filter((item) => !!item)
        .join("\n");
    }

    function parseExplicitWhereList(raw) {
      return String(raw || "")
        .split(/\r?\n/g)
        .map((line) => line.trim())
        .filter((line) => !!line);
    }

    function asPositiveInt(value, fallback) {
      const n = Number(value);
      if (Number.isInteger(n) && n > 0) return n;
      return fallback;
    }

    function normalizeDependsOnList(rawDependsOn) {
      const items = Array.isArray(rawDependsOn) ? rawDependsOn : [];
      const out = [];
      const seen = new Set();
      for (const raw of items) {
        const depId = String(raw || "").trim();
        if (!depId || seen.has(depId)) continue;
        seen.add(depId);
        out.push(depId);
      }
      return out;
    }

    function getCardDependencyMode(card) {
      const mode = String(card.dataset.dependencyMode || DEPENDENCY_MODES.PARALLEL).trim();
      if (mode === DEPENDENCY_MODES.WAIT_PREVIOUS || mode === DEPENDENCY_MODES.CUSTOM) return mode;
      return DEPENDENCY_MODES.PARALLEL;
    }

    function setCardDependencyMode(card, mode) {
      const normalized = (mode === DEPENDENCY_MODES.WAIT_PREVIOUS || mode === DEPENDENCY_MODES.CUSTOM)
        ? mode
        : DEPENDENCY_MODES.PARALLEL;
      card.dataset.dependencyMode = normalized;
      const modeSelect = card.querySelector(".dependency-mode");
      if (modeSelect && modeSelect.value !== normalized) {
        modeSelect.value = normalized;
      }
    }

    function getCardCustomDependsOn(card) {
      try {
        return normalizeDependsOnList(JSON.parse(String(card.dataset.customDependsOn || "[]")));
      } catch (_err) {
        return [];
      }
    }

    function setCardCustomDependsOn(card, dependsOn) {
      card.dataset.customDependsOn = JSON.stringify(normalizeDependsOnList(dependsOn));
    }

    function slugify(raw, fallback) {
      const out = String(raw || "")
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_]+/g, "_")
        .replace(/^_+|_+$/g, "");
      return out || fallback;
    }

    function setConnectionValue(selectId, connId) {
      const select = el(selectId);
      if (!connId) return;
      const has = Array.from(select.options).some((opt) => opt.value === connId);
      if (!has) {
        const opt = document.createElement("option");
        opt.value = connId;
        opt.textContent = `${connId} (missing)`;
        select.appendChild(opt);
      }
      select.value = connId;
    }

    function fillConnectionSelect(selectId, items, preferredConnId) {
      const select = el(selectId);
      select.innerHTML = "";
      if (!items.length) {
        const opt = document.createElement("option");
        opt.value = "";
        opt.textContent = "No connections found";
        select.appendChild(opt);
        return;
      }
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = selectId === "source_conn_id"
        ? "Select Source DB Connection"
        : "Select Target DB Connection";
      select.appendChild(placeholder);
      for (const item of items) {
        const opt = document.createElement("option");
        opt.value = item.conn_id;
        const suffix = item.conn_type ? ` (${item.conn_type})` : "";
        opt.textContent = `${item.conn_id}${suffix}`;
        select.appendChild(opt);
      }
      const matched = items.some((x) => x.conn_id === preferredConnId);
      select.value = matched ? preferredConnId : "";
    }

    function fillOptions(listId, items) {
      const list = el(listId);
      if (!list) return;
      list.innerHTML = "";
      for (const item of items || []) {
        const opt = document.createElement("option");
        opt.value = item;
        list.appendChild(opt);
      }
    }

    function getSelectedConnectionType(selectId) {
      const select = el(selectId);
      if (!select || !select.selectedOptions || !select.selectedOptions.length) return "";
      const text = String(select.selectedOptions[0].textContent || "");
      const m = text.match(/\(([^)]+)\)\s*$/);
      return m ? String(m[1] || "").trim().toLowerCase() : "";
    }

    async function parseJsonSafe(resp) {
      try {
        return await resp.json();
      } catch (_err) {
        return {};
      }
    }

    let airflowVariableKeys = [];

    function setAirflowVariableOptions(items) {
      airflowVariableKeys = Array.from(new Set((items || []).map((x) => String(x || "").trim()).filter(Boolean))).sort();
      fillOptions("airflow_variable_options", airflowVariableKeys);
    }

    async function loadAirflowVariables(search = "") {
      try {
        const query = (search || "").trim();
        const path = query
          ? `/api/airflow-variables?q=${encodeURIComponent(query)}&limit=500`
          : "/api/airflow-variables?limit=500";
        const r = await studioFetch(path);
        const data = await parseJsonSafe(r);
        if (!r.ok || !data.ok) {
          // Airflow Variable list is only used for the optional field in Bindings.
          // Therefore, we do not pollute the main UI error area here.
          console.warn("Airflow variables could not be loaded.", r.status, data);
          setAirflowVariableOptions([]);
          return;
        }
        setAirflowVariableOptions(data.items || []);
      } catch (err) {
        console.warn("Airflow variables could not be loaded.", err);
        setAirflowVariableOptions([]);
      }
    }

    let sourceSchemaTimer = null;
    let sourceTableTimer = null;
    let targetSchemaTimer = null;
    let targetTableTimer = null;

    async function autocompleteSchemas(connId, q, listId, connSelectId) {
      if (!connId || !q || q.length < 3) return;
      const path = `/api/schemas?conn_id=${encodeURIComponent(connId)}&q=${encodeURIComponent(q)}&limit=50`;
      const r = await studioFetch(path);
      const data = await parseJsonSafe(r);
      if (!r.ok || !data.ok) {
        logDebug("schema autocomplete failed", { status_code: r.status, ...data });
        return;
      }
      const rawItems = Array.isArray(data.items) ? data.items : [];
      const query = String(q || "").trim().toLowerCase();
      const filtered = rawItems.filter((x) => String(x || "").toLowerCase().includes(query));
      fillOptions(listId, filtered);
      if (!filtered.length) {
        const connType = getSelectedConnectionType(connSelectId || "");
        const extra = connType === "mssql" ? " For MSSQL, schema is usually 'dbo'." : "";
        logDebug("schema autocomplete no match", { ok: true, detail: `No schema match found for '${q}'.${extra}` });
      }
    }

    async function autocompleteTables(connId, schema, q, listId) {
      if (!connId || !q || q.length < 3) return;
      if (!schema || !schema.trim()) {
        logDebug("table autocomplete skipped", { ok: false, detail: "Enter at least 1 character for schema first." });
        return;
      }
      const path = `/api/tables?conn_id=${encodeURIComponent(connId)}&schema=${encodeURIComponent(schema)}&q=${encodeURIComponent(q)}&limit=50&offset=0`;
      const r = await studioFetch(path);
      const data = await parseJsonSafe(r);
      if (!r.ok || !data.ok) {
        logDebug("table autocomplete failed", { status_code: r.status, ...data });
        return;
      }
      fillOptions(listId, data.items || []);
    }

    function setPartitionColumnOptions(selectNode, items, preferredValue, placeholderText) {
      if (!selectNode) return;
      const preferred = String(preferredValue || "").trim();
      const normalized = Array.from(
        new Set(
          (Array.isArray(items) ? items : [])
            .map((x) => String(x || "").trim())
            .filter(Boolean)
        )
      ).sort((a, b) => a.localeCompare(b));
      if (preferred && !normalized.includes(preferred)) {
        normalized.unshift(preferred);
      }

      selectNode.innerHTML = "";
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = placeholderText || "Select source column";
      selectNode.appendChild(placeholder);
      for (const name of normalized) {
        const opt = document.createElement("option");
        opt.value = name;
        opt.textContent = name;
        selectNode.appendChild(opt);
      }
      if (preferred && normalized.includes(preferred)) {
        selectNode.value = preferred;
      } else {
        selectNode.value = "";
      }
    }

    function normalizeRelationIdentifier(raw) {
      let value = String(raw || "").trim();
      if (!value) return "";
      if (value.includes(".")) {
        const parts = value.split(".");
        value = String(parts[parts.length - 1] || "").trim();
      }
      return value.replace(/^["']+|["']+$/g, "").trim();
    }

    async function loadPartitionColumnOptions(card) {
      const selectNode = card && card.querySelector(".partitioning-column");
      if (!selectNode) return;

      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim();
      const sourceType = String(card.querySelector(".source-type")?.value || "table").trim();
      const connId = String(el("source_conn_id")?.value || "").trim();
      const schema = String(card.querySelector(".source-schema")?.value || "").trim();
      const table = normalizeRelationIdentifier(card.querySelector(".source-table")?.value || "");
      const requestKey = `${sourceType}|${connId}|${schema}|${table}`.toLowerCase();
      card.dataset.partitionColumnRequestKey = requestKey;

      const pending = String(card.dataset.pendingPartitionColumn || "").trim();
      const preferred = pending || String(selectNode.value || "").trim();

      if (taskType !== TASK_TYPES.SOURCE_TARGET) {
        setPartitionColumnOptions(selectNode, [], "", "Column selection is available only for Source Target tasks.");
        syncPartitionState(card);
        return;
      }
      if (sourceType !== "table" && sourceType !== "view") {
        setPartitionColumnOptions(selectNode, [], "", "Column selection is available only for table source.");
        syncPartitionState(card);
        return;
      }
      if (!connId || !schema || !table) {
        setPartitionColumnOptions(selectNode, [], "", "Select source schema and table first.");
        syncPartitionState(card);
        return;
      }

      try {
        const path = `/api/columns?conn_id=${encodeURIComponent(connId)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`;
        const resp = await studioFetch(path);
        const data = await parseJsonSafe(resp);
        const stillCurrent = card.dataset.partitionColumnRequestKey === requestKey;
        if (!stillCurrent) return;
        if (!resp.ok || !data.ok) {
          setPartitionColumnOptions(selectNode, [], "", "Columns could not be loaded.");
          syncPartitionState(card);
          return;
        }

        const names = (Array.isArray(data.items) ? data.items : [])
          .map((item) => String(item && item.name ? item.name : "").trim())
          .filter(Boolean);
        setPartitionColumnOptions(selectNode, names, preferred, "Select source column");
        delete card.dataset.pendingPartitionColumn;
        syncPartitionState(card);
      } catch (_err) {
        if (card.dataset.partitionColumnRequestKey !== requestKey) return;
        setPartitionColumnOptions(selectNode, [], "", "Columns could not be loaded.");
        syncPartitionState(card);
      }
    }

    function refreshAllPartitionColumnOptions() {
      for (const card of getTaskCards()) {
        loadPartitionColumnOptions(card);
      }
    }

    const pickerTemp = {
      projects: new Set(),
      domains: new Map(),
      levels: new Map(),
      flows: new Map(),
    };

    const pickerDraft = { project: "", domain: "", level: "", flow: "" };

    function setMapItem(map, key, value) {
      if (!map.has(key)) map.set(key, new Set());
      map.get(key).add(value);
    }

    function clearDraftBelow(levelName) {
      if (levelName === "project") {
        pickerDraft.domain = "";
        pickerDraft.level = "";
        pickerDraft.flow = "";
      } else if (levelName === "domain") {
        pickerDraft.level = "";
        pickerDraft.flow = "";
      } else if (levelName === "level") {
        pickerDraft.flow = "";
      }
      syncFolderApplyState();
    }

    function getFolderPathText(values) {
      const parts = [
        (values.project || "").trim(),
        (values.domain || "").trim(),
        (values.level || "").trim(),
        (values.flow || "").trim(),
      ].filter(Boolean);
      return parts.length ? parts.join("/") : "-";
    }

    function syncFolderPathDisplay() {
      const folderPathValue = getFolderPathText({
        project: el("project").value,
        domain: el("domain").value,
        level: el("level").value,
        flow: el("flow").value,
      });
      const folderPathInput = el("folder_path_display");
      folderPathInput.value = folderPathValue;
      folderPathInput.title = folderPathValue === "-" ? "Flow path (project/domain/level/flow)" : folderPathValue;
      for (const card of getTaskCards()) {
        syncMappingState(card);
      }
    }

    function renderPickerList(containerId, items, selected, onSelect) {
      const box = el(containerId);
      box.innerHTML = "";
      if (!items.length) {
        const empty = document.createElement("div");
        empty.className = "picker-empty";
        empty.textContent = "No folder";
        box.appendChild(empty);
        return;
      }
      for (const item of items) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "picker-item" + (item === selected ? " active" : "");
        btn.textContent = item;
        btn.onclick = () => onSelect(item);
        box.appendChild(btn);
      }
    }

    function appendUniqueSorted(baseItems, extraSet) {
      const all = new Set(baseItems || []);
      for (const x of extraSet || []) all.add(x);
      return Array.from(all).sort((a, b) => a.localeCompare(b));
    }

    function updatePickerSummary() {
      el("folder_picker_summary").textContent = getFolderPathText(pickerDraft);
    }

    function isFolderSelectionComplete() {
      return Boolean(
        (pickerDraft.project || "").trim()
        && (pickerDraft.domain || "").trim()
        && (pickerDraft.level || "").trim()
        && (pickerDraft.flow || "").trim()
      );
    }

    function syncFolderApplyState() {
      const applyBtn = el("btn_apply_folder_picker");
      const enabled = isFolderSelectionComplete();
      applyBtn.disabled = !enabled;
      applyBtn.setAttribute("aria-disabled", enabled ? "false" : "true");
    }

    async function fetchFolderOptions(project, domain, level) {
      const params = new URLSearchParams();
      params.set("source", "dag");
      if (project) params.set("project", project);
      if (domain) params.set("domain", domain);
      if (level) params.set("level", level);
      const r = await studioFetch(`/api/folder-options?${params.toString()}`);
      const data = await r.json();
      if (!r.ok || !data.ok) {
        logDebug("folder-options failed", { status_code: r.status, ...data });
        throw new Error(data.detail || "folder-options failed");
      }
      return data;
    }

    async function refreshPickerColumns() {
      const rootData = await fetchFolderOptions("", "", "");
      const projectItems = appendUniqueSorted(rootData.projects || [], pickerTemp.projects);

      let domainData = { domains: [] };
      let levelData = { levels: [] };
      let flowData = { flows: [] };

      if (pickerDraft.project) {
        domainData = await fetchFolderOptions(pickerDraft.project, "", "");
      }
      if (pickerDraft.project && pickerDraft.domain) {
        levelData = await fetchFolderOptions(pickerDraft.project, pickerDraft.domain, "");
      }
      if (pickerDraft.project && pickerDraft.domain && pickerDraft.level) {
        flowData = await fetchFolderOptions(pickerDraft.project, pickerDraft.domain, pickerDraft.level);
      }

      const domainsExtra = pickerTemp.domains.get(pickerDraft.project || "") || new Set();
      const levelsExtra = pickerTemp.levels.get(
        `${pickerDraft.project || ""}/${pickerDraft.domain || ""}`
      ) || new Set();
      const flowsExtra = pickerTemp.flows.get(
        `${pickerDraft.project || ""}/${pickerDraft.domain || ""}/${pickerDraft.level || ""}`
      ) || new Set();

      const domainItems = appendUniqueSorted(domainData.domains || [], domainsExtra);
      const levelItems = appendUniqueSorted(levelData.levels || [], levelsExtra);
      const flowItems = appendUniqueSorted(flowData.flows || [], flowsExtra);

      renderPickerList("picker_project_list", projectItems, pickerDraft.project, (val) => {
        pickerDraft.project = val;
        clearDraftBelow("project");
        refreshPickerColumns();
      });
      renderPickerList("picker_domain_list", domainItems, pickerDraft.domain, (val) => {
        pickerDraft.domain = val;
        clearDraftBelow("domain");
        refreshPickerColumns();
      });
      renderPickerList("picker_level_list", levelItems, pickerDraft.level, (val) => {
        pickerDraft.level = val;
        clearDraftBelow("level");
        refreshPickerColumns();
      });
      renderPickerList("picker_flow_list", flowItems, pickerDraft.flow, (val) => {
        pickerDraft.flow = val;
        refreshPickerColumns();
      });

      updatePickerSummary();
      syncFolderApplyState();
    }

    function setFolderPickerOpen(isOpen) {
      const modal = el("folder_picker_modal");
      modal.classList.toggle("open", isOpen);
      modal.setAttribute("aria-hidden", isOpen ? "false" : "true");
      document.body.classList.toggle("folder-picker-open", isOpen);
      syncFolderApplyState();
    }

    function openFolderPicker() {
      pickerDraft.project = el("project").value.trim();
      pickerDraft.domain = el("domain").value.trim();
      pickerDraft.level = el("level").value.trim();
      pickerDraft.flow = el("flow").value.trim();
      setFolderPickerOpen(true);
      refreshPickerColumns();
    }

    function closeFolderPicker() {
      setFolderPickerOpen(false);
    }

    function addDraftFolder(levelName) {
      const inputId = `new_${levelName}_name`;
      const raw = el(inputId).value.trim();
      if (!raw) return;

      if (levelName === "project") {
        pickerTemp.projects.add(raw);
        pickerDraft.project = raw;
        clearDraftBelow("project");
      } else if (levelName === "domain") {
        if (!pickerDraft.project) {
          setUpdateModeStatus("Select project first.", "warn");
          pushToast("Select project first.", "error", true);
          return;
        }
        setMapItem(pickerTemp.domains, pickerDraft.project, raw);
        pickerDraft.domain = raw;
        clearDraftBelow("domain");
      } else if (levelName === "level") {
        if (!pickerDraft.project || !pickerDraft.domain) {
          setUpdateModeStatus("Select project and domain first.", "warn");
          pushToast("Select project and domain first.", "error", true);
          return;
        }
        setMapItem(pickerTemp.levels, `${pickerDraft.project}/${pickerDraft.domain}`, raw);
        pickerDraft.level = raw;
        clearDraftBelow("level");
      } else if (levelName === "flow") {
        if (!pickerDraft.project || !pickerDraft.domain || !pickerDraft.level) {
          setUpdateModeStatus("Select project, domain, and level first.", "warn");
          pushToast("Select project, domain, and level first.", "error", true);
          return;
        }
        setMapItem(
          pickerTemp.flows,
          `${pickerDraft.project}/${pickerDraft.domain}/${pickerDraft.level}`,
          raw
        );
        pickerDraft.flow = raw;
      }

      el(inputId).value = "";
      refreshPickerColumns();
    }

    function applyFolderPickerSelection() {
      if (!isFolderSelectionComplete()) return;
      el("project").value = pickerDraft.project || "";
      el("domain").value = pickerDraft.domain || "";
      el("level").value = pickerDraft.level || "";
      el("flow").value = pickerDraft.flow || "";
      syncFolderPathDisplay();
      for (const card of getTaskCards()) syncMappingState(card);
      loadDagDependencyOptions(currentUpdateDagId).catch((_err) => {});
      closeFolderPicker();
    }

    async function loadFolderOptions() {
      syncFolderPathDisplay();
      try {
        await fetchFolderOptions("", "", "");
      } catch (_err) {
        // no-op: output already shown
      }
    }

    async function loadConnections() {
      try {
        let items = [];
        const studioResp = await studioFetch("/api/connections");
        if (studioResp.ok) {
          const studioData = await parseJsonSafe(studioResp);
          items = Array.isArray(studioData.items) ? studioData.items : [];
        } else {
          // Backward compatibility for running containers that do not yet expose /flow-studio/api/connections.
          const airflowResp = await fetch("/api/v2/connections?limit=1000&offset=0&order_by=connection_id");
          const airflowData = await parseJsonSafe(airflowResp);
          if (!airflowResp.ok) {
            const detail = airflowData.detail || "Connection list could not be loaded.";
            logDebug("airflow fallback connection list failed", { status_code: airflowResp.status, detail });
            fillConnectionSelect("source_conn_id", [], "");
            fillConnectionSelect("target_conn_id", [], "");
            return;
          }
          const rows = Array.isArray(airflowData.connections) ? airflowData.connections : [];
          items = rows.map((row) => ({
            conn_id: row.connection_id || "",
            conn_type: row.connection_type || "",
          }));
        }

        fillConnectionSelect("source_conn_id", items, "ffengine_source");
        fillConnectionSelect("target_conn_id", items, "ffengine_target");
        refreshTaskCardHeaders();
        refreshAllPartitionColumnOptions();
      } catch (err) {
        logDebug("connection list load failed", { ok: false, detail: `Connection list could not be loaded: ${String(err && err.message ? err.message : err)}` });
        fillConnectionSelect("source_conn_id", [], "");
        fillConnectionSelect("target_conn_id", [], "");
        refreshTaskCardHeaders();
        refreshAllPartitionColumnOptions();
      }
    }

    function getTaskCards() {
      return Array.from(document.querySelectorAll("#tasks_container .task-card"));
    }

    function syncTaskTypeSegment(card) {
      const currentType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim() || TASK_TYPES.SOURCE_TARGET;
      const chips = card.querySelectorAll(".task-type-chip");
      for (const chip of chips) {
        const chipType = String(chip.getAttribute("data-task-type") || "").trim();
        const isActive = chipType === currentType;
        chip.classList.toggle("active", isActive);
        chip.setAttribute("aria-pressed", isActive ? "true" : "false");
      }
    }

    function bindTaskTypeSegment(card) {
      const typeSelect = card.querySelector(".task-type");
      const chips = card.querySelectorAll(".task-type-chip");
      const segment = card.querySelector(".task-type-segment");
      if (!typeSelect || !chips.length) return;
      if (segment) {
        segment.addEventListener("pointerdown", (ev) => ev.stopPropagation());
        segment.addEventListener("mousedown", (ev) => ev.stopPropagation());
        segment.addEventListener("click", (ev) => ev.stopPropagation());
      }
      for (const chip of chips) {
        chip.addEventListener("click", (ev) => {
          ev.preventDefault();
          ev.stopPropagation();
          const nextType = String(chip.getAttribute("data-task-type") || "").trim();
          if (!nextType || nextType === typeSelect.value) return;
          typeSelect.value = nextType;
          typeSelect.dispatchEvent(new Event("change", { bubbles: true }));
        });
      }
    }

    function setTaskCardCollapsed(card, collapsed) {
      const head = card.querySelector(".task-head");
      const toggle = card.querySelector(".task-collapse-toggle");
      card.classList.toggle("collapsed", !!collapsed);
      if (head) {
        head.setAttribute("aria-expanded", collapsed ? "false" : "true");
      }
      if (toggle) {
        toggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
      }
    }

    function toggleTaskCardCollapsed(card) {
      const isCollapsed = card.classList.contains("collapsed");
      setTaskCardCollapsed(card, !isCollapsed);
    }

    function setAllTaskCardsCollapsed(collapsed) {
      const cards = getTaskCards();
      for (const card of cards) {
        setTaskCardCollapsed(card, collapsed);
      }
    }

    function bindTaskCollapse(card) {
      const toggle = card.querySelector(".task-collapse-toggle");
      if (!toggle) return;
      toggle.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        toggleTaskCardCollapsed(card);
      });
    }

    function refreshTaskCardHeaders() {
      const cards = getTaskCards();
      const oldTaskIds = cards.map((card, idx) => {
        const cached = String(card.dataset.currentTaskGroupId || "").trim();
        if (cached) return cached;
        return resolveTaskIdentity(card, idx + 1).task_group_id;
      });
      const newTaskIds = [];
      for (let i = 0; i < cards.length; i += 1) {
        cards[i].querySelector(".task-title").textContent = `Task #${i + 1}`;
        cards[i].querySelector(".btn-delete-task").disabled = cards.length <= 1;
        syncTaskTypeState(cards[i]);
        const identity = syncTaskGroupState(cards[i], i + 1);
        cards[i].dataset.currentTaskGroupId = identity.task_group_id;
        newTaskIds.push(identity.task_group_id);
        syncMappingState(cards[i]);
      }
      remapDependenciesAfterTaskIdChange(cards, oldTaskIds, newTaskIds);
      for (let i = 0; i < cards.length; i += 1) {
        syncDependencyState(cards[i], i, newTaskIds);
      }
    }

    function getBindingRows(card) {
      return Array.from(card.querySelectorAll(".binding-item"));
    }

    function updateBindingsVisibility(card) {
      const list = card.querySelector(".bindings-list");
      const hasRows = getBindingRows(card).length > 0;
      list.classList.toggle("hidden", !hasRows);
    }

    function syncBindingRowState(row) {
      const sourceSelect = row.querySelector(".binding-source");
      const defaultInput = row.querySelector(".binding-default-value");
      const sqlWrap = row.querySelector(".binding-sql-wrap");
      const sqlInput = row.querySelector(".binding-sql");
      const airflowWrap = row.querySelector(".binding-airflow-wrap");
      const airflowInput = row.querySelector(".binding-airflow-variable-key");
      const source = sourceSelect.value;

      const isDefault = source === "default";
      const isSqlSource = source === "source" || source === "target";
      const isAirflowVariable = source === "airflow_variable";

      defaultInput.disabled = !isDefault;
      sqlInput.disabled = !isSqlSource;
      airflowInput.disabled = !isAirflowVariable;
      sqlWrap.classList.toggle("hidden", !isSqlSource);
      airflowWrap.classList.toggle("hidden", !isAirflowVariable);

      if (!isDefault) defaultInput.value = "";
      if (!isSqlSource) sqlInput.value = "";
      if (!isAirflowVariable) airflowInput.value = "";
    }

    function createBindingRow(card, values = {}) {
      const list = card.querySelector(".bindings-list");
      const row = document.createElement("div");
      row.className = "binding-item";
      row.innerHTML = `
        <div class="binding-row">
          <input class="binding-variable-name" placeholder="variable_name">
          <select class="binding-source">
            <option value="target">Target</option>
            <option value="source">Source</option>
            <option value="default">Default</option>
            <option value="airflow_variable">Airflow Variable</option>
          </select>
          <input class="binding-default-value" placeholder="Default">
          <button class="btn btn-danger binding-remove" type="button">x</button>
        </div>
        <label class="binding-sql-wrap hidden">
          SQL
          <textarea class="binding-sql" rows="3" placeholder="SELECT ..."></textarea>
        </label>
        <label class="binding-airflow-wrap hidden">
          Airflow Variable
          <input class="binding-airflow-variable-key" list="airflow_variable_options" placeholder="Select variable key">
        </label>
      `;
      row.querySelector(".binding-variable-name").value = values.variable_name || "";
      row.querySelector(".binding-source").value = values.binding_source || "target";
      row.querySelector(".binding-default-value").value = values.default_value || "";
      row.querySelector(".binding-sql").value = values.sql || "";
      row.querySelector(".binding-airflow-variable-key").value = values.airflow_variable_key || "";

      row.querySelector(".binding-source").addEventListener("change", () => {
        syncBindingRowState(row);
      });
      row.querySelector(".binding-remove").addEventListener("click", () => {
        row.remove();
        updateBindingsVisibility(card);
      });
      row.querySelector(".binding-airflow-variable-key").addEventListener("input", (ev) => {
        const q = (ev.target.value || "").trim();
        if (q.length >= 2) {
          loadAirflowVariables(q);
        }
      });

      syncBindingRowState(row);
      list.appendChild(row);
      updateBindingsVisibility(card);
    }

    function setBindingsFromValues(card, bindings) {
      const list = card.querySelector(".bindings-list");
      list.innerHTML = "";
      const items = Array.isArray(bindings) ? bindings : [];
      for (const binding of items) {
        createBindingRow(card, binding || {});
      }
      updateBindingsVisibility(card);
    }

    function bindBindingsSection(card) {
      const addButton = card.querySelector(".btn-binding-add");
      addButton.addEventListener("click", () => createBindingRow(card, {}));
      updateBindingsVisibility(card);
    }

    function syncTaskTypeState(card) {
      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim() || TASK_TYPES.SOURCE_TARGET;
      syncTaskTypeSegment(card);
      card.classList.toggle("single-pane-task", taskType !== TASK_TYPES.SOURCE_TARGET);
      const sourceTargetFields = card.querySelector(".source-target-fields");
      const scriptRunFields = card.querySelector(".script-run-fields");
      const dagTaskFields = card.querySelector(".dag-task-fields");
      const targetCard = card.querySelector(".target-card");
      const whereClauseWrap = card.querySelector(".where-clause-wrap");
      const whereInput = card.querySelector(".where");

      sourceTargetFields?.classList.toggle("hidden", taskType !== TASK_TYPES.SOURCE_TARGET);
      scriptRunFields?.classList.toggle("hidden", taskType !== TASK_TYPES.SCRIPT_RUN);
      dagTaskFields?.classList.toggle("hidden", taskType !== TASK_TYPES.DAG);
      targetCard?.classList.toggle("hidden", taskType !== TASK_TYPES.SOURCE_TARGET);
      whereClauseWrap?.classList.toggle("hidden", taskType !== TASK_TYPES.SOURCE_TARGET);
      if (whereInput) whereInput.disabled = taskType !== TASK_TYPES.SOURCE_TARGET;

      const modeSelect = card.querySelector(".dependency-mode");
      const tabButtons = Array.from(card.querySelectorAll(".tab-btn"));
      const panels = Array.from(card.querySelectorAll(".tab-panel"));
      const allowAllTabs = taskType === TASK_TYPES.SOURCE_TARGET;
      const allowScriptFilterTab = taskType === TASK_TYPES.SCRIPT_RUN;
      for (const btn of tabButtons) {
        const tabId = String(btn.getAttribute("data-tab") || "");
        const keep = allowAllTabs || tabId === "dependencies" || (allowScriptFilterTab && tabId === "filter");
        btn.classList.toggle("hidden", !keep);
      }
      for (const panel of panels) {
        const panelId = String(panel.getAttribute("data-tab-panel") || "");
        const keep = allowAllTabs || panelId === "dependencies" || (allowScriptFilterTab && panelId === "filter");
        panel.classList.toggle("hidden", !keep);
      }

      if (!allowAllTabs) {
        const fallbackTab = allowScriptFilterTab ? "filter" : "dependencies";
        const depBtn = tabButtons.find((btn) => String(btn.getAttribute("data-tab") || "") === fallbackTab);
        const depPanel = panels.find((panel) => String(panel.getAttribute("data-tab-panel") || "") === fallbackTab);
        for (const btn of tabButtons) btn.classList.remove("active");
        for (const panel of panels) panel.classList.remove("active");
        if (depBtn) depBtn.classList.add("active");
        if (depPanel) depPanel.classList.add("active");
      } else if (!tabButtons.some((btn) => btn.classList.contains("active"))) {
        const firstVisible = tabButtons.find((btn) => !btn.classList.contains("hidden"));
        if (firstVisible) {
          firstVisible.classList.add("active");
          const target = firstVisible.getAttribute("data-tab");
          const panel = card.querySelector(`.tab-panel[data-tab-panel="${target}"]`);
          if (panel) panel.classList.add("active");
        }
      }

      const sourceTypeSelect = card.querySelector(".source-type");
      const mappingModeSelect = card.querySelector(".column-mapping-mode");
      const scriptEnvSelect = card.querySelector(".script-run-environment");
      const scriptSqlInput = card.querySelector(".script-sql");
      const dagTaskSelect = card.querySelector(".dag-task-dag-id");

      if (sourceTypeSelect) sourceTypeSelect.disabled = taskType !== TASK_TYPES.SOURCE_TARGET;
      if (mappingModeSelect) mappingModeSelect.disabled = taskType !== TASK_TYPES.SOURCE_TARGET || sourceTypeSelect?.value === "sql";
      if (scriptEnvSelect) scriptEnvSelect.disabled = taskType !== TASK_TYPES.SCRIPT_RUN;
      if (scriptSqlInput) scriptSqlInput.disabled = taskType !== TASK_TYPES.SCRIPT_RUN;
      if (dagTaskSelect) dagTaskSelect.disabled = taskType !== TASK_TYPES.DAG;

      if (taskType === TASK_TYPES.DAG) {
        refreshDagTaskOptions(card);
      }
      if (modeSelect && taskType !== TASK_TYPES.SOURCE_TARGET) {
        modeSelect.disabled = false;
      }
    }

    function setTaskCardValues(card, values, fallbackIndex = 1) {
      const taskType = values.task_type || TASK_TYPES.SOURCE_TARGET;
      const sourceType = values.source_type || "table";
      const partitioningMode = values.partitioning_mode || "auto_numeric";
      const loadedTaskGroupId = String(values.task_group_id || "").trim();
      const initialDependsOn = normalizeDependsOnList(values.depends_on || []);
      if (loadedTaskGroupId) {
        card.dataset.loadedTaskGroupId = loadedTaskGroupId;
      } else {
        delete card.dataset.loadedTaskGroupId;
      }
      card.dataset.initialDependsOn = JSON.stringify(initialDependsOn);
      card.dataset.dependenciesInitialized = "0";
      card.dataset.currentTaskGroupId = loadedTaskGroupId || "";
      setCardDependencyMode(card, DEPENDENCY_MODES.PARALLEL);
      setCardCustomDependsOn(card, []);
      card.dataset.loadedSignature = "";
      card.querySelector(".task-type").value = taskType;
      card.querySelector(".source-schema").value = values.source_schema || "";
      card.querySelector(".source-table").value = values.source_table || "";
      card.querySelector(".source-type").value = sourceType === "view" ? "table" : sourceType;
      card.querySelector(".source-inline-sql").value = values.inline_sql || "";
      card.querySelector(".script-run-environment").value = values.script_run_environment || "source";
      card.querySelector(".script-sql").value = values.script_sql || "";
      card.querySelector(".dag-task-dag-id").value = values.dag_task_dag_id || "";
      card.dataset.pendingDagTaskDagId = String(values.dag_task_dag_id || "").trim();
      card.querySelector(".target-schema").value = values.target_schema || "";
      card.querySelector(".target-table").value = values.target_table || "";
      card.querySelector(".load-method").value = values.load_method || "create_if_not_exists_or_truncate";
      card.querySelector(".column-mapping-mode").value = values.column_mapping_mode || "source";
      card.querySelector(".mapping-content").value = values.mapping_content || "";
      card.querySelector(".where").value = values.where || "";
      card.querySelector(".batch-size").value = String(values.batch_size || 10000);
      card.querySelector(".partitioning-enabled").checked = !!values.partitioning_enabled;
      const partitioningModeSelect = card.querySelector(".partitioning-mode");
      partitioningModeSelect.value = partitioningMode;
      if (partitioningModeSelect.value !== partitioningMode) {
        partitioningModeSelect.value = "auto_numeric";
      }
      const partitionColumn = String(values.partitioning_column || "").trim();
      const partitionColumnSelect = card.querySelector(".partitioning-column");
      partitionColumnSelect.value = partitionColumn;
      if (partitionColumn) {
        card.dataset.pendingPartitionColumn = partitionColumn;
      } else {
        delete card.dataset.pendingPartitionColumn;
      }
      loadPartitionColumnOptions(card);
      refreshDagTaskOptions(card);
      card.querySelector(".partitioning-parts").value = String(values.partitioning_parts || 2);
      card.querySelector(".partitioning-distinct-limit").value = String(
        asPositiveInt(values.partitioning_distinct_limit, 16)
      );
      card.querySelector(".partitioning-ranges").value = rangesToMultilineText(values.partitioning_ranges || []);
      setBindingsFromValues(card, values.bindings || []);
      syncTaskTypeState(card);
      toggleSourceMode(card);
      if (loadedTaskGroupId) {
        card.dataset.loadedSignature = buildTaskGroupFormula(card, fallbackIndex);
      }
      syncPartitionState(card);
      syncTaskGroupState(card, fallbackIndex);
      syncMappingState(card);
    }

    function toggleSourceMode(card) {
      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim();
      if (taskType !== TASK_TYPES.SOURCE_TARGET) return;
      const sourceType = card.querySelector(".source-type").value;
      const sqlWrap = card.querySelector(".source-sql-wrap");
      const sqlText = card.querySelector(".source-inline-sql");
      const sourceTableWrap = card.querySelector(".source-table-wrap");
      const sourceSchemaInput = card.querySelector(".source-schema");
      const sourceTableInput = card.querySelector(".source-table");
      const isSqlMode = sourceType === "sql";
      sqlWrap.classList.toggle("hidden", !isSqlMode);
      sourceTableWrap.classList.toggle("hidden", isSqlMode);
      if (!isSqlMode) {
        sqlText.value = "";
      } else {
        sourceSchemaInput.value = "";
        sourceTableInput.value = "";
      }
      sourceTableInput.placeholder = "Search table";
    }

    function buildTaskGroupFormula(card, fallbackIndex) {
      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim() || TASK_TYPES.SOURCE_TARGET;
      const sourceType = card.querySelector(".source-type").value;
      const sourceDbVal = (el("source_conn_id").value || "").trim();
      const targetDbVal = (el("target_conn_id").value || "").trim();
      const sourceSchemaVal = card.querySelector(".source-schema").value.trim();
      const sourceTableVal = card.querySelector(".source-table").value.trim();
      const loadMethodVal = card.querySelector(".load-method").value.trim();
      const targetSchemaVal = card.querySelector(".target-schema").value.trim();
      const targetTableVal = card.querySelector(".target-table").value.trim();
      const scriptEnvVal = (card.querySelector(".script-run-environment")?.value || "source").trim();
      const dagTaskDagId = (card.querySelector(".dag-task-dag-id")?.value || "").trim();
      const taskGroupSourceSchema = taskType === TASK_TYPES.SCRIPT_RUN
        ? "script"
        : (taskType === TASK_TYPES.DAG ? "dag" : (sourceType === "sql" ? "sql" : sourceSchemaVal));
      const taskGroupSourceTable = taskType === TASK_TYPES.SCRIPT_RUN
        ? (scriptEnvVal || "source")
        : (taskType === TASK_TYPES.DAG ? (dagTaskDagId || "dag") : (sourceType === "sql" ? "query" : sourceTableVal));
      const taskGroupLoadMethod = taskType === TASK_TYPES.SCRIPT_RUN
        ? "script"
        : (taskType === TASK_TYPES.DAG ? "dag" : loadMethodVal);
      return [
        String(fallbackIndex),
        slugify(sourceDbVal, "source"),
        slugify(taskGroupSourceSchema, "src"),
        slugify(taskGroupSourceTable, "table"),
        "to",
        slugify(targetDbVal, "target"),
        slugify(taskGroupLoadMethod, "method"),
        slugify(targetSchemaVal, "tgt"),
        slugify(targetTableVal, "table"),
      ].join("_");
    }

    function resolveTaskIdentity(card, fallbackIndex) {
      const generatedTaskGroupId = buildTaskGroupFormula(card, fallbackIndex);
      const loadedTaskGroupId = String(card.dataset.loadedTaskGroupId || "").trim();
      const loadedSignature = String(card.dataset.loadedSignature || "").trim();
      let taskGroupId = generatedTaskGroupId;
      if (loadedTaskGroupId && loadedSignature && loadedSignature === generatedTaskGroupId) {
        taskGroupId = loadedTaskGroupId;
      }
      return {
        task_no: fallbackIndex,
        task_group_id: taskGroupId,
      };
    }

    function syncTaskGroupState(card, fallbackIndex) {
      const identity = resolveTaskIdentity(card, fallbackIndex);
      const out = card.querySelector(".task-group-id-readonly");
      if (out) out.textContent = identity.task_group_id;
      return identity;
    }

    function deriveDependencyMode(dependsOn, previousTaskId) {
      const normalized = normalizeDependsOnList(dependsOn);
      if (!normalized.length) return DEPENDENCY_MODES.PARALLEL;
      if (normalized.length === 1 && previousTaskId && normalized[0] === previousTaskId) {
        return DEPENDENCY_MODES.WAIT_PREVIOUS;
      }
      return DEPENDENCY_MODES.CUSTOM;
    }

    function remapDependenciesAfterTaskIdChange(cards, oldTaskIds, newTaskIds) {
      const remap = new Map();
      for (let i = 0; i < oldTaskIds.length; i += 1) {
        const oldId = String(oldTaskIds[i] || "").trim();
        const newId = String(newTaskIds[i] || "").trim();
        if (!oldId || !newId || oldId === newId) continue;
        remap.set(oldId, newId);
      }
      if (!remap.size) return;

      const available = new Set(newTaskIds);
      for (let i = 0; i < cards.length; i += 1) {
        const card = cards[i];
        if (getCardDependencyMode(card) !== DEPENDENCY_MODES.CUSTOM) continue;
        const selfTaskId = String(newTaskIds[i] || "").trim();
        const remapped = normalizeDependsOnList(
          getCardCustomDependsOn(card).map((depId) => remap.get(depId) || depId)
        ).filter((depId) => depId !== selfTaskId && available.has(depId));
        setCardCustomDependsOn(card, remapped);
      }
    }

    function renderDependencyChips(card) {
      const chipsWrap = card.querySelector(".dependency-chips");
      if (!chipsWrap) return;
      const selected = getCardCustomDependsOn(card);
      chipsWrap.innerHTML = "";
      for (const depId of selected) {
        const chip = document.createElement("span");
        chip.className = "dependency-chip";
        chip.textContent = depId;
        const remove = document.createElement("button");
        remove.type = "button";
        remove.className = "dependency-chip-remove";
        remove.textContent = "x";
        remove.title = `Remove upstream: ${depId}`;
        remove.disabled = !!isBusy;
        remove.addEventListener("click", () => {
          setCardCustomDependsOn(
            card,
            getCardCustomDependsOn(card).filter((item) => item !== depId)
          );
          syncDependencyState(card);
        });
        chip.appendChild(remove);
        chipsWrap.appendChild(chip);
      }
    }

    function buildDependencyOptionLabel(taskNo, taskGroupId) {
      return `Task #${taskNo} (${taskGroupId})`;
    }

    function addDependencyToCard(card, rawDepId) {
      const depId = String(rawDepId || "").trim();
      if (!depId) return false;
      const cards = getTaskCards();
      const taskIds = cards.map((item, idx) => resolveTaskIdentity(item, idx + 1).task_group_id);
      const selfIndex = Math.max(0, cards.indexOf(card));
      const selfTaskId = String(taskIds[selfIndex] || "").trim();
      if (!depId || depId === selfTaskId) return false;
      if (!new Set(taskIds).has(depId)) return false;
      const next = normalizeDependsOnList([...getCardCustomDependsOn(card), depId]);
      setCardCustomDependsOn(card, next);
      syncDependencyState(card);
      return true;
    }

    function resolveCardDependsOnForState(card, cardIndex, taskIds) {
      const selfTaskId = String(taskIds[cardIndex] || "").trim();
      const previousTaskId = cardIndex > 0 ? String(taskIds[cardIndex - 1] || "").trim() : "";
      const mode = getCardDependencyMode(card);
      if (mode === DEPENDENCY_MODES.WAIT_PREVIOUS) {
        return previousTaskId ? [previousTaskId] : [];
      }
      if (mode === DEPENDENCY_MODES.CUSTOM) {
        const available = new Set(taskIds);
        const pendingSelected = String(card.querySelector(".dependency-custom-select")?.value || "").trim();
        const merged = pendingSelected
          ? [...getCardCustomDependsOn(card), pendingSelected]
          : getCardCustomDependsOn(card);
        return normalizeDependsOnList(
          merged.filter((depId) => depId !== selfTaskId && available.has(depId))
        );
      }
      return [];
    }

    function hasIncomingDependencyForCard(targetCard) {
      const cards = getTaskCards();
      const targetIndex = cards.indexOf(targetCard);
      if (targetIndex < 0) return false;
      const taskIds = cards.map((card, idx) => resolveTaskIdentity(card, idx + 1).task_group_id);
      const targetTaskId = String(taskIds[targetIndex] || "").trim();
      if (!targetTaskId) return false;
      for (let i = 0; i < cards.length; i += 1) {
        if (i === targetIndex) continue;
        const deps = resolveCardDependsOnForState(cards[i], i, taskIds);
        if (deps.includes(targetTaskId)) {
          return true;
        }
      }
      return false;
    }

    function syncDependencyState(card, indexOverride, taskIdsOverride) {
      const cards = getTaskCards();
      const cardIndex = Number.isInteger(indexOverride) ? indexOverride : Math.max(0, cards.indexOf(card));
      const taskIds = Array.isArray(taskIdsOverride) && taskIdsOverride.length
        ? taskIdsOverride.slice()
        : cards.map((item, idx) => resolveTaskIdentity(item, idx + 1).task_group_id);
      const selfTaskId = String(taskIds[cardIndex] || "").trim();
      const previousTaskId = cardIndex > 0 ? String(taskIds[cardIndex - 1] || "").trim() : "";
      const allTaskIds = new Set(taskIds);
      const customWrap = card.querySelector(".dependency-custom-wrap");
      const customSelect = card.querySelector(".dependency-custom-select");
      const customAddButton = card.querySelector(".btn-add-dependency");
      const summary = card.querySelector(".dependency-summary");
      const modeSelect = card.querySelector(".dependency-mode");

      if (String(card.dataset.dependenciesInitialized || "") !== "1") {
        const initialDependsOn = normalizeDependsOnList(
          parseJsonArray(String(card.dataset.initialDependsOn || "[]"))
        );
        const modeFromInitial = deriveDependencyMode(initialDependsOn, previousTaskId);
        setCardDependencyMode(card, modeFromInitial);
        if (modeFromInitial === DEPENDENCY_MODES.CUSTOM) {
          setCardCustomDependsOn(
            card,
            initialDependsOn.filter((depId) => depId !== selfTaskId && allTaskIds.has(depId))
          );
        } else {
          setCardCustomDependsOn(card, []);
        }
        card.dataset.dependenciesInitialized = "1";
      }

      let mode = getCardDependencyMode(card);
      if (!previousTaskId && mode === DEPENDENCY_MODES.WAIT_PREVIOUS) {
        mode = DEPENDENCY_MODES.PARALLEL;
        setCardDependencyMode(card, mode);
      }

      let customDependsOn = getCardCustomDependsOn(card).filter(
        (depId) => depId !== selfTaskId && allTaskIds.has(depId)
      );
      if (getCardCustomDependsOn(card).length !== customDependsOn.length) {
        setCardCustomDependsOn(card, customDependsOn);
      }

      const optionRows = [];
      for (let i = 0; i < taskIds.length; i += 1) {
        const depId = String(taskIds[i] || "").trim();
        if (!depId || depId === selfTaskId) continue;
        optionRows.push({
          task_no: i + 1,
          task_group_id: depId,
          selected: customDependsOn.includes(depId),
        });
      }

      if (customSelect) {
        customSelect.innerHTML = "";
        const placeholder = document.createElement("option");
        placeholder.value = "";
        placeholder.textContent = optionRows.length ? "Select upstream task" : "No upstream task";
        customSelect.appendChild(placeholder);
        for (const row of optionRows) {
          const opt = document.createElement("option");
          opt.value = row.task_group_id;
          opt.textContent = buildDependencyOptionLabel(row.task_no, row.task_group_id);
          opt.disabled = row.selected;
          customSelect.appendChild(opt);
        }
      }
      if (customAddButton) {
        customAddButton.disabled = !!isBusy || mode !== DEPENDENCY_MODES.CUSTOM || !optionRows.length;
      }
      if (customWrap) {
        customWrap.classList.toggle("hidden", mode !== DEPENDENCY_MODES.CUSTOM);
      }

      renderDependencyChips(card);
      if (summary) {
        if (mode === DEPENDENCY_MODES.PARALLEL) {
          summary.textContent = "Parallel: no upstream dependency.";
        } else if (mode === DEPENDENCY_MODES.WAIT_PREVIOUS) {
          summary.textContent = previousTaskId
            ? `Wait Previous: depends on ${previousTaskId}.`
            : "Wait Previous is unavailable for the first task.";
        } else if (customDependsOn.length) {
          summary.textContent = `Custom: waits for ${customDependsOn.length} upstream task(s).`;
        } else {
          summary.textContent = "Custom: select one or more upstream tasks.";
        }
      }

      if (modeSelect) {
        const waitPreviousOpt = modeSelect.querySelector('option[value="wait_previous"]');
        if (waitPreviousOpt) {
          waitPreviousOpt.disabled = !previousTaskId;
        }
        modeSelect.value = mode;
      }
    }

    function bindDependencyState(card) {
      const modeSelect = card.querySelector(".dependency-mode");
      const customSelect = card.querySelector(".dependency-custom-select");
      const addButton = card.querySelector(".btn-add-dependency");
      if (!modeSelect || !customSelect || !addButton) return;
      modeSelect.addEventListener("change", () => {
        const nextMode = String(modeSelect.value || DEPENDENCY_MODES.PARALLEL).trim();
        setCardDependencyMode(card, nextMode);
        if (nextMode !== DEPENDENCY_MODES.CUSTOM) {
          setCardCustomDependsOn(card, []);
        }
        refreshTaskCardHeaders();
      });
      addButton.addEventListener("click", () => {
        const depId = String(customSelect.value || "").trim();
        if (!depId) return;
        if (addDependencyToCard(card, depId)) {
          customSelect.value = "";
        }
      });
      customSelect.addEventListener("change", () => {
        const depId = String(customSelect.value || "").trim();
        if (!depId) return;
        if (addDependencyToCard(card, depId)) {
          customSelect.value = "";
        }
      });
      customSelect.addEventListener("dblclick", () => {
        const depId = String(customSelect.value || "").trim();
        if (!depId) return;
        if (addDependencyToCard(card, depId)) {
          customSelect.value = "";
        }
      });
    }

    function buildGeneratedMappingRelativePath(card) {
      const cards = getTaskCards();
      const index = Math.max(0, cards.indexOf(card));
      const taskNo = index + 1;
      const identity = resolveTaskIdentity(card, taskNo);
      return `mapping/${identity.task_no}_${identity.task_group_id}.yaml`;
    }

    function buildGeneratedMappingDisplayPath(card) {
      const mappingFile = buildGeneratedMappingRelativePath(card);
      const project = (el("project").value || "").trim();
      const domain = (el("domain").value || "").trim();
      const level = (el("level").value || "").trim();
      const flow = (el("flow").value || "").trim();
      return [project, domain, level, flow, mappingFile].filter(Boolean).join("/");
    }

    function setMappingStatus(card, message, isError = false) {
      const box = card.querySelector(".mapping-status");
      if (!box) return;
      box.textContent = message || "";
      box.classList.toggle("warn", !!isError);
      box.classList.toggle("ok", !isError && !!message);
    }

    function syncMappingState(card) {
      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim() || TASK_TYPES.SOURCE_TARGET;
      const sourceType = card.querySelector(".source-type").value;
      const modeSelect = card.querySelector(".column-mapping-mode");
      const mappingGeneratedPathWrap = card.querySelector(".mapping-generated-path-wrap");
      const mappingContentWrap = card.querySelector(".mapping-content-wrap");
      const mappingActions = card.querySelector(".mapping-actions");
      const generatedPathInput = card.querySelector(".mapping-generated-path");
      if (taskType !== TASK_TYPES.SOURCE_TARGET) {
        modeSelect.disabled = true;
        modeSelect.setAttribute("aria-disabled", "true");
        mappingGeneratedPathWrap.classList.add("hidden");
        mappingContentWrap.classList.add("hidden");
        mappingActions.classList.add("hidden");
        generatedPathInput.value = "";
        setMappingStatus(card, "", false);
        return;
      }
      const isSql = sourceType === "sql";
      if (isSql && modeSelect.value !== "mapping_file") {
        modeSelect.value = "mapping_file";
      }
      modeSelect.disabled = isSql;
      modeSelect.setAttribute("aria-disabled", isSql ? "true" : "false");

      const isMappingFileMode = modeSelect.value === "mapping_file";
      mappingGeneratedPathWrap.classList.toggle("hidden", !isMappingFileMode);
      mappingContentWrap.classList.toggle("hidden", !isMappingFileMode);
      mappingActions.classList.toggle("hidden", !isMappingFileMode);

      generatedPathInput.value = isMappingFileMode ? buildGeneratedMappingDisplayPath(card) : "";
      if (isSql) {
        setMappingStatus(card, "mapping_file mode is required for SQL source.", false);
      } else if (!isMappingFileMode) {
        setMappingStatus(card, "", false);
      }
    }

    async function generateMappingForCard(card) {
      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim();
      if (taskType !== TASK_TYPES.SOURCE_TARGET) {
        setMappingStatus(card, "Mapping is available only for Source Target tasks.", true);
        return;
      }
      const sourceType = card.querySelector(".source-type").value;
      const taskNo = Math.max(1, getTaskCards().indexOf(card) + 1);
      const taskIdentity = resolveTaskIdentity(card, taskNo);
      const payload = {
        project: (el("project").value || "").trim() || "webhook",
        domain: (el("domain").value || "").trim() || "default_domain",
        level: (el("level").value || "").trim() || "level1",
        flow: (el("flow").value || "").trim() || "src_to_stg",
        source_conn_id: (el("source_conn_id").value || "").trim(),
        target_conn_id: (el("target_conn_id").value || "").trim(),
        source_type: sourceType,
        task_group_id: taskIdentity.task_group_id,
        task_no: taskNo,
      };
      if (sourceType === "sql") {
        payload.inline_sql = (card.querySelector(".source-inline-sql").value || "").trim();
      } else {
        payload.source_schema = (card.querySelector(".source-schema").value || "").trim();
        payload.source_table = (card.querySelector(".source-table").value || "").trim();
      }
      setMappingStatus(card, "Mapping uretiliyor...", false);
      try {
        const data = await postJson(studioUrl("/api/mapping/generate"), payload);
        if (!data || !data.ok) {
          setMappingStatus(card && data ? apiErrorMessage(data, "Mapping uretilemedi.") : "Mapping uretilemedi.", true);
          return;
        }
        if (data.generated_mapping_file) {
          const project = (el("project").value || "").trim();
          const domain = (el("domain").value || "").trim();
          const level = (el("level").value || "").trim();
          const flow = (el("flow").value || "").trim();
          card.querySelector(".mapping-generated-path").value = [
            project,
            domain,
            level,
            flow,
            data.generated_mapping_file,
          ].filter(Boolean).join("/");
        }
        card.querySelector(".mapping-content").value = data.mapping_content || "";
        const warnings = Array.isArray(data.warnings) ? data.warnings : [];
        if (warnings.length) {
          setMappingStatus(card, `Mapping generated (warning: ${warnings.length}).`, false);
        } else {
          setMappingStatus(card, "Mapping uretildi.", false);
        }
        syncMappingState(card);
      } catch (_err) {
        setMappingStatus(card, "Error occurred while generating mapping.", true);
      }
    }

    function syncPartitionState(card) {
      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim() || TASK_TYPES.SOURCE_TARGET;
      const enabledInput = card.querySelector(".partitioning-enabled");
      const modeSelect = card.querySelector(".partitioning-mode");
      const modeRowWrap = card.querySelector(".partitioning-mode-wrap");
      const columnWrap = card.querySelector(".partitioning-column-wrap");
      const modeWrap = card.querySelector(".partitioning-mode-field-wrap");
      const modeHint = card.querySelector(".partitioning-mode-hint");
      const secondaryWrap = card.querySelector(".partitioning-secondary-wrap");
      const partsWrap = card.querySelector(".partitioning-parts-wrap");
      const distinctLimitWrap = card.querySelector(".partitioning-distinct-limit-wrap");
      const explicitWrap = card.querySelector(".partitioning-explicit-wrap");
      const columnInput = card.querySelector(".partitioning-column");
      const partsInput = card.querySelector(".partitioning-parts");
      const distinctLimitInput = card.querySelector(".partitioning-distinct-limit");
      const explicitInput = card.querySelector(".partitioning-ranges");

      const enabled = !!enabledInput.checked;
      if (taskType !== TASK_TYPES.SOURCE_TARGET) {
        enabledInput.checked = false;
      }
      const mode = String(modeSelect.value || "auto_numeric").trim() || "auto_numeric";
      const isExplicit = mode === "explicit";
      const isDistinct = mode === "distinct";
      const modeHintText = PARTITION_MODE_HINTS[mode] || "";
      const columnRequired = PARTITION_COLUMN_REQUIRED_MODES.has(mode);
      const partsRequired = PARTITION_PARTS_REQUIRED_MODES.has(mode);

      const setDisabled = (node, disabled) => {
        if (!node) return;
        node.disabled = !!disabled;
        node.setAttribute("aria-disabled", disabled ? "true" : "false");
      };
      const setHidden = (node, hidden) => {
        if (!node) return;
        node.classList.toggle("hidden", !!hidden);
      };

      const enablePartitioningUi = taskType === TASK_TYPES.SOURCE_TARGET;
      const showMode = enablePartitioningUi && enabled;
      const showColumn = enablePartitioningUi && enabled && columnRequired;
      const showParts = enablePartitioningUi && enabled && partsRequired;
      const showDistinctLimit = enablePartitioningUi && enabled && isDistinct;
      const showExplicit = enablePartitioningUi && enabled && isExplicit;
      const showModeHint = enablePartitioningUi && enabled && !!modeHintText;

      setHidden(modeWrap, !showMode);
      setHidden(modeHint, !showModeHint);
      setHidden(columnWrap, !showColumn);
      setHidden(partsWrap, !showParts);
      setHidden(distinctLimitWrap, !showDistinctLimit);
      setHidden(explicitWrap, !showExplicit);
      setHidden(modeRowWrap, !enablePartitioningUi || !enabled || (!showMode && !showColumn));
      setHidden(secondaryWrap, !enablePartitioningUi || !enabled || (!showParts && !showDistinctLimit));
      if (modeHint) {
        modeHint.textContent = modeHintText;
      }

      setDisabled(modeSelect, !enablePartitioningUi || !showMode);
      setDisabled(columnInput, !showColumn || (columnInput.options && columnInput.options.length <= 1));
      setDisabled(partsInput, !showParts);
      setDisabled(distinctLimitInput, !showDistinctLimit);
      setDisabled(explicitInput, !showExplicit);
      enabledInput.disabled = !enablePartitioningUi;
      enabledInput.setAttribute("aria-disabled", enablePartitioningUi ? "false" : "true");
      columnInput.required = !!showColumn;
      partsInput.required = !!showParts;
      distinctLimitInput.required = !!showDistinctLimit;
      explicitInput.required = !!showExplicit;
    }

    function bindPartitionState(card) {
      const enabledInput = card.querySelector(".partitioning-enabled");
      const modeSelect = card.querySelector(".partitioning-mode");
      enabledInput.addEventListener("change", () => syncPartitionState(card));
      modeSelect.addEventListener("change", () => {
        syncPartitionState(card);
      });
      syncPartitionState(card);
    }

    function bindTaskTabs(card) {
      const tabButtons = card.querySelectorAll(".tab-btn");
      const panels = card.querySelectorAll(".tab-panel");
      for (const tabBtn of tabButtons) {
        tabBtn.addEventListener("click", () => {
          const target = tabBtn.getAttribute("data-tab");
          for (const b of tabButtons) b.classList.remove("active");
          for (const p of panels) p.classList.remove("active");
          tabBtn.classList.add("active");
          const panel = card.querySelector(`.tab-panel[data-tab-panel="${target}"]`);
          if (panel) panel.classList.add("active");
        });
      }
    }

    function bindTaskAutocomplete(card) {
      const taskTypeSelect = card.querySelector(".task-type");
      const sourceSchemaInput = card.querySelector(".source-schema");
      const sourceTableInput = card.querySelector(".source-table");
      const targetSchemaInput = card.querySelector(".target-schema");
      const targetTableInput = card.querySelector(".target-table");
      const sourceTypeSelect = card.querySelector(".source-type");
      const loadMethodSelect = card.querySelector(".load-method");
      const scriptEnvSelect = card.querySelector(".script-run-environment");
      const scriptSqlInput = card.querySelector(".script-sql");
      const dagTaskDagSelect = card.querySelector(".dag-task-dag-id");
      const schedulePartitionColumnRefresh = () => {
        clearTimeout(card._ffPartitionColumnTimer);
        card._ffPartitionColumnTimer = setTimeout(() => {
          loadPartitionColumnOptions(card);
        }, 220);
      };

      taskTypeSelect.addEventListener("change", () => {
        syncTaskTypeState(card);
        syncPartitionState(card);
        syncMappingState(card);
        refreshTaskCardHeaders();
      });

      sourceTypeSelect.addEventListener("change", () => {
        toggleSourceMode(card);
        refreshTaskCardHeaders();
        syncMappingState(card);
        schedulePartitionColumnRefresh();
      });

      sourceSchemaInput.addEventListener("input", () => {
        if (sourceTypeSelect.value === "sql") return;
        clearTimeout(sourceSchemaInput._ffTimer);
        sourceSchemaInput._ffTimer = setTimeout(() => {
          autocompleteSchemas(
            el("source_conn_id").value.trim(),
            sourceSchemaInput.value.trim(),
            "source_schema_options",
            "source_conn_id"
          );
          loadPartitionColumnOptions(card);
        }, 220);
      });
      sourceSchemaInput.addEventListener("change", () => schedulePartitionColumnRefresh());

      sourceTableInput.addEventListener("input", () => {
        if (sourceTypeSelect.value === "sql") return;
        clearTimeout(sourceTableInput._ffTimer);
        sourceTableInput._ffTimer = setTimeout(() => {
          autocompleteTables(
            el("source_conn_id").value.trim(),
            sourceSchemaInput.value.trim(),
            sourceTableInput.value.trim(),
            "source_table_options"
          );
          loadPartitionColumnOptions(card);
        }, 220);
      });
      sourceTableInput.addEventListener("change", () => schedulePartitionColumnRefresh());

      targetSchemaInput.addEventListener("input", () => {
        refreshTaskCardHeaders();
        clearTimeout(targetSchemaInput._ffTimer);
        targetSchemaInput._ffTimer = setTimeout(() => {
          autocompleteSchemas(
            el("target_conn_id").value.trim(),
            targetSchemaInput.value.trim(),
            "target_schema_options",
            "target_conn_id"
          );
        }, 220);
      });

      targetTableInput.addEventListener("input", () => {
        refreshTaskCardHeaders();
        clearTimeout(targetTableInput._ffTimer);
        targetTableInput._ffTimer = setTimeout(() => {
          autocompleteTables(
            el("target_conn_id").value.trim(),
            targetSchemaInput.value.trim(),
            targetTableInput.value.trim(),
            "target_table_options"
          );
        }, 220);
      });
      sourceSchemaInput.addEventListener("input", () => refreshTaskCardHeaders());
      sourceTableInput.addEventListener("input", () => refreshTaskCardHeaders());
      loadMethodSelect.addEventListener("change", () => refreshTaskCardHeaders());
      scriptEnvSelect.addEventListener("change", () => refreshTaskCardHeaders());
      scriptSqlInput.addEventListener("input", () => refreshTaskCardHeaders());
      dagTaskDagSelect.addEventListener("change", () => {
        refreshTaskCardHeaders();
        syncTaskTypeState(card);
      });
    }

    function addTaskCard(values = {}, options = {}) {
      const template = el("task_card_template");
      const node = template.content.firstElementChild.cloneNode(true);
      const fallbackIndex = getTaskCards().length + 1;
      applyFriendlyLoadMethodLabels(node);
      bindBindingsSection(node);
      bindTaskTypeSegment(node);
      bindTaskCollapse(node);
      setTaskCardValues(node, values, fallbackIndex);
      bindTaskTabs(node);
      bindTaskAutocomplete(node);
      bindPartitionState(node);
      bindMappingState(node);
      bindDependencyState(node);
      node.querySelector(".btn-delete-task").addEventListener("click", (ev) => {
        ev.stopPropagation();
        requestTaskDelete(node);
      });
      setTaskCardCollapsed(node, false);
      el("tasks_container").appendChild(node);
      if (options.refresh !== false) {
        refreshTaskCardHeaders();
      }
    }

    function bindMappingState(card) {
      const modeSelect = card.querySelector(".column-mapping-mode");
      const generateButton = card.querySelector(".btn-generate-mapping");
      modeSelect.addEventListener("change", () => syncMappingState(card));
      generateButton.addEventListener("click", () => generateMappingForCard(card));
      syncMappingState(card);
    }

    function clearAndLoadTasks(taskItems) {
      const tasks = Array.isArray(taskItems) && taskItems.length ? taskItems : [{}];
      el("tasks_container").innerHTML = "";
      for (const item of tasks) {
        addTaskCard(item || {}, { refresh: false });
      }
      refreshTaskCardHeaders();
    }

    function applyPreloadPayload(payload, dagId) {
      el("project").value = payload.project || "";
      el("domain").value = payload.domain || "";
      el("level").value = payload.level || "";
      el("flow").value = payload.flow || "";
      setCustomTags(payload.custom_tags || []);
      setSchedulerAppliedState(payload.scheduler || null);
      const rawDagDeps = payload && typeof payload === "object" ? payload.dag_dependencies : null;
      const dagDepsUpstream = rawDagDeps && typeof rawDagDeps === "object"
        ? rawDagDeps.upstream_dag_ids
        : [];
      setDagDepsAppliedStateFromUpstreamIds(dagDepsUpstream || []);
      setSchedulerFormFromState(schedulerAppliedState);
      syncFolderPathDisplay();
      setConnectionValue("source_conn_id", payload.source_conn_id || "");
      setConnectionValue("target_conn_id", payload.target_conn_id || "");
      clearAndLoadTasks(payload.flow_tasks || [payload]);
    }

    async function preloadByDagId(rawDagId) {
      const dagId = (rawDagId || "").trim();
      if (!dagId) {
        currentUpdateDagId = "";
        setUpdateModeStatus("Enter dag_id for preload.", "warn");
        setUpdateMode(false);
        return;
      }
      const r = await studioFetch(`/api/dag-config?dag_id=${encodeURIComponent(dagId)}`);
      const data = await r.json();
      logDebug("dag-config preload response", { status_code: r.status, ...data });
      if (!r.ok || !data.ok) {
        currentUpdateDagId = "";
        setUpdateModeStatus(`DAG preload failed: ${data.detail || r.status}`, "warn");
        setUpdateMode(false);
        return;
      }
      currentUpdateDagId = dagId;
      applyPreloadPayload(data.payload || {}, dagId);
      await loadDagDependencyOptions(dagId);
      await loadFolderOptions();
      renderRevisionOptions([], data.active_revision_id || "");
      await loadRevisions(dagId);
      setUpdateModeStatus(`Update mode loaded: ${dagId}. Add a new task and save with Update.`, "ok");
      setUpdateMode(true);
    }

    function resolveInitialDagId() {
      const params = new URLSearchParams(window.location.search || "");
      const fromQuery = (params.get("dag_id") || "").trim();
      if (fromQuery) return fromQuery;

      const path = String(window.location.pathname || "").trim();
      if (path) {
        const mPath = path.match(/\/dags\/([^\/?#]+)/);
        if (mPath && mPath[1]) {
          try {
            return decodeURIComponent(mPath[1]);
          } catch (_err) {
            return String(mPath[1] || "").trim();
          }
        }
      }

      const ref = (document.referrer || "").trim();
      if (!ref) return "";
      try {
        const u = new URL(ref);
        const m = u.pathname.match(/\/dags\/([^\/?#]+)/);
        return m ? decodeURIComponent(m[1]) : "";
      } catch (_err) {
        return "";
      }
    }

    async function postJson(url, body) {
      const r = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await r.json();
      logDebug("POST response", { url, status_code: r.status, ...data });
      return data;
    }

    async function deleteJson(url) {
      const r = await fetch(url, { method: "DELETE" });
      const data = await parseJsonSafe(r);
      logDebug("DELETE response", { url, status_code: r.status, ...data });
      return data;
    }

    async function submitUpdate() {
      const dagId = (currentUpdateDagId || "").trim();
      if (!dagId) {
        setUpdateModeStatus("dag_id is required for update mode. Preload a DAG first.", "warn");
        pushToast("dag_id is required for update mode. Preload a DAG first.", "error", true);
        return;
      }

      if (!beginOperation("Updating configuration...")) {
        return;
      }
      try {
        const data = await postJson(
          studioUrl(`/api/update-dag?dag_id=${encodeURIComponent(dagId)}`),
          collectPayload()
        );
        if (!data || !data.ok) {
          setUpdateModeStatus("Update failed.", "warn");
          pushToast(apiErrorMessage(data, "Update failed."), "error", true);
          return;
        }
        currentUpdateDagId = String(data.dag_id || dagId || "").trim();
        await loadDagDependencyOptions(currentUpdateDagId);
        await loadRevisions(currentUpdateDagId);
        setUpdateModeStatus(`Update completed: ${currentUpdateDagId}`, "ok");
        pushToast(`Update completed: ${currentUpdateDagId}`, "success", false);
      } catch (err) {
        logDebug("submit update error", err);
        setUpdateModeStatus("Unexpected error occurred during update.", "warn");
        pushToast("Unexpected error occurred during update.", "error", true);
      } finally {
        endOperation();
      }
    }

    function dagIdFromDagPath(rawDagPath) {
      const dagPath = String(rawDagPath || "").trim();
      if (!dagPath) return "";
      const parts = dagPath.split("/");
      const fileName = String(parts[parts.length - 1] || "").trim();
      return fileName.endsWith(".py") ? fileName.slice(0, -3) : fileName;
    }

    async function submitCreate() {
      if (!beginOperation("Creating DAG...")) {
        return;
      }
      try {
        const data = await postJson(studioUrl("/api/create-dag"), collectPayload());
        if (!data || !data.ok) {
          pushToast(apiErrorMessage(data, "Create failed."), "error", true);
          return;
        }
        const dagId = String(data.dag_id || "").trim() || dagIdFromDagPath(data.dag_path);
        if (!dagId) {
          setUpdateModeStatus("Create succeeded, but dag_id could not be resolved. Update mode was not enabled.", "warn");
          pushToast("Create succeeded, but dag_id could not be resolved.", "error", true);
          return;
        }
        currentUpdateDagId = dagId;
        setUpdateMode(true);
        setUpdateModeStatus(`Create completed, update mode active: ${dagId}`, "ok");
        pushToast(`Create completed: ${dagId}`, "success", false);
        await loadDagDependencyOptions(dagId);
        await loadRevisions(dagId);
        try {
          const url = new URL(window.location.href);
          url.searchParams.set("dag_id", dagId);
          window.history.replaceState({}, "", url.toString());
        } catch (_err) {
          // no-op
        }
      } catch (err) {
        logDebug("submit create error", err);
        setUpdateModeStatus("Unexpected error occurred during create.", "warn");
        pushToast("Unexpected error occurred during create.", "error", true);
      } finally {
        endOperation();
      }
    }

    async function submitSave() {
      const dagId = (currentUpdateDagId || "").trim();
      if (dagId) {
        await submitUpdate();
        return;
      }
      await submitCreate();
    }

    function collectTaskPayload(card, index, taskIds) {
      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim() || TASK_TYPES.SOURCE_TARGET;
      const sourceType = card.querySelector(".source-type").value;
      const sourceSchemaVal = card.querySelector(".source-schema").value.trim();
      const sourceTableVal = card.querySelector(".source-table").value.trim();
      const targetSchemaVal = card.querySelector(".target-schema").value.trim();
      const targetTableVal = card.querySelector(".target-table").value.trim();
      const inlineSqlVal = card.querySelector(".source-inline-sql").value.trim();
      const scriptRunEnvironment = String(card.querySelector(".script-run-environment")?.value || "source").trim();
      const scriptSqlVal = String(card.querySelector(".script-sql")?.value || "").trim();
      const dagTaskDagId = String(card.querySelector(".dag-task-dag-id")?.value || "").trim();
      const identity = resolveTaskIdentity(card, index);
      const normalizedTaskIds = Array.isArray(taskIds) ? taskIds : [];
      const selfTaskId = String(normalizedTaskIds[index - 1] || identity.task_group_id || "").trim();
      const previousTaskId = index > 1 ? String(normalizedTaskIds[index - 2] || "").trim() : "";
      const dependencyMode = getCardDependencyMode(card);
      let dependsOn = [];
      if (dependencyMode === DEPENDENCY_MODES.WAIT_PREVIOUS && previousTaskId) {
        dependsOn = [previousTaskId];
      } else if (dependencyMode === DEPENDENCY_MODES.CUSTOM) {
        const available = new Set(normalizedTaskIds);
        const pendingSelected = String(card.querySelector(".dependency-custom-select")?.value || "").trim();
        const merged = pendingSelected
          ? [...getCardCustomDependsOn(card), pendingSelected]
          : getCardCustomDependsOn(card);
        dependsOn = normalizeDependsOnList(
          merged.filter((depId) => depId !== selfTaskId && available.has(depId))
        );
      }
      const partitioningMode = card.querySelector(".partitioning-mode").value;
      const partitioningEnabled = taskType === TASK_TYPES.SOURCE_TARGET && !!card.querySelector(".partitioning-enabled").checked;
      const partitioningDistinctLimit = asPositiveInt(
        card.querySelector(".partitioning-distinct-limit").value,
        16
      );
      const partitioningRanges = partitioningMode === "explicit"
        ? parseExplicitWhereList(card.querySelector(".partitioning-ranges").value)
        : [];
      const partitioningColumn = partitioningEnabled && PARTITION_COLUMN_REQUIRED_MODES.has(partitioningMode)
        ? card.querySelector(".partitioning-column").value.trim() || undefined
        : undefined;
      const partitioningParts = partitioningEnabled && PARTITION_PARTS_REQUIRED_MODES.has(partitioningMode)
        ? Number(card.querySelector(".partitioning-parts").value || 2)
        : undefined;
      const partitioningDistinctLimitValue = partitioningEnabled && partitioningMode === "distinct"
        ? partitioningDistinctLimit
        : undefined;
      const normalizedSourceSchema = taskType === TASK_TYPES.SOURCE_TARGET && sourceType !== "sql" ? sourceSchemaVal : undefined;
      const normalizedSourceTable = taskType === TASK_TYPES.SOURCE_TARGET && sourceType !== "sql" ? sourceTableVal : undefined;
      const normalizedTargetSchema = taskType === TASK_TYPES.SOURCE_TARGET
        ? (targetSchemaVal || undefined)
        : (targetSchemaVal || "script_tgt");
      const normalizedTargetTable = taskType === TASK_TYPES.SOURCE_TARGET
        ? (targetTableVal || undefined)
        : (targetTableVal || "script_task");
      const bindings = getBindingRows(card)
        .map((row) => {
          const bindingSource = row.querySelector(".binding-source").value;
          const item = {
            variable_name: row.querySelector(".binding-variable-name").value.trim(),
            binding_source: bindingSource,
          };
          if (bindingSource === "default") {
            item.default_value = row.querySelector(".binding-default-value").value.trim() || undefined;
          } else if (bindingSource === "source" || bindingSource === "target") {
            item.sql = row.querySelector(".binding-sql").value.trim() || undefined;
          } else if (bindingSource === "airflow_variable") {
            item.airflow_variable_key = row.querySelector(".binding-airflow-variable-key").value.trim() || undefined;
          }
          return item;
        })
        .filter((item) => item.variable_name);
      return {
        task_type: taskType,
        task_group_id: identity.task_group_id,
        source_schema: normalizedSourceSchema,
        source_table: normalizedSourceTable,
        source_type: sourceType,
        inline_sql: taskType === TASK_TYPES.SOURCE_TARGET && sourceType === "sql" ? (inlineSqlVal || undefined) : undefined,
        script_run_environment: taskType === TASK_TYPES.SCRIPT_RUN ? (scriptRunEnvironment || undefined) : undefined,
        script_sql: taskType === TASK_TYPES.SCRIPT_RUN ? (scriptSqlVal || undefined) : undefined,
        dag_task_dag_id: taskType === TASK_TYPES.DAG ? (dagTaskDagId || undefined) : undefined,
        target_schema: normalizedTargetSchema,
        target_table: normalizedTargetTable,
        load_method: card.querySelector(".load-method").value,
        column_mapping_mode: card.querySelector(".column-mapping-mode").value,
        mapping_content: taskType === TASK_TYPES.SOURCE_TARGET && card.querySelector(".column-mapping-mode").value === "mapping_file"
          ? (card.querySelector(".mapping-content").value || "").trim() || undefined
          : undefined,
        where: taskType === TASK_TYPES.SOURCE_TARGET ? (card.querySelector(".where").value.trim() || undefined) : undefined,
        batch_size: Number(card.querySelector(".batch-size").value || 10000),
        partitioning_enabled: partitioningEnabled,
        partitioning_mode: partitioningMode,
        partitioning_column: partitioningColumn,
        partitioning_parts: partitioningParts,
        partitioning_distinct_limit: partitioningDistinctLimitValue,
        partitioning_ranges: partitioningRanges,
        bindings: bindings.length ? bindings : undefined,
        depends_on: dependsOn,
      };
    }

    function collectPayload() {
      const projectVal = el("project").value.trim() || "webhook";
      const domainVal = el("domain").value.trim() || "default_domain";
      const levelVal = el("level").value.trim() || "level1";
      const flowVal = el("flow").value.trim() || "src_to_stg";
      const cards = getTaskCards();
      const taskIds = cards.map((card, idx) => resolveTaskIdentity(card, idx + 1).task_group_id);
      const flowTasks = cards.map((card, idx) => collectTaskPayload(card, idx + 1, taskIds));
      const firstTask = flowTasks[0] || {};
      const payload = {
        project: projectVal,
        domain: domainVal,
        level: levelVal,
        flow: flowVal,
        custom_tags: customTagsState.slice(),
        scheduler: cloneSchedulerState(schedulerAppliedState || collectSchedulerFormPayload()),
        dag_dependencies: collectDagDependenciesPayload(),
        source_conn_id: el("source_conn_id").value,
        target_conn_id: el("target_conn_id").value,
        task_group_id: firstTask.task_group_id,
        task_type: firstTask.task_type,
        source_schema: firstTask.source_schema,
        source_table: firstTask.source_table,
        source_type: firstTask.source_type,
        inline_sql: firstTask.inline_sql,
        script_run_environment: firstTask.script_run_environment,
        script_sql: firstTask.script_sql,
        dag_task_dag_id: firstTask.dag_task_dag_id,
        target_schema: firstTask.target_schema,
        target_table: firstTask.target_table,
        load_method: firstTask.load_method,
        column_mapping_mode: firstTask.column_mapping_mode,
        where: firstTask.where,
        batch_size: firstTask.batch_size,
        partitioning_enabled: firstTask.partitioning_enabled,
        partitioning_mode: firstTask.partitioning_mode,
        partitioning_column: firstTask.partitioning_column,
        partitioning_parts: firstTask.partitioning_parts,
        partitioning_distinct_limit: firstTask.partitioning_distinct_limit,
        partitioning_ranges: firstTask.partitioning_ranges,
        flow_tasks: flowTasks,
      };
      return payload;
    }

    for (const btn of document.querySelectorAll(".btn-create-dag")) {
      btn.onclick = () => submitSave();
    }
    el("btn_expand_all_tasks").onclick = () => setAllTaskCardsCollapsed(false);
    el("btn_collapse_all_tasks").onclick = () => setAllTaskCardsCollapsed(true);
    el("btn_add_task").onclick = () => addTaskCard({});
    el("btn_update_top").onclick = () => submitUpdate();
    el("btn_refresh_revisions").onclick = () => loadRevisions(currentUpdateDagId);
    el("btn_promote_revision").onclick = () => promoteSelectedRevision();
    el("btn_delete_dag").onclick = () => openDeleteDagModal();
    el("btn_cancel_delete_dag").onclick = () => closeDeleteDagModal();
    el("btn_confirm_delete_dag").onclick = () => deleteCurrentDag();
    el("btn_cancel_task_delete").onclick = () => closeTaskDeleteModal();
    el("btn_confirm_task_delete").onclick = () => confirmTaskDelete();
    const schedulerCompactPanel = el("scheduler_compact_panel");
    if (schedulerCompactPanel) {
      schedulerCompactPanel.addEventListener("click", () => openSchedulerModal());
      schedulerCompactPanel.addEventListener("keydown", (evt) => {
        if (evt.key === "Enter" || evt.key === " ") {
          evt.preventDefault();
          openSchedulerModal();
        }
      });
    }
    const dagDepsCompactPanel = el("dag_deps_compact_panel");
    if (dagDepsCompactPanel) {
      dagDepsCompactPanel.addEventListener("click", () => openDagDepsModal());
      dagDepsCompactPanel.addEventListener("keydown", (evt) => {
        if (evt.key === "Enter" || evt.key === " ") {
          evt.preventDefault();
          openDagDepsModal();
        }
      });
    }
    el("btn_cancel_scheduler_modal").onclick = () => closeSchedulerModal();
    el("btn_apply_scheduler_modal").onclick = () => applySchedulerModal();
    el("scheduler_modal_backdrop").onclick = () => closeSchedulerModal();
    el("btn_cancel_dag_deps_modal").onclick = () => closeDagDepsModal();
    el("btn_apply_dag_deps_modal").onclick = () => applyDagDepsModal();
    el("dag_deps_modal_backdrop").onclick = () => closeDagDepsModal();
    const dagDepsCustomSelect = el("dag_deps_custom_select");
    const addDagDep = () => {
      const selectNode = el("dag_deps_custom_select");
      if (!selectNode) return;
      const selectedDagId = String(selectNode.value || "").trim();
      if (!selectedDagId) return;
      const draft = cloneDagDepsState(dagDepsDraftState || dagDepsAppliedState || {});
      draft.upstream_dag_ids = normalizeDagDependencyIds([
        ...draft.upstream_dag_ids,
        selectedDagId,
      ]);
      dagDepsDraftState = draft;
      renderDagDepsModal();
    };
    el("btn_add_dag_dependency").onclick = () => addDagDep();
    if (dagDepsCustomSelect) {
      dagDepsCustomSelect.addEventListener("change", () => addDagDep());
      dagDepsCustomSelect.addEventListener("dblclick", () => addDagDep());
    }
    el("delete_dag_confirm_input").addEventListener("input", () => syncDeleteDagConfirmState());
    el("delete_dag_backdrop").onclick = () => closeDeleteDagModal();
    el("delete_task_backdrop").onclick = () => closeTaskDeleteModal();
    el("revision_select").addEventListener("change", () => renderRevisionMeta());

    el("btn_open_folder_picker").onclick = openFolderPicker;
    el("btn_close_folder_picker").onclick = closeFolderPicker;
    el("btn_cancel_folder_picker").onclick = closeFolderPicker;
    el("folder_picker_backdrop").onclick = closeFolderPicker;
    el("btn_apply_folder_picker").onclick = applyFolderPickerSelection;
    el("btn_add_project").onclick = () => addDraftFolder("project");
    el("btn_add_domain").onclick = () => addDraftFolder("domain");
    el("btn_add_level").onclick = () => addDraftFolder("level");
    el("btn_add_flow").onclick = () => addDraftFolder("flow");
    el("source_conn_id").addEventListener("change", () => {
      refreshTaskCardHeaders();
      refreshAllPartitionColumnOptions();
    });
    el("target_conn_id").addEventListener("change", () => refreshTaskCardHeaders());
    const customTagsInput = el("custom_tags_input");
    if (customTagsInput) {
      customTagsInput.addEventListener("keydown", (evt) => {
        if (evt.key === "Enter" || evt.key === "," || (evt.key === " " && String(customTagsInput.value || "").trim())) {
          evt.preventDefault();
          flushCustomTagInput();
          return;
        }
        if (evt.key === "Backspace" && !String(customTagsInput.value || "").trim() && customTagsState.length) {
          evt.preventDefault();
          customTagsState = customTagsState.slice(0, -1);
          renderCustomTags();
        }
      });
      customTagsInput.addEventListener("blur", () => flushCustomTagInput());
    }
    document.addEventListener("keydown", (evt) => {
      if (evt.key !== "Escape") return;
      if (el("scheduler_modal").classList.contains("open")) {
        closeSchedulerModal();
        return;
      }
      if (el("dag_deps_modal").classList.contains("open")) {
        closeDagDepsModal();
        return;
      }
      if (el("folder_picker_modal").classList.contains("open")) {
        closeFolderPicker();
        return;
      }
      if (el("delete_dag_modal").classList.contains("open")) {
        closeDeleteDagModal();
        return;
      }
      if (el("delete_task_modal").classList.contains("open")) {
        closeTaskDeleteModal();
      }
    });

    async function initPage() {
      await applyAirflowThemeAssets();
      bindSchedulerControls();
      setUpdateMode(false);
      setCustomTags([]);
      dagDepsAppliedState = {
        upstream_dag_ids: [],
      };
      dagDepsDraftState = null;
      dagDepsOptionsState = [];
      dagDepsReferencedByState = [];
      renderDagDepsCompactSummary();
      await initializeSchedulerDefaultsForCreate();
      syncFolderPathDisplay();
      clearAndLoadTasks([{}]);
      // Connection list must be loaded first for main form usage.
      try {
        await loadConnections();
      } catch (_err) {
        // no-op: UI message already shown
      }
      try {
        await loadAirflowVariables();
      } catch (_err) {
        // no-op: UI message already shown
      }
      try {
        await loadFolderOptions();
      } catch (_err) {
        // no-op: UI message already shown
      }
      await loadDagDependencyOptions("").catch((_err) => {});
      const initialDagId = resolveInitialDagId();
      if (initialDagId) {
        await preloadByDagId(initialDagId);
      }
    }

    initPage();

