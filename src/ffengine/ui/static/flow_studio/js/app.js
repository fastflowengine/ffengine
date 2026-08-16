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
    const SCHEDULER_MODES = ["manual", "minutely", "hourly", "daily", "weekly", "monthly", "advanced", "asset"];
    const LOAD_METHOD_LABELS = Object.freeze({
      create_if_not_exists_or_truncate: "Create if not exists or truncate",
      append: "Append rows",
      upsert: "Upsert (insert/update)",
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
      BINDING: "binding",
      DBT: "dbt",
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
    const UPSERT_MATCH_MAX_COUNT = 32;
    const FOLDER_PATH_PROMPT = "Select a project and DAG path";
    let customTagsState = [];
    let schedulerModeState = "manual";
    let schedulerAppliedState = null;
    let dagParamsAppliedState = null;
    let dagParamsDraftState = null;
    let notificationsAppliedState = null;
    let notificationsDraftState = null;
    let allConnectionsState = [];
    let mailTemplateNamesState = ["Default"];
    let dagDepsAppliedState = null;
    let dagDepsDraftState = null;
    let dagDepsOptionsState = [];
    let dagDepsReferencedByState = [];
    let pendingDeleteDagCleanupReferences = false;
    let engineConfigExplicit = false;

    function el(id) { return document.getElementById(id); }

    function syncEngineOptions() {
      const preference = String(el("engine_preference")?.value || "auto").trim();
      const sparkOptions = el("engine_spark_options");
      if (sparkOptions) sparkOptions.style.display = preference === "spark" ? "grid" : "none";
      const connId = el("engine_spark_conn_id");
      const mode = String(el("engine_spark_submit_mode")?.value || "k8s").trim();
      if (connId) connId.required = preference === "spark" && mode !== "local";
    }

    function resetEngineConfig() {
      engineConfigExplicit = false;
      if (el("engine_preference")) el("engine_preference").value = "auto";
      if (el("engine_spark_submit_mode")) el("engine_spark_submit_mode").value = "k8s";
      if (el("engine_spark_conn_id")) el("engine_spark_conn_id").value = "";
      syncEngineOptions();
    }

    function logDebug(message, payload) {
      if (typeof payload === "undefined") {
        console.debug(`[flow-studio] ${message}`);
        return;
      }
      console.debug(`[flow-studio] ${message}`, payload);
    }

    function defaultDagParams() {
      return [{
        name: "log_level",
        type: "string",
        default: "default",
        enum: ["default", "DEBUG"],
        description: "FFEngine run log detail",
      }];
    }

    function normalizeDagParams(rawParams) {
      const items = Array.isArray(rawParams) ? rawParams : [];
      const custom = items
        .filter((item) => String(item && item.name || "").trim() !== "log_level")
        .map((item) => ({
          name: String(item.name || "").trim(),
          type: String(item.type || "string").trim(),
          description: String(item.description || "").trim() || undefined,
        }));
      const logItem = items.find((item) => String(item && item.name || "").trim() === "log_level") || {};
      return [{
        name: "log_level",
        type: "string",
        default: ["default", "DEBUG"].includes(logItem.default) ? logItem.default : "default",
        enum: ["default", "DEBUG"],
        description: "FFEngine run log detail",
      }, ...custom];
    }

    function renderAdvancedSummary() {
      const summary = el("advanced_compact_summary");
      if (!summary) return;
      const params = normalizeDagParams(dagParamsAppliedState || defaultDagParams());
      const logLevel = String(params[0].default || "default");
      summary.textContent = `Log level: ${logLevel === "default" ? "Default" : logLevel} • ${Math.max(0, params.length - 1)} parameters • Notify: ${notificationsSummaryText(notificationsAppliedState)}`;
    }

    const NOTIFY_LABELS = { failure: "Failure", success: "Success", deadline: "Deadline" };

    function notificationsSummaryText(state) {
      if (!state || !Array.isArray(state.notify_on) || !state.notify_on.length) return "off";
      const minutes = parseInt(state.notify_deadline_minutes, 10);
      return state.notify_on
        .map((trigger) => {
          const label = NOTIFY_LABELS[trigger] || trigger;
          if (trigger === "deadline" && minutes > 0) return `Deadline(${minutes}m)`;
          return label;
        })
        .join("+");
    }

    function cloneNotifications(state) {
      if (!state || typeof state !== "object") return {};
      const triggers = Array.isArray(state.notify_on) ? state.notify_on.slice() : [];
      const emails = Array.isArray(state.notify_emails) ? state.notify_emails.slice() : [];
      const connId = String(state.notify_conn_id || "").trim();
      if (!triggers.length && !emails.length && !connId) return {};
      const out = { notify_on: triggers, notify_emails: emails, notify_conn_id: connId };
      const template = String(state.notify_template || "").trim();
      if (template && template !== "Default") out.notify_template = template;
      const minutes = parseInt(state.notify_deadline_minutes, 10);
      if (triggers.includes("deadline") && minutes > 0) out.notify_deadline_minutes = minutes;
      return out;
    }

    function setAdvancedTab(name) {
      const target = name === "notifications" ? "notifications" : "params";
      for (const btn of document.querySelectorAll(".advanced-tab-btn")) {
        btn.classList.toggle("active", btn.getAttribute("data-advanced-tab") === target);
      }
      for (const panel of document.querySelectorAll(".advanced-panel")) {
        panel.classList.toggle("active", panel.getAttribute("data-advanced-panel") === target);
      }
    }

    function populateNotifyConnSelect(selected) {
      const select = el("notify_conn_id");
      if (!select) return;
      const smtp = allConnectionsState.filter(
        (item) => String(item.conn_type || "").toLowerCase() === "smtp"
      );
      const useSmtpOnly = smtp.length > 0;
      const source = useSmtpOnly ? smtp : allConnectionsState;
      const chosen = String(selected || "").trim();
      select.innerHTML = "";
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = "Select an SMTP connection…";
      select.appendChild(placeholder);
      let matched = false;
      for (const item of source) {
        const connId = String(item.conn_id || "").trim();
        if (!connId) continue;
        if (connId === chosen) matched = true;
        const option = document.createElement("option");
        option.value = connId;
        option.textContent = useSmtpOnly
          ? connId
          : `${connId} (${String(item.conn_type || "?")})`;
        select.appendChild(option);
      }
      if (chosen && !matched) {
        const option = document.createElement("option");
        option.value = chosen;
        option.textContent = chosen;
        select.appendChild(option);
      }
      select.value = chosen;
    }

    function populateNotifyTemplateSelect(selected) {
      const select = el("notify_template");
      if (!select) return;
      const chosen = String(selected || "Default").trim() || "Default";
      select.innerHTML = "";
      const seen = new Set();
      for (const name of mailTemplateNamesState) {
        const value = String(name || "").trim();
        if (!value || seen.has(value)) continue;
        seen.add(value);
        const option = document.createElement("option");
        option.value = value;
        option.textContent = value;
        select.appendChild(option);
      }
      if (!seen.has(chosen)) {
        const option = document.createElement("option");
        option.value = chosen;
        option.textContent = chosen;
        select.appendChild(option);
      }
      select.value = chosen;
    }

    async function loadMailTemplateNames() {
      try {
        const resp = await studioFetch("/api/mail-templates");
        if (resp.ok) {
          const data = await parseJsonSafe(resp);
          const names = Array.isArray(data.names) ? data.names : [];
          if (names.length) mailTemplateNamesState = names;
        }
      } catch (_err) {
        // keep the current names (at least "Default")
      }
      const modal = el("advanced_modal");
      if (modal && modal.classList.contains("open")) {
        const select = el("notify_template");
        populateNotifyTemplateSelect(select ? select.value : "Default");
      }
    }

    function syncDeadlineMinutesVisibility() {
      const row = el("notify_deadline_minutes_row");
      if (row) row.style.display = el("notify_on_deadline").checked ? "" : "none";
    }

    function renderAdvancedNotifications() {
      const state = notificationsDraftState || {};
      const triggers = Array.isArray(state.notify_on) ? state.notify_on : [];
      const emails = Array.isArray(state.notify_emails) ? state.notify_emails : [];
      const connId = String(state.notify_conn_id || "").trim();
      const configured = triggers.length || emails.length || connId;
      el("notify_on_failure").checked = configured ? triggers.includes("failure") : true;
      el("notify_on_success").checked = triggers.includes("success");
      el("notify_on_deadline").checked = triggers.includes("deadline");
      const minutes = parseInt(state.notify_deadline_minutes, 10);
      el("notify_deadline_minutes").value = minutes > 0 ? String(minutes) : "";
      syncDeadlineMinutesVisibility();
      el("notify_emails").value = emails.join(", ");
      populateNotifyConnSelect(connId);
      populateNotifyTemplateSelect(state.notify_template || "Default");
    }

    function collectAdvancedModalNotifications() {
      const triggers = [];
      if (el("notify_on_failure").checked) triggers.push("failure");
      if (el("notify_on_success").checked) triggers.push("success");
      if (el("notify_on_deadline").checked) triggers.push("deadline");
      const emails = String(el("notify_emails").value || "")
        .split(/[,;\s]+/)
        .map((item) => item.trim())
        .filter(Boolean);
      const connId = String(el("notify_conn_id").value || "").trim();
      // Nothing configured -> notifications off (no error).
      if (!emails.length && !connId) return null;
      if (!triggers.length) {
        throw new Error("Select at least one notification trigger (Failure/Success/Deadline).");
      }
      if (!emails.length) {
        throw new Error("Add at least one recipient email for notifications.");
      }
      if (!connId) {
        throw new Error("Select an SMTP Airflow Connection for notifications.");
      }
      const seen = new Set();
      const uniqueEmails = [];
      for (const addr of emails) {
        if ((addr.match(/@/g) || []).length !== 1 || /\s/.test(addr) || !addr.includes(".")) {
          throw new Error(`Invalid email address: ${addr}`);
        }
        const low = addr.toLowerCase();
        if (seen.has(low)) continue;
        seen.add(low);
        uniqueEmails.push(addr);
      }
      let deadlineMinutes = 0;
      if (triggers.includes("deadline")) {
        deadlineMinutes = parseInt(el("notify_deadline_minutes").value, 10);
        if (!Number.isFinite(deadlineMinutes) || deadlineMinutes <= 0) {
          throw new Error("Enter a positive Deadline (minutes) value.");
        }
      }
      const result = { notify_on: triggers, notify_emails: uniqueEmails, notify_conn_id: connId };
      const template = String(el("notify_template").value || "").trim();
      if (template && template !== "Default") result.notify_template = template;
      if (deadlineMinutes > 0) result.notify_deadline_minutes = deadlineMinutes;
      return result;
    }

    function createAdvancedParamRow(value = {}) {
      const row = document.createElement("div");
      row.className = "dag-param-row";
      row.innerHTML = `
        <input class="dag-param-name" placeholder="parameter_name">
        <select class="dag-param-type">
          <option value="string">String</option>
          <option value="integer">Integer</option>
          <option value="number">Number</option>
          <option value="boolean">Boolean</option>
        </select>
        <input class="dag-param-description" placeholder="Description">
        <button class="btn btn-danger dag-param-remove" type="button">x</button>`;
      row.querySelector(".dag-param-name").value = value.name || "";
      row.querySelector(".dag-param-type").value = value.type || "string";
      row.querySelector(".dag-param-description").value = value.description || "";
      row.querySelector(".dag-param-remove").onclick = () => row.remove();
      el("advanced_params_list").appendChild(row);
    }

    function renderAdvancedModal() {
      const params = normalizeDagParams(dagParamsDraftState || defaultDagParams());
      el("advanced_log_level").value = params[0].default || "default";
      const list = el("advanced_params_list");
      list.innerHTML = "";
      for (const item of params.slice(1)) createAdvancedParamRow(item);
    }

    function collectAdvancedModalParams() {
      const params = defaultDagParams();
      params[0].default = el("advanced_log_level").value || "default";
      const seen = new Set(["log_level"]);
      for (const row of el("advanced_params_list").querySelectorAll(".dag-param-row")) {
        const name = row.querySelector(".dag-param-name").value.trim();
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name) || seen.has(name)) {
          throw new Error(`Invalid or duplicate DAG parameter: ${name || "(empty)"}`);
        }
        seen.add(name);
        const type = row.querySelector(".dag-param-type").value;
        params.push({
          name,
          type,
          description: row.querySelector(".dag-param-description").value.trim() || undefined,
        });
      }
      return params;
    }

    function openAdvancedModal() {
      if (isBusy) return;
      dagParamsDraftState = normalizeDagParams(dagParamsAppliedState || defaultDagParams());
      notificationsDraftState = cloneNotifications(notificationsAppliedState);
      loadMailTemplateNames();
      renderAdvancedModal();
      renderAdvancedNotifications();
      setAdvancedTab("params");
      el("advanced_modal").classList.add("open");
      el("advanced_modal").setAttribute("aria-hidden", "false");
    }

    function closeAdvancedModal() {
      el("advanced_modal").classList.remove("open");
      el("advanced_modal").setAttribute("aria-hidden", "true");
      dagParamsDraftState = null;
      notificationsDraftState = null;
    }

    function applyAdvancedModal() {
      try {
        const params = collectAdvancedModalParams();
        const notifications = collectAdvancedModalNotifications();
        dagParamsAppliedState = params;
        notificationsAppliedState = notifications;
        refreshDagParameterBindingControls();
        renderAdvancedSummary();
        closeAdvancedModal();
      } catch (err) {
        pushToast(err.message || String(err), "error", true);
      }
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
      if (next === "asset") {
        // Lazy: the catalog endpoint is hit only when the asset tab opens.
        loadDbtAssetOptions(collectSelectedSchedulerAssets());
      }
      syncSchedulerPreview();
    }

    function collectSelectedSchedulerAssets() {
      const select = el("scheduler_asset_select");
      if (!select) return [];
      return Array.from(select.selectedOptions || [])
        .map((opt) => String(opt.value || "").trim())
        .filter(Boolean);
    }

    async function loadDbtAssetOptions(selectedUris) {
      const select = el("scheduler_asset_select");
      const status = el("scheduler_asset_status");
      if (!select) return;
      const keep = new Set(
        (selectedUris || []).map((uri) => String(uri || "").trim()).filter(Boolean)
      );
      if (status) status.textContent = "Loading asset catalog...";
      let options = [];
      let errors = [];
      try {
        const r = await studioFetch("/api/dbt-assets");
        const data = await parseJsonSafe(r);
        if (!r.ok || !data || !data.ok) {
          const detail = (data && (data.detail || data.error)) || `HTTP ${r.status}`;
          if (status) status.textContent = `Asset catalog unavailable: ${detail}`;
          select.innerHTML = "";
          return;
        }
        options = Array.isArray(data.options) ? data.options : [];
        errors = Array.isArray(data.errors) ? data.errors : [];
      } catch (err) {
        if (status) status.textContent = `Asset catalog unavailable: ${err}`;
        select.innerHTML = "";
        return;
      }
      select.innerHTML = "";
      const seen = new Set();
      for (const item of options) {
        const uri = String((item && item.uri) || "").trim();
        if (!uri || seen.has(uri)) continue;
        seen.add(uri);
        const opt = document.createElement("option");
        opt.value = uri;
        const producer = String((item && item.producer_dag_id) || "").trim();
        opt.textContent = producer ? `${uri}  [${producer}]` : uri;
        if (keep.has(uri)) opt.selected = true;
        select.appendChild(opt);
      }
      // Stored URIs missing from the catalog stay VISIBLE and selected so a
      // hydrated config never silently loses them; save fails loud anyway.
      for (const uri of keep) {
        if (seen.has(uri)) continue;
        const opt = document.createElement("option");
        opt.value = uri;
        opt.textContent = `${uri}  [not in catalog]`;
        opt.selected = true;
        select.appendChild(opt);
      }
      if (status) {
        const parts = [`${seen.size} asset${seen.size === 1 ? "" : "s"} available`];
        for (const item of errors) {
          parts.push(
            `producer ${String(item.producer_dag_id || "?")}/${String(item.project_ref || "?")} failed: ${String(item.error || "")}`
          );
        }
        status.textContent = parts.join(" | ");
      }
    }

    function buildCronFromSchedulerControls() {
      if (schedulerModeState === "manual" || schedulerModeState === "asset") {
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
      const state = {
        cron_expression: cronExpression,
        timezone,
        active,
        start_date: startDate,
      };
      // F3.2b — asset trigger keys travel only when asset mode is on so
      // legacy scheduler payloads stay byte-stable.
      if (String(scheduler.trigger_type || "").trim().toLowerCase() === "asset") {
        state.trigger_type = "asset";
        state.assets = Array.isArray(scheduler.assets)
          ? scheduler.assets.map((item) => String(item || "").trim()).filter(Boolean)
          : [];
      }
      return state;
    }

    function cloneSchedulerState(state) {
      const cloned = {
        cron_expression: state && state.cron_expression ? String(state.cron_expression) : null,
        timezone: String((state && state.timezone) || SCHEDULER_DEFAULT_TIMEZONE),
        active: !!(state && state.active),
        start_date: String((state && state.start_date) || SCHEDULER_FALLBACK_START_DATE),
      };
      if (state && state.trigger_type === "asset") {
        cloned.trigger_type = "asset";
        cloned.assets = Array.isArray(state.assets) ? state.assets.slice() : [];
      }
      return cloned;
    }

    function schedulerDetailedSummaryTextFromState(state) {
      const scheduler = cloneSchedulerState(state || {});
      if (scheduler.trigger_type === "asset") {
        const count = Array.isArray(scheduler.assets) ? scheduler.assets.length : 0;
        return `Asset-triggered (${count} asset${count === 1 ? "" : "s"}, AND). Timezone: ${scheduler.timezone}. Active: ${scheduler.active ? "on" : "off"}. Start: ${scheduler.start_date}.`;
      }
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
      if (scheduler.trigger_type === "asset") {
        const count = Array.isArray(scheduler.assets) ? scheduler.assets.length : 0;
        return `Asset-triggered (${count} asset${count === 1 ? "" : "s"})`;
      }
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
      if (schedulerModeState === "asset") {
        draft.trigger_type = "asset";
        draft.assets = collectSelectedSchedulerAssets();
      }
      if (preview) {
        preview.value = schedulerModeState === "asset" ? "Asset-triggered" : (cron || "Manual");
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
      const draft = {
        cron_expression: buildCronFromSchedulerControls(),
        timezone: String(el("scheduler_timezone")?.value || "").trim() || SCHEDULER_DEFAULT_TIMEZONE,
        active: !!el("scheduler_active")?.checked,
        start_date: normalizeStartDateForPayload(el("scheduler_start_date")?.value || ""),
      };
      if (schedulerModeState === "asset") {
        draft.trigger_type = "asset";
        draft.assets = collectSelectedSchedulerAssets();
      }
      return normalizeSchedulerState(draft);
    }

    function setSchedulerFormFromState(rawScheduler) {
      const scheduler = normalizeSchedulerState(rawScheduler);
      const cronExpression = String(scheduler.cron_expression || "").trim();
      const mode = scheduler.trigger_type === "asset"
        ? "asset"
        : resolveSchedulerModeFromCron(cronExpression);
      if (mode === "asset") {
        loadDbtAssetOptions(scheduler.assets || []);
      }

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
      const hasPreferred = preferred && options.some((item) => String(item && item.dag_id || "").trim() === preferred);
      if (hasPreferred) {
        selectNode.value = preferred;
        delete card.dataset.pendingDagTaskDagId;
      } else {
        selectNode.value = "";
      }
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
      if (normalized) return normalized;
      const message = normalizeApiDetail(data && (data.message || data.msg));
      if (message) return message;
      return fallbackMessage;
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
      for (const btn of document.querySelectorAll(".btn-create-dag, #btn_update_top, #btn_promote_revision, #btn_add_task, #btn_refresh_revisions, #btn_delete_dag, #btn_cancel_delete_dag, #btn_confirm_delete_dag, #btn_cancel_scheduler_modal, #btn_apply_scheduler_modal, #btn_cancel_advanced_modal, #btn_apply_advanced_modal, #btn_add_dag_param, #btn_cancel_task_delete, #btn_confirm_task_delete, .btn-delete-task, .task-type-chip")) {
        btn.disabled = !!active;
      }
      const schedulerCompactPanel = el("scheduler_compact_panel");
      if (schedulerCompactPanel) {
        schedulerCompactPanel.classList.toggle("disabled", !!active);
        schedulerCompactPanel.setAttribute("aria-disabled", active ? "true" : "false");
      }
      const advancedCompactPanel = el("advanced_compact_panel");
      if (advancedCompactPanel) {
        advancedCompactPanel.classList.toggle("disabled", !!active);
        advancedCompactPanel.setAttribute("aria-disabled", active ? "true" : "false");
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
      if (el("advanced_modal")?.classList.contains("open")) renderAdvancedModal();
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
        closeAdvancedModal();
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
      dagParamsAppliedState = defaultDagParams();
      dagParamsDraftState = null;
      notificationsAppliedState = null;
      notificationsDraftState = null;
      dagDepsReferencedByState = [];
      renderAdvancedSummary();
      setSchedulerAppliedState({
        cron_expression: null,
        timezone: resolveBrowserTimezone() || SCHEDULER_DEFAULT_TIMEZONE,
        active: true,
        start_date: nowDateTimeIsoSecondsLocal(),
      });
      setSchedulerFormFromState(schedulerAppliedState);
      closeSchedulerModal();
      resetEngineConfig();
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
        ? "Select Source Connection"
        : "Select Target Connection";
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
    let airflowVariableQuery = "";

    function normalizeAirflowVariableKeys(items) {
      return Array.from(
        new Set((items || []).map((x) => String(x || "").trim()).filter(Boolean))
      ).sort().slice(0, 50);
    }

    async function fetchAirflowVariableKeys(search = "", exact = false, limit = 50) {
      const query = String(search || "").trim();
      const params = new URLSearchParams({ limit: String(Math.min(50, Math.max(1, limit))) });
      if (query) params.set("q", query);
      if (exact) params.set("exact", "true");
      const r = await studioFetch(`/api/airflow-variables?${params.toString()}`);
      const data = await parseJsonSafe(r);
      if (!r.ok || !data.ok) {
        throw new Error("Airflow Variable service is unavailable.");
      }
      return normalizeAirflowVariableKeys(data.items || []);
    }

    async function loadAirflowVariables(search = "") {
      const query = String(search || "").trim();
      const items = await fetchAirflowVariableKeys(query, false, 50);
      airflowVariableQuery = query;
      airflowVariableKeys = items;
      return items;
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
      if (sourceType === "sql") {
        // A SQL query has no table: its partition-column candidates are the
        // query's SELECT columns, which are the mapping's source_name entries
        // (persisted in the card's .mapping-content, modal-independent). The
        // engine partitions inline SQL by wrapping it as a subquery.
        const mappingContent = String(card.querySelector(".mapping-content")?.value || "");
        const names = parseMappingSourceColumns(mappingContent);
        if (names.length) {
          setPartitionColumnOptions(selectNode, names, preferred, "Select source column");
          delete card.dataset.pendingPartitionColumn;
        } else {
          setPartitionColumnOptions(selectNode, [], "", "Generate the mapping first to choose a partition column.");
        }
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
      return parts.length ? parts.join("/") : "";
    }

    function requireFolderSelection() {
      const values = {
        project: el("project").value.trim(),
        domain: el("domain").value.trim(),
        level: el("level").value.trim(),
        flow: el("flow").value.trim(),
      };
      if (!Object.values(values).every(Boolean)) {
        throw new Error(`${FOLDER_PATH_PROMPT}.`);
      }
      return values;
    }

    function validateFolderSelectionBeforeSubmit() {
      try {
        requireFolderSelection();
        return true;
      } catch (err) {
        const message = String(err && err.message ? err.message : FOLDER_PATH_PROMPT);
        setUpdateModeStatus(message, "warn");
        pushToast(message, "error", true);
        return false;
      }
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
      folderPathInput.title = folderPathValue || FOLDER_PATH_PROMPT;
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
      el("folder_picker_summary").textContent = getFolderPathText(pickerDraft) || "-";
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

        allConnectionsState = Array.isArray(items) ? items : [];
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

    function customDagParameterNames() {
      return normalizeDagParams(dagParamsAppliedState || defaultDagParams())
        .map((item) => String(item.name || "").trim())
        .filter((name) => name && name !== "log_level");
    }

    function selectedDagBindingNames(card, excludedRow) {
      const selected = new Set();
      for (const row of getBindingRows(card)) {
        if (row === excludedRow) continue;
        const name = String(
          row.querySelector(".binding-variable-name-select")?.value || ""
        ).trim();
        if (name) selected.add(name);
      }
      return selected;
    }

    function refreshBindingVariableControl(row) {
      const card = row.closest(".task-card");
      const isDagBinding = card?.querySelector(".task-type")?.value === TASK_TYPES.BINDING;
      const input = row.querySelector(".binding-variable-name");
      const select = row.querySelector(".binding-variable-name-select");
      const current = String(select.value || input.value || "").trim();
      input.classList.toggle("hidden", isDagBinding);
      input.disabled = isDagBinding;
      select.classList.toggle("hidden", !isDagBinding);
      select.disabled = !isDagBinding;
      if (!isDagBinding) return;

      const selectedByOtherRows = selectedDagBindingNames(card, row);
      select.replaceChildren();
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = "Select DAG parameter";
      select.appendChild(placeholder);
      for (const name of customDagParameterNames()) {
        const option = document.createElement("option");
        option.value = name;
        option.textContent = name;
        option.disabled = selectedByOtherRows.has(name);
        select.appendChild(option);
      }
      if (current && !customDagParameterNames().includes(current)) {
        const legacy = document.createElement("option");
        legacy.value = current;
        legacy.textContent = `${current} (not declared)`;
        select.appendChild(legacy);
      }
      select.value = current;
    }

    function refreshBindingVariableControls(card) {
      for (const row of getBindingRows(card)) {
        refreshBindingVariableControl(row);
      }
    }

    function refreshDagParameterBindingControls() {
      for (const card of getTaskCards()) {
        const isDagBinding = card.querySelector(".task-type")?.value === TASK_TYPES.BINDING;
        const title = card.querySelector(".bindings-title");
        if (title) title.textContent = isDagBinding ? "DAG Parameter Bindings" : "Bindings";
        refreshBindingVariableControls(card);
        for (const row of getBindingRows(card)) {
          syncBindingRowState(row);
        }
      }
    }

    function setAirflowVariableValidation(row, state = "", message = "") {
      const input = row.querySelector(".binding-airflow-variable-key");
      const note = row.querySelector(".binding-airflow-validation");
      row.dataset.airflowVariableValidation = state;
      input.setAttribute("aria-invalid", state === "invalid" || state === "unavailable" ? "true" : "false");
      note.textContent = message;
      note.classList.toggle("hidden", !message);
      note.classList.toggle("warn", state === "invalid" || state === "unavailable");
    }

    function closeAirflowVariableSelector(row) {
      const input = row.querySelector(".binding-airflow-variable-key");
      const options = row.querySelector(".binding-airflow-options");
      options.classList.add("hidden");
      input.setAttribute("aria-expanded", "false");
      row.dataset.airflowVariableActiveIndex = "-1";
    }

    function setAirflowVariableActiveOption(row, index) {
      const options = Array.from(row.querySelectorAll(".binding-airflow-option"));
      if (!options.length) return;
      const safeIndex = ((index % options.length) + options.length) % options.length;
      options.forEach((option, optionIndex) => {
        const active = optionIndex === safeIndex;
        option.classList.toggle("active", active);
        option.setAttribute("aria-selected", active ? "true" : "false");
      });
      options[safeIndex].scrollIntoView({ block: "nearest" });
      row.dataset.airflowVariableActiveIndex = String(safeIndex);
    }

    function selectAirflowVariableOption(row, key) {
      const input = row.querySelector(".binding-airflow-variable-key");
      input.value = String(key || "");
      setAirflowVariableValidation(row, "valid", "");
      closeAirflowVariableSelector(row);
    }

    function renderAirflowVariableOptions(row, items) {
      const options = row.querySelector(".binding-airflow-options");
      options.replaceChildren();
      const keys = normalizeAirflowVariableKeys(items);
      if (!keys.length) {
        const empty = document.createElement("div");
        empty.className = "binding-airflow-empty muted-note";
        empty.textContent = "No matching Airflow Variable key.";
        options.appendChild(empty);
      } else {
        keys.forEach((key) => {
          const option = document.createElement("button");
          option.type = "button";
          option.className = "binding-airflow-option";
          option.setAttribute("role", "option");
          option.setAttribute("aria-selected", "false");
          option.textContent = key;
          option.addEventListener("mousedown", (event) => event.preventDefault());
          option.addEventListener("click", () => selectAirflowVariableOption(row, key));
          options.appendChild(option);
        });
      }
      options.classList.remove("hidden");
      row.querySelector(".binding-airflow-variable-key").setAttribute("aria-expanded", "true");
      row.dataset.airflowVariableActiveIndex = "-1";
    }

    async function openAirflowVariableSelector(row) {
      const input = row.querySelector(".binding-airflow-variable-key");
      const query = String(input.value || "").trim();
      try {
        const items = query === airflowVariableQuery
          ? airflowVariableKeys
          : await loadAirflowVariables(query);
        renderAirflowVariableOptions(row, items);
      } catch (_err) {
        setAirflowVariableValidation(
          row,
          "unavailable",
          "Airflow Variable service is unavailable; this binding cannot be saved."
        );
        closeAirflowVariableSelector(row);
      }
    }

    async function validateAirflowVariableKey(row) {
      const source = row.querySelector(".binding-source").value;
      if (source !== "airflow_variable") return true;
      const input = row.querySelector(".binding-airflow-variable-key");
      const key = String(input.value || "").trim();
      input.value = key;
      if (!key) {
        setAirflowVariableValidation(row, "invalid", "Enter an existing Airflow Variable key.");
        return false;
      }

      const requestId = Number(row.dataset.airflowVariableValidationRequest || "0") + 1;
      row.dataset.airflowVariableValidationRequest = String(requestId);
      setAirflowVariableValidation(row, "pending", "Validating Airflow Variable key...");
      try {
        const items = await fetchAirflowVariableKeys(key, true, 1);
        if (Number(row.dataset.airflowVariableValidationRequest) !== requestId) return false;
        if (items.includes(key)) {
          setAirflowVariableValidation(row, "valid", "");
          return true;
        }
        setAirflowVariableValidation(
          row,
          "invalid",
          `Airflow Variable '${key}' no longer exists.`
        );
      } catch (_err) {
        if (Number(row.dataset.airflowVariableValidationRequest) !== requestId) return false;
        setAirflowVariableValidation(
          row,
          "unavailable",
          "Airflow Variable service is unavailable; this binding cannot be saved."
        );
      }
      return false;
    }

    async function validateAllAirflowVariableBindings() {
      const rows = Array.from(document.querySelectorAll(".binding-item")).filter(
        (row) => row.querySelector(".binding-source")?.value === "airflow_variable"
      );
      for (const row of rows) {
        if (!(await validateAirflowVariableKey(row))) {
          row.querySelector(".binding-airflow-variable-key")?.focus();
          const note = row.querySelector(".binding-airflow-validation")?.textContent;
          throw new Error(note || "Airflow Variable key validation failed.");
        }
      }
    }

    function expressionNamespaceRefs(expression) {
      const text = String(expression || "");
      const dag = Array.from(text.matchAll(/\{\{\s*dag\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}/g), (match) => match[1]);
      const airflow = Array.from(text.matchAll(/\{\{\s*airflow\.([^\s{}]+)\s*\}\}/g), (match) => match[1]);
      const local = Array.from(text.matchAll(/\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}/g), (match) => match[1]);
      const legacyAirflow = Array.from(text.matchAll(/\{\{\s*airflow_var\.([^\s{}]+)\s*\}\}/g), (match) => match[1]);
      return {
        dag: new Set(dag),
        airflow: new Set(airflow),
        local: new Set(local),
        legacyAirflow: new Set(legacyAirflow),
      };
    }

    // F3.2 — per-type text scanned for {{ p }} / {{ dag.p }} references.
    // Mirror of dag_param_flow._reference_expression: every task type that
    // consumes DAG parameters MUST contribute its expression source here.
    function taskParamExpression(task) {
      if (task.task_type === TASK_TYPES.SCRIPT_RUN) return task.script_sql;
      if (task.task_type === TASK_TYPES.DBT) {
        const vars = task.dbt_vars || {};
        return Object.keys(vars).sort().map((key) => String(vars[key])).join("\n");
      }
      return task.where;
    }

    function cardParamExpression(card) {
      const taskType = card.querySelector(".task-type")?.value;
      if (taskType === TASK_TYPES.SCRIPT_RUN) return card.querySelector(".script-sql")?.value;
      if (taskType === TASK_TYPES.DBT) return card.querySelector(".dbt-vars")?.value;
      return card.querySelector(".where")?.value;
    }

    async function validateAirflowNamespaceKeys() {
      const keys = new Set();
      for (const card of getTaskCards()) {
        const expression = cardParamExpression(card);
        for (const key of expressionNamespaceRefs(expression).airflow) keys.add(key);
      }
      for (const key of keys) {
        let items;
        try {
          items = await fetchAirflowVariableKeys(key, true, 1);
        } catch (_err) {
          throw new Error(
            `Airflow Variable service is unavailable; '${key}' cannot be validated.`
          );
        }
        if (!items.includes(key)) {
          throw new Error(`Airflow Variable '${key}' no longer exists.`);
        }
      }
    }

    function compileDagParameterFlow(payload, customNames) {
      const triggerSource = "__dag_run_conf__";
      const ambiguousSource = "__ambiguous__";
      const tasks = Array.isArray(payload.flow_tasks) ? payload.flow_tasks : [];
      const taskById = new Map(tasks.map((task) => [task.task_group_id, task]));
      if (taskById.size !== tasks.length || taskById.has("")) {
        throw new Error("Task IDs must be non-empty and unique.");
      }
      const parents = new Map();
      const children = new Map(tasks.map((task) => [task.task_group_id, []]));
      const remaining = new Map();
      for (const task of tasks) {
        const upstream = Array.from(new Set(task.depends_on || []));
        for (const parentId of upstream) {
          if (!taskById.has(parentId) || parentId === task.task_group_id) {
            throw new Error(`Invalid task dependency: ${parentId}.`);
          }
          children.get(parentId).push(task.task_group_id);
        }
        parents.set(task.task_group_id, upstream);
        remaining.set(task.task_group_id, upstream.length);
      }
      const ready = tasks
        .map((task) => task.task_group_id)
        .filter((taskId) => remaining.get(taskId) === 0);
      const ordered = [];
      while (ready.length) {
        const taskId = ready.shift();
        ordered.push(taskId);
        for (const childId of children.get(taskId)) {
          remaining.set(childId, remaining.get(childId) - 1);
          if (remaining.get(childId) === 0) ready.push(childId);
        }
      }
      if (ordered.length !== tasks.length) throw new Error("Task dependencies contain a cycle.");

      const outputs = new Map();
      for (const taskId of ordered) {
        const task = taskById.get(taskId);
        const upstream = parents.get(taskId);
        const incoming = new Map();
        for (const name of customNames) {
          if (!upstream.length) {
            incoming.set(name, triggerSource);
            continue;
          }
          const sources = new Set(upstream.map((parentId) => outputs.get(parentId).get(name)));
          incoming.set(name, sources.size === 1 ? Array.from(sources)[0] : ambiguousSource);
        }
        if (task.task_type !== TASK_TYPES.BINDING) {
          const expression = taskParamExpression(task);
          const ambiguous = Array.from(expressionNamespaceRefs(expression).dag)
            .filter((name) => incoming.get(name) === ambiguousSource);
          if (ambiguous.length) {
            throw new Error(`Ambiguous DAG parameter source at task '${taskId}': ${ambiguous.join(", ")}.`);
          }
        }
        const outgoing = new Map(incoming);
        if (task.task_type === TASK_TYPES.BINDING) {
          for (const binding of task.bindings || []) {
            outgoing.set(String(binding.variable_name || "").trim(), taskId);
          }
        }
        outputs.set(taskId, outgoing);
      }
    }

    function validateUniqueTaskBindingNames(tasks) {
      for (const task of tasks) {
        const names = new Set();
        const taskId = String(task.task_group_id || "(unnamed)").trim();
        for (const binding of task.bindings || []) {
          const name = String(binding.variable_name || "").trim();
          if (!name) continue;
          if (names.has(name)) {
            throw new Error(
              `Binding '${name}' is defined more than once in task '${taskId}'.`
            );
          }
          names.add(name);
        }
      }
    }

    function validateDagParameterPayload(payload) {
      validateUniqueTaskBindingNames(payload.flow_tasks || []);
      const params = Array.isArray(payload.dag_params) ? payload.dag_params : [];
      const customNames = new Set(
        params.map((item) => String(item.name || "").trim()).filter((name) => name && name !== "log_level")
      );
      const paramTypes = new Map(
        params.map((item) => [String(item.name || "").trim(), String(item.type || "string")])
      );
      for (const task of payload.flow_tasks || []) {
        if (task.task_type !== TASK_TYPES.BINDING) continue;
        for (const binding of task.bindings || []) {
          const name = String(binding.variable_name || "").trim();
          if (!["source", "target", "default"].includes(binding.binding_source)) {
            throw new Error("DAG Parameter Bindings support Source, Target, or Default.");
          }
          if (!customNames.has(name)) {
            throw new Error(`Binding target is not a declared custom DAG parameter: ${name || "(empty)"}.`);
          }
          if (binding.binding_source === "default") {
            const value = String(binding.default_value || "").trim();
            const type = paramTypes.get(name) || "string";
            const valid = type === "string"
              ? !!value
              : (type === "integer"
                ? /^-?\d+$/.test(value)
                : (type === "number"
                  ? value !== "" && Number.isFinite(Number(value))
                  : ["true", "false"].includes(value.toLowerCase())));
            if (!valid) throw new Error(`Default binding for '${name}' does not match type ${type}.`);
          }
        }
      }
      for (const task of payload.flow_tasks || []) {
        if (task.task_type === TASK_TYPES.BINDING) continue;
        const expression = taskParamExpression(task);
        const refs = expressionNamespaceRefs(expression);
        if (refs.legacyAirflow.size) {
          const key = Array.from(refs.legacyAirflow)[0];
          throw new Error(`Replace {{ airflow_var.${key} }} with {{ airflow.${key} }}.`);
        }
        const localNames = new Set((task.bindings || []).map((item) => item.variable_name));
        for (const name of refs.local) {
          if (!localNames.has(name) && !customNames.has(name)) {
            throw new Error(`Task-local binding '${name}' is not defined.`);
          }
          if (!localNames.has(name) && customNames.has(name)) {
            throw new Error(`Replace legacy {{ ${name} }} with {{ dag.${name} }}.`);
          }
        }
        for (const name of refs.dag) {
          if (!customNames.has(name)) throw new Error(`DAG parameter '${name}' is not declared.`);
        }
      }
      compileDagParameterFlow(payload, customNames);
    }

    function syncBindingRowState(row) {
      const sourceSelect = row.querySelector(".binding-source");
      const bindingRow = row.querySelector(".binding-row");
      const defaultWrap = row.querySelector(".binding-default-wrap");
      const defaultInput = row.querySelector(".binding-default-value");
      const sqlWrap = row.querySelector(".binding-sql-wrap");
      const sqlInput = row.querySelector(".binding-sql");
      const airflowWrap = row.querySelector(".binding-airflow-wrap");
      const airflowInput = row.querySelector(".binding-airflow-variable-key");
      const card = row.closest(".task-card");
      const isDagBinding = card?.querySelector(".task-type")?.value === TASK_TYPES.BINDING;
      const airflowOption = sourceSelect.querySelector('option[value="airflow_variable"]');
      const source = sourceSelect.value;

      const isDefault = source === "default";
      const isSqlSource = source === "source" || source === "target";
      const isAirflowVariable = source === "airflow_variable";

      if (airflowOption) {
        airflowOption.hidden = isDagBinding;
        airflowOption.disabled = isDagBinding && !isAirflowVariable;
      }

      defaultInput.disabled = !isDefault;
      sqlInput.disabled = !isSqlSource;
      airflowInput.disabled = !isAirflowVariable;
      bindingRow.classList.toggle("has-default-value", isDefault);
      defaultWrap.classList.toggle("hidden", !isDefault);
      sqlWrap.classList.toggle("hidden", !isSqlSource);
      airflowWrap.classList.toggle("hidden", !isAirflowVariable);

      if (!isDefault) defaultInput.value = "";
      if (!isSqlSource) sqlInput.value = "";
      if (!isAirflowVariable) {
        airflowInput.value = "";
        setAirflowVariableValidation(row, "", "");
        closeAirflowVariableSelector(row);
      }
    }

    function createBindingRow(card, values = {}) {
      const list = card.querySelector(".bindings-list");
      const row = document.createElement("div");
      row.className = "binding-item";
      row.innerHTML = `
        <div class="binding-row">
          <input class="binding-variable-name" placeholder="variable_name">
          <select class="binding-variable-name-select hidden" aria-label="DAG parameter"></select>
          <select class="binding-source">
            <option value="target">Target</option>
            <option value="source">Source</option>
            <option value="default">Default</option>
            <option value="airflow_variable">Airflow Variable</option>
          </select>
          <div class="binding-default-wrap hidden">
            <input class="binding-default-value" placeholder="Default">
          </div>
          <button class="btn btn-danger binding-remove" type="button">x</button>
        </div>
        <label class="binding-sql-wrap hidden">
          SQL
          <textarea class="binding-sql" rows="3" placeholder="SELECT ..."></textarea>
        </label>
        <label class="binding-airflow-wrap hidden">
          Airflow Variable
          <div class="binding-airflow-selector">
            <input class="binding-airflow-variable-key" role="combobox" aria-autocomplete="list" aria-expanded="false" aria-controls="" autocomplete="off" placeholder="Select or enter variable key">
            <div class="binding-airflow-options hidden" role="listbox"></div>
          </div>
          <div class="binding-airflow-validation muted-note hidden" role="status"></div>
        </label>
      `;
      row.querySelector(".binding-variable-name").value = values.variable_name || "";
      const variableSelect = row.querySelector(".binding-variable-name-select");
      variableSelect.dataset.initialValue = values.variable_name || "";
      row.querySelector(".binding-source").value = values.binding_source || "target";
      row.querySelector(".binding-default-value").value = values.default_value || "";
      row.querySelector(".binding-sql").value = values.sql || "";
      row.querySelector(".binding-airflow-variable-key").value = values.airflow_variable_key || "";

      const airflowInput = row.querySelector(".binding-airflow-variable-key");
      const optionsId = `binding_airflow_options_${Math.random().toString(36).slice(2)}`;
      row.querySelector(".binding-airflow-options").id = optionsId;
      airflowInput.setAttribute("aria-controls", optionsId);

      row.querySelector(".binding-source").addEventListener("change", () => {
        syncBindingRowState(row);
        if (row.querySelector(".binding-source").value === "airflow_variable") {
          if (document.activeElement === airflowInput) {
            openAirflowVariableSelector(row);
          } else {
            airflowInput.focus();
          }
        }
      });
      variableSelect.addEventListener("change", () => {
        refreshBindingVariableControls(card);
      });
      row.querySelector(".binding-remove").addEventListener("click", () => {
        row.remove();
        refreshBindingVariableControls(card);
        updateBindingsVisibility(card);
      });
      airflowInput.addEventListener("focus", () => openAirflowVariableSelector(row));
      airflowInput.addEventListener("input", () => {
        row.dataset.airflowVariableValidationRequest = String(
          Number(row.dataset.airflowVariableValidationRequest || "0") + 1
        );
        setAirflowVariableValidation(row, "", "");
        clearTimeout(row.airflowVariableSearchTimer);
        row.airflowVariableSearchTimer = setTimeout(() => openAirflowVariableSelector(row), 180);
      });
      airflowInput.addEventListener("blur", () => {
        clearTimeout(row.airflowVariableSearchTimer);
        setTimeout(() => closeAirflowVariableSelector(row), 120);
        validateAirflowVariableKey(row);
      });
      airflowInput.addEventListener("keydown", (event) => {
        const options = Array.from(row.querySelectorAll(".binding-airflow-option"));
        const current = Number(row.dataset.airflowVariableActiveIndex || "-1");
        if (event.key === "ArrowDown" || event.key === "ArrowUp") {
          event.preventDefault();
          if (row.querySelector(".binding-airflow-options").classList.contains("hidden")) {
            openAirflowVariableSelector(row);
            return;
          }
          setAirflowVariableActiveOption(row, current + (event.key === "ArrowDown" ? 1 : -1));
        } else if (event.key === "Enter" && current >= 0 && options[current]) {
          event.preventDefault();
          selectAirflowVariableOption(row, options[current].textContent);
        } else if (event.key === "Escape") {
          event.preventDefault();
          closeAirflowVariableSelector(row);
        }
      });

      list.appendChild(row);
      variableSelect.value = values.variable_name || "";
      refreshBindingVariableControls(card);
      syncBindingRowState(row);
      updateBindingsVisibility(card);
      if (row.querySelector(".binding-source").value === "airflow_variable") {
        validateAirflowVariableKey(row);
      }
    }

    function ensureBindingRowForBindingTask(card) {
      const taskType = String(card.querySelector(".task-type")?.value || "").trim();
      if (taskType === TASK_TYPES.BINDING && !getBindingRows(card).length) {
        createBindingRow(card, {});
      }
    }

    function setBindingsFromValues(card, bindings) {
      const list = card.querySelector(".bindings-list");
      list.innerHTML = "";
      const items = Array.isArray(bindings) ? bindings : [];
      for (const binding of items) {
        createBindingRow(card, binding || {});
      }
      refreshBindingVariableControls(card);
      updateBindingsVisibility(card);
    }

    function bindBindingsSection(card) {
      const addButton = card.querySelector(".btn-binding-add");
      addButton.addEventListener("click", () => createBindingRow(card, {}));
      updateBindingsVisibility(card);
    }

    function normalizeUpsertMatchColumns(rawValues) {
      const values = Array.isArray(rawValues) ? rawValues : [];
      const out = [];
      const seen = new Set();
      for (const raw of values) {
        const col = String(raw || "").trim();
        if (!col || seen.has(col)) continue;
        seen.add(col);
        out.push(col);
        if (out.length >= UPSERT_MATCH_MAX_COUNT) break;
      }
      return out;
    }

    function parseMappingTargetColumns(mappingContent) {
      const text = String(mappingContent || "");
      if (!text.trim()) return [];
      const out = [];
      const seen = new Set();
      const lines = text.split(/\r?\n/);
      for (const line of lines) {
        const match = line.match(/^\s*target_name\s*:\s*(.+)\s*$/);
        if (!match) continue;
        let value = String(match[1] || "").trim();
        value = value.replace(/^['"]|['"]$/g, "").trim();
        if (!value || seen.has(value)) continue;
        seen.add(value);
        out.push(value);
      }
      return out;
    }

    // Source (SELECT) columns of a mapping = the Direct rows' source_name. These
    // are the columns present in a SQL source's output, hence the valid
    // partition columns. The `-?` tolerates both YAML shapes: client-serialized
    // (`  source_name:`) and server-dumped (`- source_name:` first key).
    function parseMappingSourceColumns(mappingContent) {
      const text = String(mappingContent || "");
      if (!text.trim()) return [];
      const out = [];
      const seen = new Set();
      for (const line of text.split(/\r?\n/)) {
        const match = line.match(/^\s*-?\s*source_name\s*:\s*(.+)\s*$/);
        if (!match) continue;
        let value = String(match[1] || "").trim();
        value = value.replace(/^['"]|['"]$/g, "").trim();
        if (!value || seen.has(value)) continue;
        seen.add(value);
        out.push(value);
      }
      return out;
    }

    function setUpsertMatchOptions(card, options) {
      const datalistNode = card.querySelector(".upsert-match-options");
      if (!datalistNode) return;
      const normalized = Array.from(
        new Set(
          (Array.isArray(options) ? options : [])
            .map((x) => String(x || "").trim())
            .filter(Boolean)
        )
      ).sort((a, b) => a.localeCompare(b));
      datalistNode.innerHTML = "";
      for (const col of normalized) {
        const opt = document.createElement("option");
        opt.value = col;
        datalistNode.appendChild(opt);
      }
    }

    function setUpsertMatchState(card, values) {
      const normalized = normalizeUpsertMatchColumns(values);
      card.dataset.upsertMatchColumns = JSON.stringify(normalized);
      renderUpsertMatchChips(card);
    }

    function getUpsertMatchState(card) {
      try {
        const parsed = JSON.parse(String(card.dataset.upsertMatchColumns || "[]"));
        return normalizeUpsertMatchColumns(parsed);
      } catch (_err) {
        return [];
      }
    }

    function setUpsertMatchNote(card, message, isError = false) {
      const note = card.querySelector(".upsert-match-note");
      if (!note) return;
      note.textContent = String(message || "");
      note.classList.toggle("warn", !!isError);
      note.classList.toggle("ok", !!message && !isError);
    }

    function renderUpsertMatchChips(card) {
      const chipsWrap = card.querySelector(".upsert-match-chips");
      if (!chipsWrap) return;
      const values = getUpsertMatchState(card);
      chipsWrap.innerHTML = "";
      for (const col of values) {
        const chip = document.createElement("span");
        chip.className = "tag-chip";
        chip.textContent = col;
        const remove = document.createElement("button");
        remove.type = "button";
        remove.className = "tag-chip-remove";
        remove.textContent = "x";
        remove.title = `Remove match column: ${col}`;
        remove.disabled = !!isBusy;
        remove.addEventListener("click", () => {
          setUpsertMatchState(card, values.filter((item) => item !== col));
          syncUpsertMatchState(card);
        });
        chip.appendChild(remove);
        chipsWrap.appendChild(chip);
      }
    }

    function addUpsertMatchColumn(card, rawValue) {
      const value = String(rawValue || "").trim();
      if (!value) return;
      const next = normalizeUpsertMatchColumns([...getUpsertMatchState(card), value]);
      setUpsertMatchState(card, next);
      syncUpsertMatchState(card);
    }

    async function loadUpsertMatchOptions(card) {
      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim() || TASK_TYPES.SOURCE_TARGET;
      const loadMethod = String(card.querySelector(".load-method")?.value || "").trim();
      if (taskType !== TASK_TYPES.SOURCE_TARGET || loadMethod !== "upsert") {
        return;
      }

      const sourceType = String(card.querySelector(".source-type")?.value || "table").trim() || "table";
      const sourceConnId = String(el("source_conn_id")?.value || "").trim();
      const targetConnId = String(el("target_conn_id")?.value || "").trim();
      const sourceSchema = String(card.querySelector(".source-schema")?.value || "").trim();
      const sourceTable = normalizeRelationIdentifier(card.querySelector(".source-table")?.value || "");
      const targetSchema = String(card.querySelector(".target-schema")?.value || "").trim();
      const targetTable = normalizeRelationIdentifier(card.querySelector(".target-table")?.value || "");
      const mappingMode = String(card.querySelector(".column-mapping-mode")?.value || "source").trim();
      const mappingContent = String(card.querySelector(".mapping-content")?.value || "");

      const requestKey = [
        sourceType,
        sourceConnId,
        sourceSchema,
        sourceTable,
        targetConnId,
        targetSchema,
        targetTable,
        mappingMode,
        mappingContent.length,
      ].join("|").toLowerCase();
      card.dataset.upsertMatchRequestKey = requestKey;

      const candidates = [];

      if (mappingMode === "mapping_file") {
        for (const col of parseMappingTargetColumns(mappingContent)) {
          candidates.push(col);
        }
      } else if ((sourceType === "table" || sourceType === "view") && sourceConnId && sourceSchema && sourceTable) {
        try {
          const srcResp = await studioFetch(
            `/api/columns?conn_id=${encodeURIComponent(sourceConnId)}&schema=${encodeURIComponent(sourceSchema)}&table=${encodeURIComponent(sourceTable)}`
          );
          const srcData = await parseJsonSafe(srcResp);
          if (srcResp.ok && srcData.ok) {
            for (const item of Array.isArray(srcData.items) ? srcData.items : []) {
              const name = String(item && item.name ? item.name : "").trim();
              if (name) candidates.push(name);
            }
          }
        } catch (_err) {
          // no-op
        }
      }

      if (targetConnId && targetSchema && targetTable) {
        try {
          const tgtResp = await studioFetch(
            `/api/columns?conn_id=${encodeURIComponent(targetConnId)}&schema=${encodeURIComponent(targetSchema)}&table=${encodeURIComponent(targetTable)}`
          );
          const tgtData = await parseJsonSafe(tgtResp);
          if (tgtResp.ok && tgtData.ok) {
            for (const item of Array.isArray(tgtData.items) ? tgtData.items : []) {
              const name = String(item && item.name ? item.name : "").trim();
              if (name) candidates.push(name);
            }
          }
        } catch (_err) {
          // no-op
        }
      }

      if (card.dataset.upsertMatchRequestKey !== requestKey) return;
      setUpsertMatchOptions(card, candidates);
    }

    function refreshAllUpsertMatchOptions() {
      for (const card of getTaskCards()) {
        loadUpsertMatchOptions(card);
      }
    }

    function syncUpsertMatchState(card) {
      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim() || TASK_TYPES.SOURCE_TARGET;
      const loadMethod = String(card.querySelector(".load-method")?.value || "").trim();
      const wrap = card.querySelector(".upsert-match-wrap");
      const input = card.querySelector(".upsert-match-input");
      if (!wrap || !input) return;

      const isVisible = taskType === TASK_TYPES.SOURCE_TARGET && loadMethod === "upsert";
      wrap.classList.toggle("hidden", !isVisible);
      input.disabled = !isVisible;
      input.setAttribute("aria-disabled", isVisible ? "false" : "true");

      if (!isVisible) {
        setUpsertMatchNote(card, "", false);
        return;
      }

      const selected = getUpsertMatchState(card);
      if (!selected.length) {
        setUpsertMatchNote(card, "Select at least one match column for upsert.", true);
      } else {
        setUpsertMatchNote(card, `${selected.length} match column selected.`, false);
      }
      loadUpsertMatchOptions(card);
    }

    function syncTaskTypeState(card) {
      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim() || TASK_TYPES.SOURCE_TARGET;
      syncTaskTypeSegment(card);
      card.classList.toggle("single-pane-task", taskType !== TASK_TYPES.SOURCE_TARGET);
      card.classList.toggle("binding-task", taskType === TASK_TYPES.BINDING);
      const sourceTargetFields = card.querySelector(".source-target-fields");
      const scriptRunFields = card.querySelector(".script-run-fields");
      const dagTaskFields = card.querySelector(".dag-task-fields");
      const dbtFields = card.querySelector(".dbt-fields");
      const sourceCard = card.querySelector(".source-card");
      const targetCard = card.querySelector(".target-card");
      const whereClauseWrap = card.querySelector(".where-clause-wrap");
      const whereInput = card.querySelector(".where");
      const filterTabButton = card.querySelector(".filter-tab-button");

      sourceTargetFields?.classList.toggle("hidden", taskType !== TASK_TYPES.SOURCE_TARGET);
      scriptRunFields?.classList.toggle("hidden", taskType !== TASK_TYPES.SCRIPT_RUN);
      dagTaskFields?.classList.toggle("hidden", taskType !== TASK_TYPES.DAG);
      dbtFields?.classList.toggle("hidden", taskType !== TASK_TYPES.DBT);
      sourceCard?.classList.toggle("hidden", taskType === TASK_TYPES.BINDING);
      targetCard?.classList.toggle("hidden", taskType !== TASK_TYPES.SOURCE_TARGET);
      whereClauseWrap?.classList.toggle("hidden", taskType !== TASK_TYPES.SOURCE_TARGET);
      if (whereInput) whereInput.disabled = taskType !== TASK_TYPES.SOURCE_TARGET;
      if (filterTabButton) {
        filterTabButton.textContent = taskType === TASK_TYPES.BINDING ? "Parameters" : "Filter & Bindings";
      }
      const bindingsTitle = card.querySelector(".bindings-title");
      if (bindingsTitle) {
        bindingsTitle.textContent = taskType === TASK_TYPES.BINDING
          ? "DAG Parameter Bindings"
          : "Bindings";
      }
      refreshBindingVariableControls(card);
      for (const row of getBindingRows(card)) {
        syncBindingRowState(row);
      }

      const modeSelect = card.querySelector(".dependency-mode");
      const tabButtons = Array.from(card.querySelectorAll(".tab-btn"));
      const panels = Array.from(card.querySelectorAll(".tab-panel"));
      const allowAllTabs = taskType === TASK_TYPES.SOURCE_TARGET;
      const allowScriptFilterTab = taskType === TASK_TYPES.SCRIPT_RUN;
      const allowBindingFilterTab = taskType === TASK_TYPES.BINDING;
      for (const btn of tabButtons) {
        const tabId = String(btn.getAttribute("data-tab") || "");
        const keep = allowAllTabs || tabId === "dependencies" || ((allowScriptFilterTab || allowBindingFilterTab) && tabId === "filter");
        btn.classList.toggle("hidden", !keep);
      }
      for (const panel of panels) {
        const panelId = String(panel.getAttribute("data-tab-panel") || "");
        const keep = allowAllTabs || panelId === "dependencies" || ((allowScriptFilterTab || allowBindingFilterTab) && panelId === "filter");
        panel.classList.toggle("hidden", !keep);
      }

      if (!allowAllTabs) {
        const fallbackTab = (allowScriptFilterTab || allowBindingFilterTab) ? "filter" : "dependencies";
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
      for (const selector of [".dbt-project-ref", ".dbt-command", ".dbt-select", ".dbt-target", ".dbt-threads", ".dbt-vars"]) {
        const input = card.querySelector(selector);
        if (input) input.disabled = taskType !== TASK_TYPES.DBT;
      }

      if (taskType === TASK_TYPES.DAG) {
        refreshDagTaskOptions(card);
      }
      if (modeSelect && taskType !== TASK_TYPES.SOURCE_TARGET) {
        modeSelect.disabled = false;
      }
      syncUpsertMatchState(card);
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
      const dbtProjectRefInput = card.querySelector(".dbt-project-ref");
      if (dbtProjectRefInput) {
        dbtProjectRefInput.value = values.dbt_project_ref || "";
        card.querySelector(".dbt-command").value = values.dbt_command || "run";
        card.querySelector(".dbt-select").value = values.dbt_select || "";
        card.querySelector(".dbt-target").value = values.dbt_target || "";
        card.querySelector(".dbt-threads").value = values.dbt_threads ? String(values.dbt_threads) : "";
        const dbtVars = values.dbt_vars && typeof values.dbt_vars === "object" ? values.dbt_vars : null;
        card.querySelector(".dbt-vars").value = dbtVars && Object.keys(dbtVars).length
          ? JSON.stringify(dbtVars, null, 2)
          : "";
        const dbtExecution = card.querySelector(".dbt-execution");
        if (dbtExecution) {
          dbtExecution.value = values.dbt_execution === "task" ? "task" : "cosmos";
          const behaviorSel = card.querySelector(".dbt-test-behavior");
          if (behaviorSel) behaviorSel.value = values.dbt_test_behavior || "";
          const emitBox = card.querySelector(".dbt-emit-datasets");
          if (emitBox) emitBox.checked = values.emit_datasets === true;
          const platformSel = card.querySelector(".dbt-target-platform");
          if (platformSel) platformSel.value = values.dbt_target_platform || "";
          syncDbtExecutionControls(card);
        }
      }
      card.querySelector(".target-schema").value = values.target_schema || "";
      card.querySelector(".target-table").value = values.target_table || "";
      card.querySelector(".load-method").value = values.load_method || "create_if_not_exists_or_truncate";
      hydrateFileEndpointValues(card, values);
      setUpsertMatchState(card, values.upsert_match_columns || []);
      card.querySelector(".column-mapping-mode").value = values.column_mapping_mode || "source";
      card.querySelector(".mapping-content").value = values.mapping_content || "";
      card.querySelector(".where").value = values.where || "";
      card.querySelector(".batch-size").value = String(values.batch_size || 10000);
      card.querySelector(".use-bulk-api").checked = !!values.use_bulk_api;
      card.querySelector(".bulk-api-method").value = values.bulk_api_method || "";
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
      ensureBindingRowForBindingTask(card);
      syncTaskTypeState(card);
      toggleSourceMode(card);
      toggleTargetMode(card);
      if (loadedTaskGroupId) {
        card.dataset.loadedSignature = buildTaskGroupFormula(card, fallbackIndex);
      }
      syncPartitionState(card);
      syncTaskGroupState(card, fallbackIndex);
      syncMappingState(card);
      refreshMappingSummary(card);
      syncUpsertMatchState(card);
    }

    function toggleSourceMode(card) {
      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim();
      if (taskType !== TASK_TYPES.SOURCE_TARGET) return;
      const sourceType = card.querySelector(".source-type").value;
      const sqlWrap = card.querySelector(".source-sql-wrap");
      const sqlText = card.querySelector(".source-inline-sql");
      const sourceTableWrap = card.querySelector(".source-table-wrap");
      const fileWrap = card.querySelector(".source-file-wrap");
      const sourceSchemaInput = card.querySelector(".source-schema");
      const sourceTableInput = card.querySelector(".source-table");
      const isSqlMode = sourceType === "sql";
      const isFileMode = sourceType === "csv" || sourceType === "json";
      sqlWrap.classList.toggle("hidden", !isSqlMode);
      sourceTableWrap.classList.toggle("hidden", isSqlMode || isFileMode);
      if (fileWrap) {
        fileWrap.classList.toggle("hidden", !isFileMode);
        const jsonOpts = fileWrap.querySelector(".source-json-opts");
        if (jsonOpts) jsonOpts.classList.toggle("hidden", sourceType !== "json");
      }
      // File sources are always explicit-mapping (no type inference).
      const mappingModeSelect = card.querySelector(".column-mapping-mode");
      if (mappingModeSelect && isFileMode) {
        mappingModeSelect.value = "mapping_file";
      }
      if (mappingModeSelect) {
        mappingModeSelect.disabled = isSqlMode || isFileMode;
      }
      if (isSqlMode) {
        sourceSchemaInput.value = "";
        sourceTableInput.value = "";
      } else if (!isFileMode) {
        sqlText.value = "";
      }
      sourceTableInput.placeholder = "Search table";
    }

    function hydrateFileEndpointValues(card, values) {
      const setVal = (sel, v) => { const n = card.querySelector(sel); if (n) n.value = v; };
      const setChk = (sel, v) => { const n = card.querySelector(sel); if (n) n.checked = v; };
      setVal(".source-file-path", values.file_path || "");
      setVal(".source-file-delimiter", values.delimiter || "");
      setVal(".source-file-encoding", values.encoding || "");
      setVal(".source-file-quotechar", values.quotechar || "");
      setChk(".source-file-header", values.header !== false);
      setVal(".source-file-json-mode", values.json_mode || "flat");
      const targetType = String(values.target_type || "db").trim() || "db";
      setVal(".target-type", targetType);
      // F6.2 acik kalemi: iceberg hedef karta yalniz yuklu config'ten gelir
      // (Target Type seciciden secilemez). Bayrak, append-rerun notu icindir.
      card.dataset.icebergTarget = targetType === "iceberg" ? "1" : "0";
      syncIcebergAppendHint(card);
      setVal(".target-file-path", values.target_file_path || "");
      setVal(".target-file-delimiter", values.target_delimiter || "");
      setVal(".target-file-encoding", values.target_encoding || "");
      setChk(".target-file-header", values.target_header !== false);
    }

    // Iceberg hedefte `append` rerun'i satirlari ciftler (idempotent degil);
    // dogrulama yolunda warnings kanali olmadigi icin uyari istemciden verilir.
    function syncIcebergAppendHint(card) {
      const note = card.querySelector(".iceberg-append-note");
      if (!note) return;
      const isIceberg = card.dataset.icebergTarget === "1";
      const loadMethod = String(card.querySelector(".load-method")?.value || "").trim();
      note.hidden = !(isIceberg && loadMethod === "append");
    }

    function toggleTargetMode(card) {
      const targetType = String(card.querySelector(".target-type")?.value || "db").trim() || "db";
      const isFile = targetType === "file";
      const dbWrap = card.querySelector(".target-db-wrap");
      const tableWrap = card.querySelector(".target-table-wrap");
      const fileWrap = card.querySelector(".target-file-wrap");
      if (dbWrap) dbWrap.classList.toggle("hidden", isFile);
      if (tableWrap) tableWrap.classList.toggle("hidden", isFile);
      if (fileWrap) fileWrap.classList.toggle("hidden", !isFile);
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
      const dbtCommandVal = (card.querySelector(".dbt-command")?.value || "run").trim();
      const taskGroupSourceSchema = taskType === TASK_TYPES.SCRIPT_RUN
        ? "script"
        : (taskType === TASK_TYPES.DAG
          ? "dag"
          : (taskType === TASK_TYPES.BINDING
            ? "binding"
            : (taskType === TASK_TYPES.DBT ? "dbt" : (sourceType === "sql" ? "sql" : sourceSchemaVal))));
      const taskGroupSourceTable = taskType === TASK_TYPES.SCRIPT_RUN
        ? (scriptEnvVal || "source")
        : (taskType === TASK_TYPES.DAG
          ? (dagTaskDagId || "dag")
          : (taskType === TASK_TYPES.BINDING
            ? `parameters_${fallbackIndex}`
            : (taskType === TASK_TYPES.DBT ? dbtCommandVal : (sourceType === "sql" ? "query" : sourceTableVal))));
      const taskGroupLoadMethod = taskType === TASK_TYPES.SCRIPT_RUN
        ? "script"
        : (taskType === TASK_TYPES.DAG
          ? "dag"
          : (taskType === TASK_TYPES.BINDING
            ? "binding"
            : (taskType === TASK_TYPES.DBT ? "dbt" : loadMethodVal)));
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
      const mappingContentWrap = card.querySelector(".mapping-content-wrap");
      const mappingEditorLaunch = card.querySelector(".mapping-editor-launch");
      const mappingRawWrap = card.querySelector(".mapping-raw-wrap");
      if (taskType !== TASK_TYPES.SOURCE_TARGET) {
        modeSelect.disabled = true;
        modeSelect.setAttribute("aria-disabled", "true");
        mappingContentWrap.classList.add("hidden");
        if (mappingEditorLaunch) mappingEditorLaunch.classList.add("hidden");
        if (mappingRawWrap) mappingRawWrap.classList.add("hidden");
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
      mappingContentWrap.classList.toggle("hidden", !isMappingFileMode);
      if (mappingEditorLaunch) mappingEditorLaunch.classList.toggle("hidden", !isMappingFileMode);
      if (mappingRawWrap) mappingRawWrap.classList.toggle("hidden", !isMappingFileMode);

      if (isSql) {
        setMappingStatus(card, "Custom Mapping mode is required for SQL source.", false);
      } else if (!isMappingFileMode) {
        setMappingStatus(card, "", false);
      }
      syncUpsertMatchState(card);
    }

    async function generateMappingForCard(card) {
      const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim();
      if (taskType !== TASK_TYPES.SOURCE_TARGET) {
        setMappingStatus(card, "Mapping is available only for Source Target tasks.", true);
        return;
      }
      let folderSelection;
      try {
        folderSelection = requireFolderSelection();
      } catch (err) {
        const message = String(err && err.message ? err.message : FOLDER_PATH_PROMPT);
        setMappingStatus(card, message, true);
        pushToast(message, "error", true);
        return;
      }
      const sourceType = card.querySelector(".source-type").value;
      const taskNo = Math.max(1, getTaskCards().indexOf(card) + 1);
      const taskIdentity = resolveTaskIdentity(card, taskNo);
      const payload = {
        ...folderSelection,
        source_conn_id: (el("source_conn_id").value || "").trim(),
        target_conn_id: (el("target_conn_id").value || "").trim(),
        source_type: sourceType,
        task_group_id: taskIdentity.task_group_id,
        task_no: taskNo,
      };
      if (sourceType === "sql") {
        payload.inline_sql = (card.querySelector(".source-inline-sql").value || "").trim();
      } else if (sourceType === "csv" || sourceType === "json") {
        payload.file_path = (card.querySelector(".source-file-path")?.value || "").trim();
        const delimiter = (card.querySelector(".source-file-delimiter")?.value || "").trim();
        if (delimiter) payload.delimiter = delimiter;
        const encoding = (card.querySelector(".source-file-encoding")?.value || "").trim();
        if (encoding) payload.encoding = encoding;
        const quotechar = (card.querySelector(".source-file-quotechar")?.value || "").trim();
        if (quotechar) payload.quotechar = quotechar;
        payload.header = !!card.querySelector(".source-file-header")?.checked;
        if (sourceType === "json") {
          payload.json_mode = String(card.querySelector(".source-file-json-mode")?.value || "flat").trim() || "flat";
        }
      } else {
        payload.source_schema = (card.querySelector(".source-schema").value || "").trim();
        payload.source_table = (card.querySelector(".source-table").value || "").trim();
      }
      setMappingStatus(card, "Mapping uretiliyor...", false);
      try {
        const data = await postJson(studioUrl("/api/mapping/generate"), payload);
        if (!data || !data.ok) {
          const failMsg = apiErrorMessage(data || {}, "Mapping uretilemedi.");
          setMappingStatus(card, failMsg, true);
          pushToast(sourceType === "sql" ? `SQL Query mapping failed: ${failMsg}` : failMsg, "error", true);
          return;
        }
        card.querySelector(".mapping-content").value = data.mapping_content || "";
        setMappingRowsFromColumns(card, data.columns || [], { serialize: true });
        const warnings = Array.isArray(data.warnings) ? data.warnings : [];
        if (warnings.length) {
          setMappingStatus(card, `Mapping generated (warning: ${warnings.length}).`, false);
        } else {
          setMappingStatus(card, "Mapping uretildi.", false);
        }
        syncMappingState(card);
        // Newly-generated SQL columns are now the partition-column candidates.
        loadPartitionColumnOptions(card);
      } catch (err) {
        const message = apiErrorMessage(err, "Error occurred while generating mapping.");
        const visibleMessage = sourceType === "sql" ? `SQL Query mapping failed: ${message}` : message;
        setMappingStatus(
          card,
          visibleMessage,
          true,
        );
        pushToast(visibleMessage, "error", true);
      }
    }

    // --- F1.2 Talend tMap-style mapping editor popup ----------------------
    // The mapping editor is a single shared modal bound to `activeMappingCard`.
    // Target rows render into the modal's `#mapping_editor_target_list`; the
    // per-card hidden `.mapping-content` textarea stays the single submit
    // source of truth (serialized live on every edit + on Apply).
    let activeMappingCard = null;
    let mappingEditorSnapshot = null; // { content, source } captured on open for Cancel
    let mappingSourceCols = []; // [{name, data_type, nullable}] for the Input panel
    let lastFocusedMappingField = null; // last-focused source/expression input (click-to-insert target)

    function mappingTargetListEl() {
      return el("mapping_editor_target_list");
    }

    function yamlScalar(value) {
      const s = String(value == null ? "" : value);
      return '"' + s.replace(/\\/g, "\\\\").replace(/"/g, '\\"') + '"';
    }

    function syncMappingRowState(row) {
      const derived = row.querySelector(".mapping-col-kind").value === "derived";
      // Source column stays inline (its grid track is kept for alignment even
      // when hidden); the Expression field expands full-width below.
      row.querySelector(".mapping-col-source-name").classList.toggle("hidden", derived);
      row.querySelector(".mapping-col-expression-wrap").classList.toggle("hidden", !derived);
    }

    function markMappingRowsDirty(card) {
      if (!card) return;
      card.dataset.mappingSource = "rows";
      mappingRowsToYaml(card);
      refreshMappingEditorMeta();
    }

    function createMappingRow(card, values = {}) {
      const list = mappingTargetListEl();
      if (!list) return null;
      const row = document.createElement("div");
      row.className = "mapping-column-item";
      row.innerHTML = `
        <div class="mapping-column-row">
          <select class="mapping-col-kind" aria-label="Column kind">
            <option value="direct">Direct</option>
            <option value="derived">Derived</option>
          </select>
          <input class="mapping-col-source-name" list="mapping_editor_source_options" placeholder="source_name">
          <input class="mapping-col-target-name" placeholder="target_name">
          <input class="mapping-col-target-type" placeholder="target_type e.g. varchar(50)">
          <label class="mapping-col-nullable-wrap"><input class="mapping-col-nullable" type="checkbox"></label>
          <button class="btn btn-danger mapping-col-remove" type="button">x</button>
        </div>
        <div class="mapping-col-expression-wrap mapping-col-map-field hidden">
          <input class="mapping-col-expression" placeholder="expression, e.g. concat(first_name, ' ', last_name)">
        </div>
      `;
      row.querySelector(".mapping-col-kind").value = values.expression ? "derived" : "direct";
      row.querySelector(".mapping-col-source-name").value = values.source_name || "";
      row.querySelector(".mapping-col-target-name").value = values.target_name || "";
      row.querySelector(".mapping-col-target-type").value = values.target_type || "";
      row.querySelector(".mapping-col-nullable").checked = values.nullable !== false;
      row.querySelector(".mapping-col-expression").value = values.expression || "";
      if (values.source_type) row.dataset.sourceType = String(values.source_type);

      row.querySelector(".mapping-col-kind").addEventListener("change", () => {
        syncMappingRowState(row);
        row.querySelectorAll(".mapping-col-invalid").forEach((e) => {
          e.classList.remove("mapping-col-invalid");
        });
        markMappingRowsDirty(card);
      });
      row.querySelectorAll("input").forEach((elm) => {
        elm.addEventListener("input", () => {
          elm.classList.remove("mapping-col-invalid");
          markMappingRowsDirty(card);
        });
        if (elm.type === "checkbox") {
          elm.addEventListener("change", () => markMappingRowsDirty(card));
        }
      });
      const srcInput = row.querySelector(".mapping-col-source-name");
      const exprInput = row.querySelector(".mapping-col-expression");
      srcInput.addEventListener("focus", () => {
        lastFocusedMappingField = srcInput;
      });
      exprInput.addEventListener("focus", () => {
        lastFocusedMappingField = exprInput;
      });
      row.querySelector(".mapping-col-remove").addEventListener("click", () => {
        if (lastFocusedMappingField === srcInput || lastFocusedMappingField === exprInput) {
          lastFocusedMappingField = null;
        }
        row.remove();
        markMappingRowsDirty(card);
      });

      list.appendChild(row);
      syncMappingRowState(row);
      return row;
    }

    function getMappingRows() {
      const list = mappingTargetListEl();
      const items = list ? Array.from(list.querySelectorAll(".mapping-column-item")) : [];
      return items.map((row) => {
        const derived = row.querySelector(".mapping-col-kind").value === "derived";
        const base = {
          target_name: row.querySelector(".mapping-col-target-name").value.trim(),
          target_type: row.querySelector(".mapping-col-target-type").value.trim(),
          nullable: row.querySelector(".mapping-col-nullable").checked,
        };
        if (derived) {
          base.expression = row.querySelector(".mapping-col-expression").value.trim();
        } else {
          base.source_name = row.querySelector(".mapping-col-source-name").value.trim();
          if (row.dataset.sourceType) base.source_type = row.dataset.sourceType;
        }
        return base;
      });
    }

    function mappingRowsToYaml(card) {
      const rows = getMappingRows();
      const hasDerived = rows.some((r) => r.expression != null);
      const lines = [`version: ${hasDerived ? "v1.1" : "v1"}`];
      if (!rows.length) {
        lines.push("columns: []");
      } else {
        lines.push("columns:");
        for (const r of rows) {
          lines.push(`- target_name: ${yamlScalar(r.target_name)}`);
          lines.push(`  target_type: ${yamlScalar(r.target_type)}`);
          if (r.expression != null) {
            lines.push(`  expression: ${yamlScalar(r.expression)}`);
          } else {
            lines.push(`  source_name: ${yamlScalar(r.source_name)}`);
            if (r.source_type) lines.push(`  source_type: ${yamlScalar(r.source_type)}`);
          }
          lines.push(`  nullable: ${r.nullable ? "true" : "false"}`);
        }
      }
      const yamlText = lines.join("\n") + "\n";
      const ta = card.querySelector(".mapping-content");
      if (ta) ta.value = yamlText;
      return yamlText;
    }

    // Sticky column-header row inside the scrolling list, so its columns line
    // up pixel-perfect with the row controls (same grid template + same width,
    // scrollbar gutter included). Re-created on every list reset.
    function renderMappingListHeader() {
      const list = mappingTargetListEl();
      if (!list) return;
      const head = document.createElement("div");
      head.className = "mapping-io-col-head";
      head.innerHTML =
        "<span>Type</span>" +
        "<span>Source</span>" +
        "<span>Target</span>" +
        "<span>Data type</span>" +
        '<span class="mapping-col-head-center">Nullable</span>' +
        "<span></span>";
      list.appendChild(head);
    }

    function setMappingRowsFromColumns(card, columns, options = {}) {
      const list = mappingTargetListEl();
      if (!list) return;
      list.innerHTML = "";
      renderMappingListHeader();
      lastFocusedMappingField = null;
      const items = Array.isArray(columns) ? columns : [];
      for (const col of items) createMappingRow(card, col || {});
      if (options.serialize) {
        card.dataset.mappingSource = "rows";
        mappingRowsToYaml(card);
      }
      refreshMappingEditorMeta();
    }

    async function renderMappingTargetRows(card) {
      const content = String((card.querySelector(".mapping-content") || {}).value || "").trim();
      if (!content) {
        setMappingRowsFromColumns(card, []);
        return;
      }
      try {
        const data = await postJson(studioUrl("/api/mapping/parse"), { mapping_content: content });
        setMappingRowsFromColumns(card, (data && data.columns) || []);
      } catch (err) {
        setMappingRowsFromColumns(card, []);
        setMappingEditorStatus(
          "Raw YAML could not be parsed; fix it under the task's Raw YAML (advanced) box.",
          true,
        );
      }
    }

    function describeMappingSource(card) {
      const sourceType = String((card.querySelector(".source-type") || {}).value || "table");
      const conn = (el("source_conn_id").value || "").trim();
      if (sourceType === "sql") {
        return conn ? `${conn} - SQL query source` : "SQL query source";
      }
      const schema = String((card.querySelector(".source-schema") || {}).value || "").trim();
      const table = String((card.querySelector(".source-table") || {}).value || "").trim();
      const loc = [schema, table].filter(Boolean).join(".");
      return [conn, loc].filter(Boolean).join(" - ") || "source not configured";
    }

    function setMappingEditorStatus(message, isError = false) {
      const box = el("mapping_editor_status");
      if (!box) return;
      box.textContent = message || "";
      box.classList.toggle("warn", !!isError);
      box.classList.toggle("ok", !isError && !!message);
      // Hide the footer hint while a status message is shown, restore it after.
      const actions = box.closest(".mapping-editor-actions");
      if (actions) actions.classList.toggle("showing-status", !!message);
    }

    function refreshMappingEditorMeta() {
      const box = el("mapping_editor_meta");
      if (!box) return;
      const rows = getMappingRows();
      const count = rows.length;
      const version = rows.some((r) => r.expression != null) ? "v1.1" : "v1";
      box.textContent = count
        ? `${count} column${count === 1 ? "" : "s"} · ${version}`
        : "no columns · v1";
    }

    function refreshMappingSummary(card) {
      const summary = card.querySelector(".mapping-summary");
      if (!summary) return;
      const content = String((card.querySelector(".mapping-content") || {}).value || "");
      const count = (content.match(/^\s*-?\s*target_name\s*:/gm) || []).length;
      const version = /v1\.1/.test(content) ? "v1.1" : content.trim() ? "v1" : "";
      summary.textContent = count
        ? `${count} column${count === 1 ? "" : "s"}${version ? " - " + version : ""}`
        : "no columns yet";
    }

    async function fetchMappingSourceColumns(card) {
      const sourceType = String((card.querySelector(".source-type") || {}).value || "table");
      if (sourceType === "sql") {
        // A SQL query has no table: its source columns are the SELECT columns,
        // which are the Direct rows' source_name (+ source_type) already in the
        // editor after Generate or on reopen. Derive the Input panel from them.
        const seen = new Set();
        const cols = [];
        for (const r of getMappingRows()) {
          const nm = String(r.source_name || "").trim();
          if (!nm || seen.has(nm)) continue;
          seen.add(nm);
          cols.push({
            name: nm,
            data_type: String(r.source_type || "").trim(),
            precision: null,
            scale: null,
            nullable: r.nullable !== false,
          });
        }
        return cols;
      }
      const connId = (el("source_conn_id").value || "").trim();
      const schema = String((card.querySelector(".source-schema") || {}).value || "").trim();
      const table = String((card.querySelector(".source-table") || {}).value || "").trim();
      if (!connId || !table) return [];
      try {
        const resp = await studioFetch(
          `/api/columns?conn_id=${encodeURIComponent(connId)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`,
        );
        const data = await parseJsonSafe(resp);
        if (resp.ok && data.ok && Array.isArray(data.items)) {
          return data.items
            .map((it) => ({
              name: String((it && it.name) || "").trim(),
              data_type: String((it && it.data_type) || "").trim(),
              precision: it && it.precision != null ? it.precision : null,
              scale: it && it.scale != null ? it.scale : null,
              nullable: !(it && it.nullable === false),
            }))
            .filter((c) => c.name);
        }
      } catch (_err) {
        // best-effort: source panel just stays empty
      }
      return [];
    }

    function formatSourceType(col) {
      const base = String((col && col.data_type) || "").toLowerCase().trim();
      if (!base) return "";
      if (col.precision != null && col.scale != null) return `${base}(${col.precision},${col.scale})`;
      if (col.precision != null) return `${base}(${col.precision})`;
      return base;
    }

    function refreshMappingSourceDatalist(cols) {
      const dl = el("mapping_editor_source_options");
      if (!dl) return;
      dl.innerHTML = "";
      for (const col of cols || []) {
        const opt = document.createElement("option");
        opt.value = col.name;
        dl.appendChild(opt);
      }
    }

    function renderMappingSourcePanel(card, cols) {
      const list = el("mapping_editor_source_list");
      if (!list) return;
      list.innerHTML = "";
      if (!cols || !cols.length) {
        const empty = document.createElement("div");
        empty.className = "mapping-io-empty muted-note";
        empty.textContent = "No source columns. Set source connection & table (or click Generate Mapping).";
        list.appendChild(empty);
        return;
      }
      for (const col of cols) {
        const chip = document.createElement("button");
        chip.type = "button";
        chip.className = "mapping-io-col";
        chip.title = "Click to insert into the focused Source column / Expression field";
        chip.innerHTML = '<span class="mapping-io-col-name"></span><span class="mapping-io-col-type muted-note"></span>';
        chip.querySelector(".mapping-io-col-name").textContent = col.name;
        chip.querySelector(".mapping-io-col-type").textContent = formatSourceType(col) + (col.nullable ? "" : " - not null");
        // Keep the focused field (and its caret) from being stolen by the button.
        chip.addEventListener("mousedown", (evt) => evt.preventDefault());
        chip.addEventListener("click", () => insertSourceColumn(col.name));
        list.appendChild(chip);
      }
    }

    function mappingFieldUsable(field) {
      if (!field || !document.body.contains(field)) return false;
      if (field.classList.contains("mapping-col-source-name")) {
        return !field.classList.contains("hidden");
      }
      const wrap = field.closest(".mapping-col-map-field");
      return !!wrap && !wrap.classList.contains("hidden");
    }

    function lastVisibleDerivedExpr() {
      const list = mappingTargetListEl();
      if (!list) return null;
      const exprs = Array.from(list.querySelectorAll(".mapping-column-item"))
        .filter((item) => item.querySelector(".mapping-col-kind").value === "derived")
        .map((item) => item.querySelector(".mapping-col-expression"));
      return exprs.length ? exprs[exprs.length - 1] : null;
    }

    function insertSourceColumn(name) {
      let field = mappingFieldUsable(lastFocusedMappingField) ? lastFocusedMappingField : null;
      if (!field) {
        // Nothing focused: fall back to the last Derived row's Expression field.
        field = lastVisibleDerivedExpr();
      }
      if (!field) {
        setMappingEditorStatus(
          "Add a Derived column (or focus a Source column / Expression field), then click a source column.",
          true,
        );
        return;
      }
      if (field.classList.contains("mapping-col-source-name")) {
        // Direct source column: replace the whole value with the picked column.
        field.value = name;
      } else {
        // Expression: insert the column name at the caret.
        const start = field.selectionStart != null ? field.selectionStart : field.value.length;
        const end = field.selectionEnd != null ? field.selectionEnd : field.value.length;
        field.value = field.value.slice(0, start) + name + field.value.slice(end);
        const caret = start + name.length;
        try {
          field.setSelectionRange(caret, caret);
        } catch (_err) {
          // some inputs disallow setSelectionRange; ignore
        }
      }
      lastFocusedMappingField = field;
      field.focus();
      field.dispatchEvent(new Event("input", { bubbles: true }));
      setMappingEditorStatus("", false);
    }

    async function openMappingEditor(card) {
      if (isBusy) return;
      const modal = el("mapping_editor_modal");
      if (!modal || !card) return;
      activeMappingCard = card;
      const ta = card.querySelector(".mapping-content");
      mappingEditorSnapshot = {
        content: ta ? ta.value : "",
        source: card.dataset.mappingSource || "",
      };
      const cap = el("mapping_editor_source_caption");
      if (cap) cap.textContent = describeMappingSource(card);
      setMappingEditorStatus("", false);
      // Render the target rows first so a SQL source can derive its Input panel
      // columns from them (they carry source_name/source_type).
      await renderMappingTargetRows(card);
      mappingSourceCols = await fetchMappingSourceColumns(card);
      refreshMappingSourceDatalist(mappingSourceCols);
      renderMappingSourcePanel(card, mappingSourceCols);
      refreshMappingEditorMeta();
      modal.classList.add("open");
      modal.setAttribute("aria-hidden", "false");
      document.body.classList.add("mapping-editor-open");
      // Build the source->target link overlay once the modal is laid out.
      buildMappingLinkOverlay();
    }

    function closeMappingEditor() {
      const modal = el("mapping_editor_modal");
      teardownMappingLinkOverlay();
      if (modal) {
        modal.classList.remove("open");
        modal.setAttribute("aria-hidden", "true");
      }
      document.body.classList.remove("mapping-editor-open");
      const list = mappingTargetListEl();
      if (list) list.innerHTML = "";
      activeMappingCard = null;
      mappingEditorSnapshot = null;
      lastFocusedMappingField = null;
    }

    // ---------------------------------------------------------------
    // Mapping Editor connection-line overlay (Talend tMap style).
    // Draws a dimmed SVG curve from each Source (Input) column to the
    // Target (Output) row(s) that use it (Direct source_name, or every
    // source referenced in a Derived expression); hovering a column
    // brightens its connections. Pure client-side, torn down on close.
    // ---------------------------------------------------------------
    let mappingLinkState = null;
    const _mappingExprRegexCache = new Map();

    function mappingGridEl() {
      return document.querySelector("#mapping_editor_modal .mapping-io-grid");
    }

    function mappingExprReferences(expr, name) {
      let re = _mappingExprRegexCache.get(name);
      if (!re) {
        const esc = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        re = new RegExp(`\\b${esc}\\b`);
        _mappingExprRegexCache.set(name, re);
      }
      return re.test(expr);
    }

    function mappingSourceChipsByName() {
      const map = new Map();
      const list = el("mapping_editor_source_list");
      if (!list) return map;
      list.querySelectorAll(".mapping-io-col").forEach((chip) => {
        const nameEl = chip.querySelector(".mapping-io-col-name");
        const name = nameEl ? nameEl.textContent.trim() : "";
        if (name && !map.has(name)) map.set(name, chip);
      });
      return map;
    }

    function extractMappingLinks() {
      const out = [];
      const targetList = mappingTargetListEl();
      if (!targetList) return out;
      const chipByName = mappingSourceChipsByName();
      const sourceNames = Array.from(chipByName.keys());
      targetList.querySelectorAll(".mapping-column-item").forEach((row) => {
        const kindSel = row.querySelector(".mapping-col-kind");
        const derived = !!kindSel && kindSel.value === "derived";
        if (derived) {
          const expr = String((row.querySelector(".mapping-col-expression") || {}).value || "");
          if (!expr.trim()) return;
          for (const name of sourceNames) {
            if (mappingExprReferences(expr, name)) {
              out.push({ sourceName: name, chip: chipByName.get(name), row });
            }
          }
        } else {
          const src = String((row.querySelector(".mapping-col-source-name") || {}).value || "").trim();
          if (src && chipByName.has(src)) {
            out.push({ sourceName: src, chip: chipByName.get(src), row });
          }
        }
      });
      return out;
    }

    function scheduleMappingLinkRedraw() {
      const st = mappingLinkState;
      if (!st || st.rafId) return;
      st.rafId = requestAnimationFrame(() => {
        st.rafId = 0;
        redrawMappingLinks();
      });
    }

    function redrawMappingLinks() {
      const st = mappingLinkState;
      if (!st || !st.svg || !st.grid) return;
      const gridRect = st.grid.getBoundingClientRect();
      if (!gridRect.width || !gridRect.height) return;
      st.svg.setAttribute("viewBox", `0 0 ${gridRect.width} ${gridRect.height}`);
      while (st.svg.firstChild) st.svg.removeChild(st.svg.firstChild);
      st.links = [];

      const srcRect = st.sourceList.getBoundingClientRect();
      const tgtRect = st.targetList.getBoundingClientRect();
      const tgtHeader = st.targetList.querySelector(".mapping-io-col-head");
      const tgtTop = tgtHeader ? tgtHeader.getBoundingClientRect().bottom : tgtRect.top;

      for (const link of extractMappingLinks()) {
        if (!link.chip || !link.row) continue;
        const cRect = link.chip.getBoundingClientRect();
        const rRect = link.row.getBoundingClientRect();
        const cy = cRect.top + cRect.height / 2;
        const ry = rRect.top + rRect.height / 2;
        // Clip: only draw when both endpoints are within their (independently
        // scrolling) visible list areas.
        if (cy < srcRect.top || cy > srcRect.bottom) continue;
        if (ry < tgtTop || ry > tgtRect.bottom) continue;
        const sx = cRect.right - gridRect.left;
        const sy = cy - gridRect.top;
        const tx = rRect.left - gridRect.left;
        const ty = ry - gridRect.top;
        const dx = Math.max(24, (tx - sx) * 0.5);
        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        path.setAttribute(
          "d",
          `M ${sx} ${sy} C ${sx + dx} ${sy}, ${tx - dx} ${ty}, ${tx} ${ty}`,
        );
        path.setAttribute("class", "mapping-io-link");
        st.svg.appendChild(path);
        st.links.push({ ...link, path });
      }
      if (st.hoverKey) applyMappingLinkHover(st.hoverKey);
    }

    function clearMappingLinkHoverClasses() {
      const st = mappingLinkState;
      if (!st) return;
      (st.links || []).forEach((l) => l.path.classList.remove("is-active"));
      [el("mapping_editor_source_list"), mappingTargetListEl()].forEach((box) => {
        if (box) box.querySelectorAll(".is-linked").forEach((n) => n.classList.remove("is-linked"));
      });
    }

    function applyMappingLinkHover(key) {
      const st = mappingLinkState;
      if (!st || !st.svg) return;
      st.hoverKey = key;
      st.svg.classList.add("has-hover");
      clearMappingLinkHoverClasses();
      for (const link of st.links) {
        const match = key.type === "source"
          ? link.sourceName === key.name
          : link.row === key.row;
        if (!match) continue;
        link.path.classList.add("is-active");
        if (link.chip) link.chip.classList.add("is-linked");
        if (link.row) link.row.classList.add("is-linked");
      }
    }

    function clearMappingLinkHover() {
      const st = mappingLinkState;
      if (!st) return;
      st.hoverKey = null;
      if (st.svg) st.svg.classList.remove("has-hover");
      clearMappingLinkHoverClasses();
    }

    function onMappingLinkHoverOver(evt) {
      const st = mappingLinkState;
      if (!st) return;
      const sourceList = el("mapping_editor_source_list");
      const chip = evt.target.closest && evt.target.closest(".mapping-io-col");
      if (chip && sourceList && sourceList.contains(chip)) {
        const nameEl = chip.querySelector(".mapping-io-col-name");
        const name = nameEl ? nameEl.textContent.trim() : "";
        if (name) applyMappingLinkHover({ type: "source", name });
        return;
      }
      const row = evt.target.closest && evt.target.closest(".mapping-column-item");
      const targetList = mappingTargetListEl();
      if (row && targetList && targetList.contains(row)) {
        applyMappingLinkHover({ type: "target", row });
      }
    }

    function buildMappingLinkOverlay() {
      teardownMappingLinkOverlay();
      const grid = mappingGridEl();
      const sourceList = el("mapping_editor_source_list");
      const targetList = mappingTargetListEl();
      if (!grid || !sourceList || !targetList) return;

      let svg = grid.querySelector(".mapping-io-links");
      if (!svg) {
        svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
        svg.setAttribute("class", "mapping-io-links");
        svg.setAttribute("aria-hidden", "true");
        grid.insertBefore(svg, grid.firstChild);
      }

      const st = {
        svg, grid, sourceList, targetList, links: [], rafId: 0, hoverKey: null,
        onScroll: () => scheduleMappingLinkRedraw(),
        onInput: () => scheduleMappingLinkRedraw(),
        onResize: () => scheduleMappingLinkRedraw(),
        onHoverOver: (evt) => onMappingLinkHoverOver(evt),
        onHoverLeave: () => clearMappingLinkHover(),
      };
      mappingLinkState = st;

      sourceList.addEventListener("scroll", st.onScroll, { passive: true });
      targetList.addEventListener("scroll", st.onScroll, { passive: true });
      targetList.addEventListener("input", st.onInput);
      window.addEventListener("resize", st.onResize);
      sourceList.addEventListener("mouseover", st.onHoverOver);
      targetList.addEventListener("mouseover", st.onHoverOver);
      sourceList.addEventListener("mouseleave", st.onHoverLeave);
      targetList.addEventListener("mouseleave", st.onHoverLeave);

      st.mo = new MutationObserver(() => scheduleMappingLinkRedraw());
      st.mo.observe(sourceList, { childList: true, subtree: true });
      st.mo.observe(targetList, {
        childList: true, subtree: true, attributes: true, attributeFilter: ["class"],
      });
      if (typeof ResizeObserver !== "undefined") {
        st.ro = new ResizeObserver(() => scheduleMappingLinkRedraw());
        st.ro.observe(grid);
      }
      scheduleMappingLinkRedraw();
    }

    function teardownMappingLinkOverlay() {
      const st = mappingLinkState;
      if (!st) return;
      if (st.rafId) cancelAnimationFrame(st.rafId);
      if (st.mo) st.mo.disconnect();
      if (st.ro) st.ro.disconnect();
      window.removeEventListener("resize", st.onResize);
      if (st.sourceList) {
        st.sourceList.removeEventListener("scroll", st.onScroll);
        st.sourceList.removeEventListener("mouseover", st.onHoverOver);
        st.sourceList.removeEventListener("mouseleave", st.onHoverLeave);
      }
      if (st.targetList) {
        st.targetList.removeEventListener("scroll", st.onScroll);
        st.targetList.removeEventListener("input", st.onInput);
        st.targetList.removeEventListener("mouseover", st.onHoverOver);
        st.targetList.removeEventListener("mouseleave", st.onHoverLeave);
      }
      if (st.svg) {
        while (st.svg.firstChild) st.svg.removeChild(st.svg.firstChild);
        st.svg.classList.remove("has-hover");
      }
      mappingLinkState = null;
    }

    // Client-side Apply gate. Collects ALL issues (the backend raises on the
    // first) and flags each offending cell red. Save re-validates on the server
    // (defense-in-depth). A bare (unsized) numeric/length target is a valid
    // "max size" passthrough on a same-dialect mapping and an intentional blank
    // to be filled on a cross-dialect one, so the only type rule enforced here
    // is "not empty"; the server's dialect-aware gate owns the rest.
    function clearMappingInvalid() {
      const list = mappingTargetListEl();
      if (!list) return;
      list.querySelectorAll(".mapping-col-invalid").forEach((elm) => {
        elm.classList.remove("mapping-col-invalid");
      });
    }

    function validateMappingRows() {
      const list = mappingTargetListEl();
      const issues = [];
      if (!list) return { ok: true, issues };
      const seen = new Set();
      Array.from(list.querySelectorAll(".mapping-column-item")).forEach((row) => {
        const kindSel = row.querySelector(".mapping-col-kind");
        const derived = !!kindSel && kindSel.value === "derived";
        const nameInput = row.querySelector(".mapping-col-target-name");
        const typeInput = row.querySelector(".mapping-col-target-type");
        const srcInput = row.querySelector(".mapping-col-source-name");
        const exprInput = row.querySelector(".mapping-col-expression");
        const tName = ((nameInput && nameInput.value) || "").trim();
        const tType = ((typeInput && typeInput.value) || "").trim();
        const flag = (input, msg) => {
          if (input) input.classList.add("mapping-col-invalid");
          issues.push({ name: tName || "(unnamed)", msg });
        };

        if (!tName) flag(nameInput, "target name required");
        else if (seen.has(tName.toLowerCase())) flag(nameInput, `duplicate target '${tName}'`);
        else seen.add(tName.toLowerCase());

        if (derived) {
          if (!(exprInput && exprInput.value.trim())) flag(exprInput, "expression required");
        } else if (!(srcInput && srcInput.value.trim())) {
          flag(srcInput, "source column required");
        }

        if (!tType) {
          flag(typeInput, "data type required");
        }
      });
      return { ok: issues.length === 0, issues };
    }

    function distinctIssueNames(issues) {
      const names = [];
      for (const it of issues) {
        if (!names.includes(it.name)) names.push(it.name);
      }
      return names;
    }

    function applyMappingEditor() {
      const card = activeMappingCard;
      if (!card) {
        closeMappingEditor();
        return;
      }
      clearMappingInvalid();
      const result = validateMappingRows();
      if (!result.ok) {
        const names = distinctIssueNames(result.issues);
        const shown = names.slice(0, 6).join(", ");
        const more = names.length > 6 ? ` +${names.length - 6} more` : "";
        setMappingEditorStatus(
          `Cannot apply - ${result.issues.length} issue(s) in: ${shown}${more}. Fix the highlighted cells.`,
          true,
        );
        return; // keep the modal open until the developer fixes the flagged cells
      }
      card.dataset.mappingSource = "rows";
      mappingRowsToYaml(card);
      refreshMappingSummary(card);
      syncUpsertMatchState(card);
      loadPartitionColumnOptions(card);
      closeMappingEditor();
    }

    function cancelMappingEditor() {
      const card = activeMappingCard;
      if (card && mappingEditorSnapshot) {
        const ta = card.querySelector(".mapping-content");
        if (ta) ta.value = mappingEditorSnapshot.content;
        card.dataset.mappingSource = mappingEditorSnapshot.source || "raw";
        refreshMappingSummary(card);
      }
      closeMappingEditor();
    }

    function bindMappingSection(card) {
      const openBtn = card.querySelector(".btn-open-mapping-editor");
      if (openBtn) {
        openBtn.addEventListener("click", () => openMappingEditor(card));
      }
      const ta = card.querySelector(".mapping-content");
      if (ta) {
        ta.addEventListener("input", () => {
          card.dataset.mappingSource = "raw";
          refreshMappingSummary(card);
        });
      }
      refreshMappingSummary(card);
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
      const scheduleUpsertColumnRefresh = () => {
        clearTimeout(card._ffUpsertColumnTimer);
        card._ffUpsertColumnTimer = setTimeout(() => {
          loadUpsertMatchOptions(card);
        }, 220);
      };

      taskTypeSelect.addEventListener("change", () => {
        syncTaskTypeState(card);
        ensureBindingRowForBindingTask(card);
        syncPartitionState(card);
        syncMappingState(card);
        syncUpsertMatchState(card);
        refreshTaskCardHeaders();
      });

      sourceTypeSelect.addEventListener("change", () => {
        toggleSourceMode(card);
        refreshTaskCardHeaders();
        syncMappingState(card);
        schedulePartitionColumnRefresh();
        scheduleUpsertColumnRefresh();
      });

      const targetTypeSelect = card.querySelector(".target-type");
      if (targetTypeSelect) {
        targetTypeSelect.addEventListener("change", () => {
          // Secici db|file sunar; elle degisiklik yuklu iceberg hedefini terk eder.
          card.dataset.icebergTarget = "0";
          syncIcebergAppendHint(card);
          toggleTargetMode(card);
          refreshTaskCardHeaders();
        });
      }
      if (loadMethodSelect) {
        loadMethodSelect.addEventListener("change", () => syncIcebergAppendHint(card));
      }

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
          loadUpsertMatchOptions(card);
        }, 220);
      });
      sourceSchemaInput.addEventListener("change", () => {
        schedulePartitionColumnRefresh();
        scheduleUpsertColumnRefresh();
      });

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
          loadUpsertMatchOptions(card);
        }, 220);
      });
      sourceTableInput.addEventListener("change", () => {
        schedulePartitionColumnRefresh();
        scheduleUpsertColumnRefresh();
      });

      targetSchemaInput.addEventListener("input", () => {
        refreshTaskCardHeaders();
        scheduleUpsertColumnRefresh();
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
        scheduleUpsertColumnRefresh();
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
      loadMethodSelect.addEventListener("change", () => {
        refreshTaskCardHeaders();
        syncUpsertMatchState(card);
        scheduleUpsertColumnRefresh();
      });
      scriptEnvSelect.addEventListener("change", () => refreshTaskCardHeaders());
      scriptSqlInput.addEventListener("input", () => refreshTaskCardHeaders());
      // F3.2b — cosmos/task mode toggles the cosmos-only controls.
      const dbtExecutionSelect = card.querySelector(".dbt-execution");
      if (dbtExecutionSelect) {
        dbtExecutionSelect.addEventListener("change", () => syncDbtExecutionControls(card));
      }
      dagTaskDagSelect.addEventListener("change", () => {
        refreshTaskCardHeaders();
        syncTaskTypeState(card);
      });
      card.querySelector(".mapping-content").addEventListener("input", () => {
        scheduleUpsertColumnRefresh();
        loadPartitionColumnOptions(card);
      });
      card.querySelector(".upsert-match-input").addEventListener("keydown", (evt) => {
        if (evt.key === "Enter" || evt.key === ",") {
          evt.preventDefault();
          const inputNode = card.querySelector(".upsert-match-input");
          addUpsertMatchColumn(card, inputNode.value);
          inputNode.value = "";
        }
      });
      card.querySelector(".upsert-match-input").addEventListener("blur", () => {
        const inputNode = card.querySelector(".upsert-match-input");
        addUpsertMatchColumn(card, inputNode.value);
        inputNode.value = "";
      });
    }

    function addTaskCard(values = {}, options = {}) {
      const template = el("task_card_template");
      const node = template.content.firstElementChild.cloneNode(true);
      const fallbackIndex = getTaskCards().length + 1;
      const upsertOptionsNode = node.querySelector(".upsert-match-options");
      const upsertInputNode = node.querySelector(".upsert-match-input");
      if (upsertOptionsNode && upsertInputNode) {
        const upsertListId = `upsert_match_options_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
        upsertOptionsNode.id = upsertListId;
        upsertInputNode.setAttribute("list", upsertListId);
      }
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
      modeSelect.addEventListener("change", () => {
        syncMappingState(card);
        const taskType = String(card.querySelector(".task-type")?.value || TASK_TYPES.SOURCE_TARGET).trim();
        // Auto-open the editor when the user selects mapping_file from the list.
        if (modeSelect.value === "mapping_file" && taskType === TASK_TYPES.SOURCE_TARGET) {
          openMappingEditor(card);
        }
      });
      bindMappingSection(card);
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
      const loadedDagParams = Array.isArray(payload.dag_params) ? payload.dag_params : [];
      dagParamsAppliedState = normalizeDagParams(loadedDagParams || defaultDagParams());
      notificationsAppliedState = payload.notifications || null;
      renderAdvancedSummary();
      setSchedulerFormFromState(schedulerAppliedState);
      syncFolderPathDisplay();
      setConnectionValue("source_conn_id", payload.source_conn_id || "");
      setConnectionValue("target_conn_id", payload.target_conn_id || "");
      const engine = payload.engine && typeof payload.engine === "object" ? payload.engine : null;
      const spark = engine && engine.spark && typeof engine.spark === "object" ? engine.spark : {};
      engineConfigExplicit = !!engine;
      el("engine_preference").value = String(engine?.preference || "auto");
      el("engine_spark_submit_mode").value = String(spark.submit_mode || "k8s");
      el("engine_spark_conn_id").value = String(spark.conn_id || "");
      syncEngineOptions();
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
      const data = await parseJsonSafe(r);
      if (!r.ok && !data.detail && !data.message) {
        data.detail = `${r.status} ${r.statusText || "HTTP error"}`.trim();
      }
      if (data.ok == null && !r.ok) {
        data.ok = false;
      }
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
      if (!validateFolderSelectionBeforeSubmit()) return;

      if (!beginOperation("Updating configuration...")) {
        return;
      }
      try {
        await validateAllAirflowVariableBindings();
        await validateAirflowNamespaceKeys();
        const payload = collectPayload();
        validateDagParameterPayload(payload);
        const data = await postJson(
          studioUrl(`/api/update-dag?dag_id=${encodeURIComponent(dagId)}`),
          payload
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
        const message = String(err && err.message ? err.message : "Unexpected error occurred during update.");
        setUpdateModeStatus(message, "warn");
        pushToast(message, "error", true);
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
      if (!validateFolderSelectionBeforeSubmit()) return;
      if (!beginOperation("Creating DAG...")) {
        return;
      }
      try {
        await validateAllAirflowVariableBindings();
        await validateAirflowNamespaceKeys();
        const payload = collectPayload();
        validateDagParameterPayload(payload);
        const data = await postJson(studioUrl("/api/create-dag"), payload);
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
        const message = String(err && err.message ? err.message : "Unexpected error occurred during create.");
        setUpdateModeStatus(message, "warn");
        pushToast(message, "error", true);
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
      // `.mapping-content` is kept live by the editor (serialized on every edit
      // and on Apply), so it is already authoritative here — no re-serialize.
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
      const isFileSourceType = sourceType === "csv" || sourceType === "json";
      const targetTypeVal = String(card.querySelector(".target-type")?.value || "db").trim() || "db";
      const isFileTargetType = targetTypeVal === "file";
      const dbSourceRelation = taskType === TASK_TYPES.SOURCE_TARGET && sourceType !== "sql" && !isFileSourceType;
      const normalizedSourceSchema = dbSourceRelation ? sourceSchemaVal : undefined;
      const normalizedSourceTable = dbSourceRelation ? sourceTableVal : undefined;
      const normalizedTargetSchema = isFileTargetType
        ? undefined
        : (taskType === TASK_TYPES.SOURCE_TARGET
          ? (targetSchemaVal || undefined)
          : (taskType === TASK_TYPES.SCRIPT_RUN ? (targetSchemaVal || "script_tgt") : undefined));
      const normalizedTargetTable = isFileTargetType
        ? undefined
        : (taskType === TASK_TYPES.SOURCE_TARGET
          ? (targetTableVal || undefined)
          : (taskType === TASK_TYPES.SCRIPT_RUN ? (targetTableVal || "script_task") : undefined));
      const loadMethod = card.querySelector(".load-method").value;
      const upsertMatchColumns = getUpsertMatchState(card);
      if (
        taskType === TASK_TYPES.SOURCE_TARGET
        && loadMethod === "upsert"
        && !upsertMatchColumns.length
      ) {
        setUpsertMatchNote(card, "Select at least one match column for upsert.", true);
        throw new Error("Upsert requires at least one match column.");
      }
      const bindings = getBindingRows(card)
        .map((row) => {
          const bindingSource = row.querySelector(".binding-source").value;
          const variableName = taskType === TASK_TYPES.BINDING
            ? row.querySelector(".binding-variable-name-select").value.trim()
            : row.querySelector(".binding-variable-name").value.trim();
          const item = {
            variable_name: variableName,
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
        load_method: loadMethod,
        upsert_match_columns:
          taskType === TASK_TYPES.SOURCE_TARGET
          && loadMethod === "upsert"
          && upsertMatchColumns.length
            ? upsertMatchColumns
            : undefined,
        column_mapping_mode: card.querySelector(".column-mapping-mode").value,
        mapping_content: taskType === TASK_TYPES.SOURCE_TARGET && card.querySelector(".column-mapping-mode").value === "mapping_file"
          ? (card.querySelector(".mapping-content").value || "").trim() || undefined
          : undefined,
        where: taskType === TASK_TYPES.SOURCE_TARGET ? (card.querySelector(".where").value.trim() || undefined) : undefined,
        batch_size: Number(card.querySelector(".batch-size").value || 10000),
        use_bulk_api: !!card.querySelector(".use-bulk-api").checked,
        bulk_api_method:
          (card.querySelector(".bulk-api-method").value || "").trim() || undefined,
        partitioning_enabled: partitioningEnabled,
        partitioning_mode: partitioningMode,
        partitioning_column: partitioningColumn,
        partitioning_parts: partitioningParts,
        partitioning_distinct_limit: partitioningDistinctLimitValue,
        partitioning_ranges: partitioningRanges,
        bindings: bindings.length ? bindings : undefined,
        ...collectFileEndpointFields(card, taskType, sourceType, targetTypeVal),
        ...collectDbtFields(card, taskType),
        depends_on: dependsOn,
      };
    }

    // F3.2 — dbt task fields (Enterprise-run; backend validates the contract
    // and rejects with 422 when no provider is installed).
    function collectDbtFields(card, taskType) {
      if (taskType !== TASK_TYPES.DBT) return {};
      const out = {
        dbt_project_ref: (card.querySelector(".dbt-project-ref")?.value || "").trim() || undefined,
        dbt_command: (card.querySelector(".dbt-command")?.value || "run").trim(),
        dbt_select: (card.querySelector(".dbt-select")?.value || "").trim() || undefined,
      };
      const target = (card.querySelector(".dbt-target")?.value || "").trim();
      if (target) out.dbt_target = target;
      const threadsRaw = (card.querySelector(".dbt-threads")?.value || "").trim();
      if (threadsRaw) {
        const threads = Number(threadsRaw);
        if (!Number.isInteger(threads) || threads < 1) {
          throw new Error("dbt Threads must be a positive integer.");
        }
        out.dbt_threads = threads;
      }
      const varsRaw = (card.querySelector(".dbt-vars")?.value || "").trim();
      if (varsRaw) {
        let parsed;
        try {
          parsed = JSON.parse(varsRaw);
        } catch (_err) {
          throw new Error("dbt Vars must be a flat JSON object (e.g. {\"run_date\": \"{{ dag.run_date }}\"}).");
        }
        if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
          throw new Error("dbt Vars must be a flat JSON object.");
        }
        out.dbt_vars = parsed;
      }
      // F3.2b (Cosmos) — execution mode + cosmos-only knobs. Task mode
      // sends neither test behavior nor emit flag (backend rejects them
      // fail-loud; the UI simply never produces that combination).
      const execution = (card.querySelector(".dbt-execution")?.value || "cosmos").trim();
      out.dbt_execution = execution;
      if (execution === "cosmos") {
        const behavior = (card.querySelector(".dbt-test-behavior")?.value || "").trim();
        if (behavior) out.dbt_test_behavior = behavior;
        if (card.querySelector(".dbt-emit-datasets")?.checked) {
          out.emit_datasets = true;
        }
        // F6.4 — adapter/platform selector (cosmos only; empty = backend
        // default postgres, so a pre-F6.4 payload stays byte-identical).
        const platform = (card.querySelector(".dbt-target-platform")?.value || "").trim();
        if (platform) out.dbt_target_platform = platform;
      }
      return out;
    }

    // F3.2b — cosmos-only controls are disabled (not silently dropped) in
    // task mode so the operator sees why they do not apply.
    function syncDbtExecutionControls(card) {
      const execution = (card.querySelector(".dbt-execution")?.value || "cosmos").trim();
      const cosmosMode = execution === "cosmos";
      const behavior = card.querySelector(".dbt-test-behavior");
      const emit = card.querySelector(".dbt-emit-datasets");
      if (behavior) {
        behavior.disabled = !cosmosMode;
        if (!cosmosMode) behavior.value = "";
      }
      if (emit) {
        emit.disabled = !cosmosMode;
        if (!cosmosMode) emit.checked = false;
      }
      const platform = card.querySelector(".dbt-target-platform");
      if (platform) {
        platform.disabled = !cosmosMode;
        if (!cosmosMode) platform.value = "";
      }
    }

    function collectFileEndpointFields(card, taskType, sourceType, targetType) {
      const out = {};
      if (taskType !== TASK_TYPES.SOURCE_TARGET) return out;
      if (sourceType === "csv" || sourceType === "json") {
        out.file_path = (card.querySelector(".source-file-path")?.value || "").trim() || undefined;
        const delimiter = (card.querySelector(".source-file-delimiter")?.value || "").trim();
        if (delimiter) out.delimiter = delimiter;
        const encoding = (card.querySelector(".source-file-encoding")?.value || "").trim();
        if (encoding) out.encoding = encoding;
        const quotechar = (card.querySelector(".source-file-quotechar")?.value || "").trim();
        if (quotechar) out.quotechar = quotechar;
        out.header = !!card.querySelector(".source-file-header")?.checked;
        if (sourceType === "json") {
          out.json_mode = String(card.querySelector(".source-file-json-mode")?.value || "flat").trim() || "flat";
        }
      }
      if (targetType === "file") {
        out.target_type = "file";
        out.target_file_path = (card.querySelector(".target-file-path")?.value || "").trim() || undefined;
        const tdelim = (card.querySelector(".target-file-delimiter")?.value || "").trim();
        if (tdelim) out.target_delimiter = tdelim;
        const tenc = (card.querySelector(".target-file-encoding")?.value || "").trim();
        if (tenc) out.target_encoding = tenc;
        out.target_header = !!card.querySelector(".target-file-header")?.checked;
      }
      return out;
    }

    function collectPayload() {
      const folderSelection = requireFolderSelection();
      const cards = getTaskCards();
      const taskIds = cards.map((card, idx) => resolveTaskIdentity(card, idx + 1).task_group_id);
      const flowTasks = cards.map((card, idx) => collectTaskPayload(card, idx + 1, taskIds));
      const firstTask = flowTasks[0] || {};
      const payload = {
        ...folderSelection,
        custom_tags: customTagsState.slice(),
        scheduler: cloneSchedulerState(schedulerAppliedState || collectSchedulerFormPayload()),
        dag_params: normalizeDagParams(dagParamsAppliedState || defaultDagParams()),
        notifications: cloneNotifications(notificationsAppliedState),
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
        upsert_match_columns: firstTask.upsert_match_columns,
        column_mapping_mode: firstTask.column_mapping_mode,
        where: firstTask.where,
        batch_size: firstTask.batch_size,
        use_bulk_api: firstTask.use_bulk_api,
        bulk_api_method: firstTask.bulk_api_method,
        partitioning_enabled: firstTask.partitioning_enabled,
        partitioning_mode: firstTask.partitioning_mode,
        partitioning_column: firstTask.partitioning_column,
        partitioning_parts: firstTask.partitioning_parts,
        partitioning_distinct_limit: firstTask.partitioning_distinct_limit,
        partitioning_ranges: firstTask.partitioning_ranges,
        flow_tasks: flowTasks,
      };
      const enginePreference = String(el("engine_preference")?.value || "auto").trim();
      if (engineConfigExplicit || enginePreference !== "auto") {
        payload.engine = { preference: enginePreference };
        if (enginePreference === "spark") {
          payload.engine.spark = {
            submit_mode: String(el("engine_spark_submit_mode")?.value || "").trim(),
          };
          const sparkConnId = String(el("engine_spark_conn_id")?.value || "").trim();
          if (sparkConnId) payload.engine.spark.conn_id = sparkConnId;
        }
      }
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
    const advancedCompactPanel = el("advanced_compact_panel");
    if (advancedCompactPanel) {
      advancedCompactPanel.addEventListener("click", () => openAdvancedModal());
      advancedCompactPanel.addEventListener("keydown", (evt) => {
        if (evt.key === "Enter" || evt.key === " ") {
          evt.preventDefault();
          openAdvancedModal();
        }
      });
    }
    el("btn_cancel_scheduler_modal").onclick = () => closeSchedulerModal();
    el("btn_apply_scheduler_modal").onclick = () => applySchedulerModal();
    el("scheduler_modal_backdrop").onclick = () => closeSchedulerModal();
    el("btn_cancel_advanced_modal").onclick = () => closeAdvancedModal();
    el("btn_apply_advanced_modal").onclick = () => applyAdvancedModal();
    el("advanced_modal_backdrop").onclick = () => closeAdvancedModal();
    for (const tab of document.querySelectorAll(".advanced-tab-btn")) {
      tab.addEventListener("click", () => setAdvancedTab(tab.getAttribute("data-advanced-tab") || "params"));
    }
    el("notify_on_deadline").addEventListener("change", syncDeadlineMinutesVisibility);
    el("btn_add_dag_param").onclick = () => createAdvancedParamRow({});
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

    el("mapping_editor_backdrop").onclick = () => cancelMappingEditor();
    el("btn_cancel_mapping_editor").onclick = () => cancelMappingEditor();
    el("btn_apply_mapping_editor").onclick = () => applyMappingEditor();
    el("mapping_editor_add_column").onclick = () => {
      if (!activeMappingCard) return;
      createMappingRow(activeMappingCard, { nullable: true });
      markMappingRowsDirty(activeMappingCard);
    };
    el("mapping_editor_generate").onclick = async () => {
      const card = activeMappingCard;
      if (!card) return;
      setMappingEditorStatus("Generating mapping…", false);
      await generateMappingForCard(card);
      // The tab status + toast sit behind the modal, so surface the result in
      // the modal footer (below the hint) where the user can actually see it.
      const statusBox = card.querySelector(".mapping-status");
      const failed = !!statusBox && statusBox.classList.contains("warn");
      // Flag draft cells that still need an explicit Data Type (non-blocking).
      clearMappingInvalid();
      const check = validateMappingRows();
      if (failed) {
        setMappingEditorStatus(statusBox.textContent || "", true);
      } else if (!check.ok) {
        const names = distinctIssueNames(check.issues);
        const more = names.length > 6 ? ` +${names.length - 6} more` : "";
        setMappingEditorStatus(
          `Draft ready - complete the highlighted Data Type cell(s): ${names.slice(0, 6).join(", ")}${more}.`,
          true,
        );
      } else {
        setMappingEditorStatus(statusBox ? statusBox.textContent || "" : "", false);
      }
      // The source table may only now be known; refresh the Input panel + datalist.
      mappingSourceCols = await fetchMappingSourceColumns(card);
      refreshMappingSourceDatalist(mappingSourceCols);
      renderMappingSourcePanel(card, mappingSourceCols);
    };
    el("source_conn_id").addEventListener("change", () => {
      refreshTaskCardHeaders();
      refreshAllPartitionColumnOptions();
      refreshAllUpsertMatchOptions();
    });
    el("target_conn_id").addEventListener("change", () => {
      refreshTaskCardHeaders();
      refreshAllUpsertMatchOptions();
    });
    el("engine_preference").addEventListener("change", () => {
      engineConfigExplicit = true;
      syncEngineOptions();
    });
    el("engine_spark_submit_mode").addEventListener("change", () => {
      engineConfigExplicit = true;
      syncEngineOptions();
    });
    el("engine_spark_conn_id").addEventListener("input", () => {
      engineConfigExplicit = true;
    });
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
      if (el("mapping_editor_modal") && el("mapping_editor_modal").classList.contains("open")) {
        cancelMappingEditor();
        return;
      }
      if (el("scheduler_modal").classList.contains("open")) {
        closeSchedulerModal();
        return;
      }
      if (el("advanced_modal").classList.contains("open")) {
        closeAdvancedModal();
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
      resetEngineConfig();
      setUpdateMode(false);
      setCustomTags([]);
      dagParamsAppliedState = defaultDagParams();
      dagParamsDraftState = null;
      notificationsAppliedState = null;
      notificationsDraftState = null;
      dagDepsOptionsState = [];
      dagDepsReferencedByState = [];
      renderAdvancedSummary();
      await initializeSchedulerDefaultsForCreate();
      syncFolderPathDisplay();
      clearAndLoadTasks([{}]);
      // Connection list must be loaded first for main form usage.
      try {
        await loadConnections();
      } catch (_err) {
        // no-op: UI message already shown
      }
      loadMailTemplateNames();
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

// F6.3 (EX-D036) - CDC Operations paneli: lag / checkpoint / blocked / auditli skip.
// Kanonik kaynak target-side kontrol tablolaridir; bu panel yalniz okur ve
// exact-match skip REQUEST'i yazar (uygulama kararini koordinator verir).
(function cdcOpsPanel() {
  const panel = document.getElementById("cdc_ops_panel");
  if (!panel) return;
  const el = (id) => document.getElementById(id);
  const statusEl = el("cdc_ops_status");
  const tablesEl = el("cdc_ops_tables");
  const skipEl = el("cdc_ops_skip");
  let lastStatus = null;

  function opsPayload() {
    return {
      kafka_conn_id: el("cdc_ops_kafka_conn").value.trim(),
      target_conn_id: el("cdc_ops_target_conn").value.trim(),
      flow_id: el("cdc_ops_flow").value.trim(),
      task_group_id: el("cdc_ops_tg").value.trim(),
      topic: el("cdc_ops_topic").value.trim(),
    };
  }

  async function cdcPost(url, body) {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    let data = {};
    try { data = await resp.json(); } catch (_e) { data = {}; }
    if (!resp.ok) {
      throw new Error(data.detail || data.message || (resp.status + " HTTP"));
    }
    return data;
  }

  function esc(value) {
    return String(value == null ? "" : value).replace(/[&<>"]/g, (ch) => (
      { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]
    ));
  }

  function renderStatus(data) {
    lastStatus = data;
    if (!data.initialized) {
      tablesEl.innerHTML = "";
      skipEl.hidden = true;
      statusEl.textContent =
        "Akis henuz kosmadi (kontrol tablolari yok); ilk batch bootstrap yapar.";
      return;
    }
    const lag = data.lag || {};
    const lagNote = data.lag_error
      ? "lag: BILINMIYOR (broker erisilemedi: " + esc(data.lag_error) + ")"
      : "";
    const rows = (data.checkpoints || []).map((c) => {
      const l = lag[c.partition];
      return "<tr><td>" + c.partition + "</td><td>" + c.next_offset +
        "</td><td>" + c.high_watermark + "</td><td>" +
        (l == null ? "?" : l) + "</td><td>" + esc(c.status) + "</td><td>" +
        esc(c.updated_at) + "</td></tr>";
    }).join("");
    const blocked = (data.blocked || []).map((b) => (
      "<tr><td>" + b.partition + "</td><td>" + b.offset + "</td><td>" +
      esc(b.reason) + "</td><td>" + esc(b.created_at) + "</td></tr>"
    )).join("");
    const pend = (data.pending_skip_requests || []).map((s) => (
      "<tr><td>" + s.partition + "</td><td>" + s.offset + "</td><td>" +
      esc(s.reason) + "</td><td>" + esc(s.requested_by) + "</td></tr>"
    )).join("");
    tablesEl.innerHTML =
      "<h4>Checkpoint / Lag</h4><table class=\"table\"><thead><tr>" +
      "<th>part</th><th>next_offset</th><th>high</th><th>lag</th>" +
      "<th>status</th><th>updated_at</th></tr></thead><tbody>" +
      (rows || "<tr><td colspan=6>-</td></tr>") + "</tbody></table>" +
      "<h4>Bloklu Offset</h4><table class=\"table\"><thead><tr>" +
      "<th>part</th><th>offset</th><th>sebep</th><th>ne zaman</th></tr>" +
      "</thead><tbody>" + (blocked || "<tr><td colspan=4>-</td></tr>") +
      "</tbody></table>" +
      (pend ? "<h4>Bekleyen Skip Istekleri</h4><table class=\"table\"><tbody>" +
        pend + "</tbody></table>" : "");
    skipEl.hidden = (data.blocked || []).length === 0;
    statusEl.textContent = lagNote || "Guncellendi.";
  }

  el("cdc_ops_refresh").addEventListener("click", async () => {
    statusEl.textContent = "Yukleniyor...";
    try {
      renderStatus(await cdcPost("/api/cdc/status", opsPayload()));
    } catch (err) {
      tablesEl.innerHTML = "";
      skipEl.hidden = true;
      statusEl.textContent = "Hata: " + err.message;
    }
  });

  el("cdc_skip_submit").addEventListener("click", async () => {
    const blockedRows = (lastStatus && lastStatus.blocked) || [];
    if (!blockedRows.length) {
      statusEl.textContent = "Bloklu event yok; skip istegi anlamsiz.";
      return;
    }
    const blocked = blockedRows[0];
    const body = Object.assign(opsPayload(), {
      partition: blocked.partition,
      offset: blocked.offset,
      config_hash: blocked.config_hash,
      source_cluster_fingerprint: el("cdc_skip_cluster").value.trim(),
      reason: el("cdc_skip_reason").value.trim(),
      requested_by: el("cdc_skip_by").value.trim(),
    });
    statusEl.textContent = "Skip istegi gonderiliyor...";
    try {
      await cdcPost("/api/cdc/skip-request", body);
      statusEl.textContent =
        "Skip istegi kaydedildi (part " + blocked.partition + ", offset " +
        blocked.offset + "); sonraki kosuda eslesirse auditli uygulanir.";
    } catch (err) {
      statusEl.textContent = "Hata: " + err.message;
    }
  });
})();
