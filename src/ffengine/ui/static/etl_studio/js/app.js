const STUDIO_BASE_CANDIDATES = (() => {
  const pathname = (window.location.pathname || "").toLowerCase();
  const candidates = [];
  if (pathname.startsWith("/plugin/etl-studio")) {
    candidates.push("/plugin/etl-studio");
  }
  if (pathname.startsWith("/etl-studio")) {
    candidates.push("/etl-studio");
  }
  candidates.push("/etl-studio", "/plugin/etl-studio");
  return Array.from(new Set(candidates));
})();
let studioBase = STUDIO_BASE_CANDIDATES[0] || "/etl-studio";

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
    const THEME_CACHE_KEY = "etl_studio_airflow_theme_css_v1";

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
          // Airflow app'in runtime global kurallarini (opacity, app layout vb.)
          // buraya tasimamak icin yalniz tema token tanimlari olan stilleri kopyala.
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
      const shellResp = await fetch("/plugin/etl_studio");
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
        console.info(`[etl-studio-theme] source=parent copied_assets=${copied} direct_sync=${parentSynced}`);
        return;
      }
      diagnostics.push("parent_assets=0");

      try {
        const source = await loadThemeFromPluginEntryScript();
        syncThemeTokensFromDocument(document);
        setThemeSource(source);
        console.info(`[etl-studio-theme] source=${source}`);
        return;
      } catch (err) {
        diagnostics.push(`plugin_script=${String(err && err.message || err)}`);
      }

      const linked = await tryAttachAirflowCss();
      if (linked) {
        syncThemeTokensFromDocument(document);
        setThemeSource("known-static-link");
        console.info("[etl-studio-theme] source=known-static-link");
        return;
      }
      diagnostics.push("known_static_link=0");

      setThemeSource("fallback");
      showThemeNotice("Airflow tema assetleri yuklenemedi, fallback tema kullaniliyor.");
      console.warn(`[etl-studio-theme] source=fallback ${diagnostics.join(" | ")}`);
    }

    const out = document.getElementById("out");
    const show = (value) => { out.textContent = JSON.stringify(value, null, 2); };

    function el(id) { return document.getElementById(id); }
    function setUpdateModeStatus(message, variant) {
      const box = el("update_mode_status");
      box.classList.remove("hidden", "ok", "warn");
      if (variant === "ok") box.classList.add("ok");
      if (variant === "warn") box.classList.add("warn");
      box.textContent = message;
    }

    function setLegacyGuard(data) {
      const hint = (data && data.migration_hint) || "Legacy DAG update mode desteklenmiyor.";
      const url = (data && data.migration_url) || "/etl-studio/";
      const box = el("legacy_guard");
      box.classList.remove("hidden");
      box.innerHTML = `${hint} <a href="${url}">Migration rehberi</a>`;
      setUpdateModeStatus("Legacy DAG icin update kilitlendi.", "warn");
    }

    function clearLegacyGuard() {
      const box = el("legacy_guard");
      box.classList.add("hidden");
      box.textContent = "";
    }

    function setUpdateMode(active) {
      const top = el("update_actions_top");
      const bottomCreate = el("create_actions_bottom");
      if (active) {
        top.classList.remove("hidden");
        bottomCreate.classList.add("hidden");
      } else {
        top.classList.add("hidden");
        bottomCreate.classList.remove("hidden");
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
        opt.textContent = "Connection bulunamadi";
        select.appendChild(opt);
        return;
      }
      for (const item of items) {
        const opt = document.createElement("option");
        opt.value = item.conn_id;
        const suffix = item.conn_type ? ` (${item.conn_type})` : "";
        opt.textContent = `${item.conn_id}${suffix}`;
        select.appendChild(opt);
      }
      const matched = items.some((x) => x.conn_id === preferredConnId);
      select.value = matched ? preferredConnId : items[0].conn_id;
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
          // Airflow Variable listesi sadece Bindings icindeki opsiyonel alan icin kullanilir.
          // Bu nedenle burada ana UI hata kutusunu kirletmiyoruz.
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
        show({ status_code: r.status, ...data });
        return;
      }
      const rawItems = Array.isArray(data.items) ? data.items : [];
      const query = String(q || "").trim().toLowerCase();
      const filtered = rawItems.filter((x) => String(x || "").toLowerCase().includes(query));
      fillOptions(listId, filtered);
      if (!filtered.length) {
        const connType = getSelectedConnectionType(connSelectId || "");
        const extra = connType === "mssql" ? " MSSQL icin schema genelde 'dbo' olur." : "";
        show({ ok: true, detail: `'${q}' icin schema eslesmesi bulunamadi.${extra}` });
      }
    }

    async function autocompleteTables(connId, schema, q, listId) {
      if (!connId || !q || q.length < 3) return;
      if (!schema || !schema.trim()) {
        show({ ok: false, detail: "Once schema alani icin en az 1 karakter girin." });
        return;
      }
      const path = `/api/tables?conn_id=${encodeURIComponent(connId)}&schema=${encodeURIComponent(schema)}&q=${encodeURIComponent(q)}&limit=50&offset=0`;
      const r = await studioFetch(path);
      const data = await parseJsonSafe(r);
      if (!r.ok || !data.ok) {
        show({ status_code: r.status, ...data });
        return;
      }
      fillOptions(listId, data.items || []);
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
      el("folder_path_display").value = getFolderPathText({
        project: el("project").value,
        domain: el("domain").value,
        level: el("level").value,
        flow: el("flow").value,
      });
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
        show({ status_code: r.status, ...data });
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
        if (!pickerDraft.project) return show({ ok: false, detail: "Once project secin." });
        setMapItem(pickerTemp.domains, pickerDraft.project, raw);
        pickerDraft.domain = raw;
        clearDraftBelow("domain");
      } else if (levelName === "level") {
        if (!pickerDraft.project || !pickerDraft.domain) {
          return show({ ok: false, detail: "Once project ve domain secin." });
        }
        setMapItem(pickerTemp.levels, `${pickerDraft.project}/${pickerDraft.domain}`, raw);
        pickerDraft.level = raw;
        clearDraftBelow("level");
      } else if (levelName === "flow") {
        if (!pickerDraft.project || !pickerDraft.domain || !pickerDraft.level) {
          return show({ ok: false, detail: "Once project/domain/level secin." });
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
          // Backward compatibility for running containers that do not yet expose /etl-studio/api/connections.
          const airflowResp = await fetch("/api/v2/connections?limit=1000&offset=0&order_by=connection_id");
          const airflowData = await parseJsonSafe(airflowResp);
          if (!airflowResp.ok) {
            const detail = airflowData.detail || "Connection listesi yuklenemedi.";
            show({ status_code: airflowResp.status, detail });
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
      } catch (err) {
        show({ ok: false, detail: `Connection listesi yuklenemedi: ${String(err && err.message ? err.message : err)}` });
        fillConnectionSelect("source_conn_id", [], "");
        fillConnectionSelect("target_conn_id", [], "");
      }
    }

    function getTaskCards() {
      return Array.from(document.querySelectorAll("#tasks_container .task-card"));
    }

    function setTaskCardCollapsed(card, collapsed) {
      const head = card.querySelector(".task-head");
      card.classList.toggle("collapsed", !!collapsed);
      if (head) {
        head.setAttribute("aria-expanded", collapsed ? "false" : "true");
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
      const head = card.querySelector(".task-head");
      if (!head) return;
      head.addEventListener("click", () => {
        toggleTaskCardCollapsed(card);
      });
      head.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          toggleTaskCardCollapsed(card);
        }
      });
    }

    function refreshTaskCardHeaders() {
      const cards = getTaskCards();
      for (let i = 0; i < cards.length; i += 1) {
        cards[i].querySelector(".task-title").textContent = `Task #${i + 1}`;
        cards[i].querySelector(".btn-delete-task").disabled = cards.length <= 1;
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

    const LEGACY_PARTITION_MODE = "full_scan";

    function syncLegacyPartitionModeOption(modeSelect, rawMode) {
      if (!modeSelect) return;
      const mode = String(rawMode || "").trim();
      const legacyOptions = modeSelect.querySelectorAll('option[data-legacy-partition-mode="true"]');
      for (const opt of legacyOptions) {
        opt.remove();
      }
      if (mode !== LEGACY_PARTITION_MODE) return;

      const opt = document.createElement("option");
      opt.value = LEGACY_PARTITION_MODE;
      opt.textContent = "full_scan (legacy, deprecated)";
      opt.disabled = true;
      opt.setAttribute("data-legacy-partition-mode", "true");
      modeSelect.insertBefore(opt, modeSelect.firstChild);
    }

    function setTaskCardValues(card, values) {
      const sourceType = values.source_type || "table";
      const partitioningMode = values.partitioning_mode || "auto";
      card.querySelector(".task-group-id").value = values.task_group_id || "";
      card.querySelector(".source-schema").value = values.source_schema || "";
      card.querySelector(".source-table").value = values.source_table || "";
      card.querySelector(".source-type").value = sourceType === "view" ? "table" : sourceType;
      card.querySelector(".source-inline-sql").value = values.inline_sql || "";
      card.querySelector(".target-schema").value = values.target_schema || "";
      card.querySelector(".target-table").value = values.target_table || "";
      card.querySelector(".load-method").value = values.load_method || "create_if_not_exists_or_truncate";
      card.querySelector(".column-mapping-mode").value = values.column_mapping_mode || "source";
      card.querySelector(".mapping-file").value = values.mapping_file || "";
      card.querySelector(".where").value = values.where || "";
      card.querySelector(".batch-size").value = String(values.batch_size || 10000);
      card.querySelector(".partitioning-enabled").checked = !!values.partitioning_enabled;
      const partitioningModeSelect = card.querySelector(".partitioning-mode");
      syncLegacyPartitionModeOption(partitioningModeSelect, partitioningMode);
      partitioningModeSelect.value = partitioningMode;
      if (partitioningModeSelect.value !== partitioningMode) {
        partitioningModeSelect.value = "auto";
      }
      card.querySelector(".partitioning-column").value = values.partitioning_column || "";
      card.querySelector(".partitioning-parts").value = String(values.partitioning_parts || 2);
      card.querySelector(".partitioning-distinct-limit").value = String(
        asPositiveInt(values.partitioning_distinct_limit, 16)
      );
      card.querySelector(".partitioning-ranges").value = rangesToMultilineText(values.partitioning_ranges || []);
      setBindingsFromValues(card, values.bindings || []);
      toggleSourceMode(card);
      syncPartitionState(card);
    }

    function toggleSourceMode(card) {
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
      sourceTableInput.placeholder = "Type 3+ chars";
    }

    function syncPartitionState(card) {
      const enabledInput = card.querySelector(".partitioning-enabled");
      const modeSelect = card.querySelector(".partitioning-mode");
      const modeRowWrap = card.querySelector(".partitioning-mode-wrap");
      const columnWrap = card.querySelector(".partitioning-column-wrap");
      const modeWrap = card.querySelector(".partitioning-mode-field-wrap");
      const secondaryWrap = card.querySelector(".partitioning-secondary-wrap");
      const partsWrap = card.querySelector(".partitioning-parts-wrap");
      const distinctLimitWrap = card.querySelector(".partitioning-distinct-limit-wrap");
      const explicitWrap = card.querySelector(".partitioning-explicit-wrap");
      const columnInput = card.querySelector(".partitioning-column");
      const partsInput = card.querySelector(".partitioning-parts");
      const distinctLimitInput = card.querySelector(".partitioning-distinct-limit");
      const explicitInput = card.querySelector(".partitioning-ranges");

      const enabled = !!enabledInput.checked;
      const mode = String(modeSelect.value || "auto").trim() || "auto";
      const isLegacyMode = mode === LEGACY_PARTITION_MODE;
      const isExplicit = mode === "explicit";
      const isDistinct = mode === "distinct";
      const isHashMod = mode === "hash_mod";

      const setDisabled = (node, disabled) => {
        if (!node) return;
        node.disabled = !!disabled;
        node.setAttribute("aria-disabled", disabled ? "true" : "false");
      };
      const setHidden = (node, hidden) => {
        if (!node) return;
        node.classList.toggle("hidden", !!hidden);
      };

      const showMode = enabled;
      const showColumn = enabled && !isExplicit && !isLegacyMode;
      const showParts = enabled && (isDistinct || isHashMod);
      const showDistinctLimit = enabled && isDistinct;
      const showExplicit = enabled && isExplicit;

      setHidden(modeWrap, !showMode);
      setHidden(columnWrap, !showColumn);
      setHidden(partsWrap, !showParts);
      setHidden(distinctLimitWrap, !showDistinctLimit);
      setHidden(explicitWrap, !showExplicit);
      setHidden(modeRowWrap, !enabled || (!showMode && !showColumn));
      setHidden(secondaryWrap, !enabled || (!showParts && !showDistinctLimit));

      setDisabled(modeSelect, !showMode);
      setDisabled(columnInput, !showColumn);
      setDisabled(partsInput, !showParts);
      setDisabled(distinctLimitInput, !showDistinctLimit);
      setDisabled(explicitInput, !showExplicit);
    }

    function bindPartitionState(card) {
      const enabledInput = card.querySelector(".partitioning-enabled");
      const modeSelect = card.querySelector(".partitioning-mode");
      enabledInput.addEventListener("change", () => syncPartitionState(card));
      modeSelect.addEventListener("change", () => {
        syncLegacyPartitionModeOption(modeSelect, modeSelect.value);
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
      const sourceSchemaInput = card.querySelector(".source-schema");
      const sourceTableInput = card.querySelector(".source-table");
      const targetSchemaInput = card.querySelector(".target-schema");
      const targetTableInput = card.querySelector(".target-table");
      const sourceTypeSelect = card.querySelector(".source-type");

      sourceTypeSelect.addEventListener("change", () => toggleSourceMode(card));

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
        }, 220);
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
        }, 220);
      });

      targetSchemaInput.addEventListener("input", () => {
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
    }

    function addTaskCard(values = {}) {
      const template = el("task_card_template");
      const node = template.content.firstElementChild.cloneNode(true);
      bindBindingsSection(node);
      bindTaskCollapse(node);
      setTaskCardValues(node, values);
      bindTaskTabs(node);
      bindTaskAutocomplete(node);
      bindPartitionState(node);
      node.querySelector(".btn-delete-task").addEventListener("click", (ev) => {
        ev.stopPropagation();
        node.remove();
        refreshTaskCardHeaders();
      });
      setTaskCardCollapsed(node, false);
      el("tasks_container").appendChild(node);
      refreshTaskCardHeaders();
    }

    function clearAndLoadTasks(taskItems) {
      const tasks = Array.isArray(taskItems) && taskItems.length ? taskItems : [{}];
      el("tasks_container").innerHTML = "";
      for (const item of tasks) {
        addTaskCard(item || {});
      }
    }

    function applyPreloadPayload(payload, dagId) {
      el("project").value = payload.project || "";
      el("domain").value = payload.domain || "";
      el("level").value = payload.level || "";
      el("flow").value = payload.flow || "";
      syncFolderPathDisplay();
      setConnectionValue("source_conn_id", payload.source_conn_id || "");
      setConnectionValue("target_conn_id", payload.target_conn_id || "");
      clearAndLoadTasks(payload.etl_tasks || [payload]);
    }

    async function preloadByDagId(rawDagId) {
      const dagId = (rawDagId || "").trim();
      if (!dagId) {
        setUpdateModeStatus("Preload icin dag_id girin.", "warn");
        setUpdateMode(false);
        return;
      }
      const r = await studioFetch(`/api/dag-config?dag_id=${encodeURIComponent(dagId)}`);
      const data = await r.json();
      show({ status_code: r.status, ...data });
      if (!r.ok || !data.ok) {
        clearLegacyGuard();
        setUpdateModeStatus(`DAG preload basarisiz: ${data.detail || r.status}`, "warn");
        setUpdateMode(false);
        return;
      }
      if (!data.supported_for_update) {
        setLegacyGuard(data);
        setUpdateMode(false);
        return;
      }
      clearLegacyGuard();
      applyPreloadPayload(data.payload || {}, dagId);
      await loadFolderOptions();
      setUpdateModeStatus(`Update mode loaded: ${dagId}`, "ok");
      setUpdateMode(true);
    }

    function resolveInitialDagId() {
      const params = new URLSearchParams(window.location.search || "");
      const fromQuery = (params.get("dag_id") || "").trim();
      if (fromQuery) return fromQuery;

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

    async function getJson(url) {
      const r = await fetch(url);
      const data = await r.json();
      show({ status_code: r.status, ...data });
      return data;
    }

    async function postJson(url, body) {
      const r = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await r.json();
      show({ status_code: r.status, ...data });
      return data;
    }

    function collectTaskPayload(card, index) {
      const sourceType = card.querySelector(".source-type").value;
      const sourceSchemaVal = card.querySelector(".source-schema").value.trim();
      const sourceTableVal = card.querySelector(".source-table").value.trim();
      const targetSchemaVal = card.querySelector(".target-schema").value.trim();
      const targetTableVal = card.querySelector(".target-table").value.trim();
      const inlineSqlVal = card.querySelector(".source-inline-sql").value.trim();
      const manualTaskGroupId = card.querySelector(".task-group-id").value.trim();
      const partitioningMode = card.querySelector(".partitioning-mode").value;
      const partitioningDistinctLimit = asPositiveInt(
        card.querySelector(".partitioning-distinct-limit").value,
        16
      );
      const partitioningRanges = partitioningMode === "explicit"
        ? parseExplicitWhereList(card.querySelector(".partitioning-ranges").value)
        : [];
      const normalizedSourceSchema = sourceType === "sql" ? undefined : sourceSchemaVal;
      const normalizedSourceTable = sourceType === "sql" ? undefined : sourceTableVal;
      const taskGroupSourceSchema = sourceType === "sql" ? "sql" : sourceSchemaVal;
      const taskGroupSourceTable = sourceType === "sql" ? "query" : sourceTableVal;
      const generatedTaskGroupId = [
        slugify(taskGroupSourceSchema, "src"),
        slugify(taskGroupSourceTable, "table"),
        "to",
        slugify(targetSchemaVal, "tgt"),
        slugify(targetTableVal, "table"),
        `task_${index}`,
      ].join("_");
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
        task_group_id: manualTaskGroupId || generatedTaskGroupId,
        source_schema: normalizedSourceSchema,
        source_table: normalizedSourceTable,
        source_type: sourceType,
        inline_sql: sourceType === "sql" ? (inlineSqlVal || undefined) : undefined,
        target_schema: targetSchemaVal,
        target_table: targetTableVal,
        load_method: card.querySelector(".load-method").value,
        column_mapping_mode: card.querySelector(".column-mapping-mode").value,
        mapping_file: card.querySelector(".mapping-file").value.trim() || undefined,
        where: card.querySelector(".where").value.trim() || undefined,
        batch_size: Number(card.querySelector(".batch-size").value || 10000),
        partitioning_enabled: !!card.querySelector(".partitioning-enabled").checked,
        partitioning_mode: partitioningMode,
        partitioning_column: card.querySelector(".partitioning-column").value.trim() || undefined,
        partitioning_parts: Number(card.querySelector(".partitioning-parts").value || 2),
        partitioning_distinct_limit: partitioningDistinctLimit,
        partitioning_ranges: partitioningRanges,
        bindings: bindings.length ? bindings : undefined,
      };
    }

    function collectPayload() {
      const projectVal = el("project").value.trim() || "webhook";
      const domainVal = el("domain").value.trim() || "default_domain";
      const levelVal = el("level").value.trim() || "level1";
      const flowVal = el("flow").value.trim() || "src_to_stg";
      const cards = getTaskCards();
      const etlTasks = cards.map((card, idx) => collectTaskPayload(card, idx + 1));
      const firstTask = etlTasks[0] || {};
      const payload = {
        project: projectVal,
        domain: domainVal,
        level: levelVal,
        flow: flowVal,
        source_conn_id: el("source_conn_id").value,
        target_conn_id: el("target_conn_id").value,
        task_group_id: firstTask.task_group_id,
        source_schema: firstTask.source_schema,
        source_table: firstTask.source_table,
        source_type: firstTask.source_type,
        inline_sql: firstTask.inline_sql,
        target_schema: firstTask.target_schema,
        target_table: firstTask.target_table,
        load_method: firstTask.load_method,
        column_mapping_mode: firstTask.column_mapping_mode,
        mapping_file: firstTask.mapping_file,
        where: firstTask.where,
        batch_size: firstTask.batch_size,
        partitioning_enabled: firstTask.partitioning_enabled,
        partitioning_mode: firstTask.partitioning_mode,
        partitioning_column: firstTask.partitioning_column,
        partitioning_parts: firstTask.partitioning_parts,
        partitioning_distinct_limit: firstTask.partitioning_distinct_limit,
        partitioning_ranges: firstTask.partitioning_ranges,
        etl_tasks: etlTasks,
      };
      return payload;
    }

    for (const btn of document.querySelectorAll(".btn-create-dag")) {
      btn.onclick = () => postJson(studioUrl("/api/create-dag"), collectPayload());
    }
    el("btn_expand_all_tasks").onclick = () => setAllTaskCardsCollapsed(false);
    el("btn_collapse_all_tasks").onclick = () => setAllTaskCardsCollapsed(true);
    el("btn_add_task").onclick = () => addTaskCard({});
    el("btn_update_top").onclick = () => postJson(studioUrl("/api/update-dag"), collectPayload());

    el("btn_open_folder_picker").onclick = openFolderPicker;
    el("btn_close_folder_picker").onclick = closeFolderPicker;
    el("btn_cancel_folder_picker").onclick = closeFolderPicker;
    el("folder_picker_backdrop").onclick = closeFolderPicker;
    el("btn_apply_folder_picker").onclick = applyFolderPickerSelection;
    el("btn_add_project").onclick = () => addDraftFolder("project");
    el("btn_add_domain").onclick = () => addDraftFolder("domain");
    el("btn_add_level").onclick = () => addDraftFolder("level");
    el("btn_add_flow").onclick = () => addDraftFolder("flow");
    document.addEventListener("keydown", (evt) => {
      if (evt.key !== "Escape") return;
      if (el("folder_picker_modal").classList.contains("open")) {
        closeFolderPicker();
      }
    });

    async function initPage() {
      await applyAirflowThemeAssets();
      setUpdateMode(false);
      syncFolderPathDisplay();
      clearAndLoadTasks([{}]);
      // Ana form kullanimi icin once connection listesi yuklenmeli.
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
      const initialDagId = resolveInitialDagId();
      if (initialDagId) {
        await preloadByDagId(initialDagId);
      }
    }

    initPage();
