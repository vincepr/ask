const config = window.ASK_FRONTEND_CONFIG || { embeddingMode: "tei" };

const state = {
    apiOnline: false,
    stats: null,
    apiHealthInFlight: false,
    statsInFlight: false
};

const apiStatusBadge = document.getElementById("api-status-badge");
const apiStatusDetail = document.getElementById("api-status-detail");
const searchForm = document.getElementById("search-form");
const ingestForm = document.getElementById("ingest-form");
const searchOutput = document.getElementById("search-output");
const ingestOutput = document.getElementById("ingest-output");

bootstrap();

function bootstrap() {
    initializeTheme();
    initializeTabs();
    initializeForms();
    updateActionState();

    pollHealth();
    pollStats();

    window.setInterval(pollHealth, 2000);
    window.setInterval(pollStats, 3000);
}

function initializeTheme() {
    const root = document.documentElement;
    const storedTheme = window.localStorage.getItem("ask-theme");
    if (storedTheme === "light" || storedTheme === "dark") {
        root.dataset.theme = storedTheme;
    } else {
        root.dataset.theme = window.matchMedia("(prefers-color-scheme: dark)").matches
            ? "dark"
            : "light";
    }

    document.getElementById("theme-toggle").addEventListener("click", () => {
        root.dataset.theme = root.dataset.theme === "dark" ? "light" : "dark";
        window.localStorage.setItem("ask-theme", root.dataset.theme);
    });
}

function initializeTabs() {
    const buttons = document.querySelectorAll(".tab-button");
    buttons.forEach((button) => {
        button.addEventListener("click", () => {
            const nextTab = button.dataset.tab;
            buttons.forEach((item) => {
                item.classList.toggle("active", item === button);
            });
            document.querySelectorAll(".tab-section").forEach((section) => {
                section.classList.toggle("active", section.id === `tab-${nextTab}`);
            });
        });
    });
}

function initializeForms() {
    searchForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        if (!state.apiOnline) {
            return;
        }

        const query = document.getElementById("search-query").value.trim();
        const limit = Number.parseInt(document.getElementById("search-limit").value, 10) || 10;
        const includeLocation = document.getElementById("search-include-location").checked;

        searchOutput.textContent = "Running search...";
        try {
            const payload = {
                query,
                limit,
                include_location: includeLocation
            };
            const response = await fetchJson("/api/search", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });
            searchOutput.textContent = JSON.stringify(response, null, 2);
        } catch (error) {
            searchOutput.textContent = formatError(error);
        }
    });

    ingestForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        if (!state.apiOnline) {
            return;
        }

        const rootPath = document.getElementById("ingest-root-path").value.trim();
        const filePattern = document.getElementById("ingest-file-pattern").value.trim();
        const useGit = document.getElementById("ingest-use-git").checked;
        const endpoint = useGit ? "/api/ingest/git" : "/api/ingest";
        const payload = { root_path: rootPath };
        if (filePattern) {
            payload.file_pattern = filePattern;
        }

        ingestOutput.textContent = "Queueing ingest...";
        try {
            const response = await fetchJson(endpoint, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });
            ingestOutput.textContent = JSON.stringify(response, null, 2);
        } catch (error) {
            ingestOutput.textContent = formatError(error);
        }
    });
}

async function pollHealth() {
    if (state.apiHealthInFlight) {
        return;
    }

    state.apiHealthInFlight = true;
    try {
        await fetchJson("/api/health");
        state.apiOnline = true;
        setStatus(
            apiStatusBadge,
            apiStatusDetail,
            "online",
            "Online",
            "API is responding"
        );
    } catch (error) {
        state.apiOnline = false;
        setStatus(
            apiStatusBadge,
            apiStatusDetail,
            "starting",
            "Starting",
            extractShortError(error)
        );
    } finally {
        state.apiHealthInFlight = false;
    }
    updateActionState();
}

async function pollStats() {
    if (!state.apiOnline) {
        renderStats(null);
        return;
    }

    if (state.statsInFlight) {
        return;
    }

    state.statsInFlight = true;
    try {
        state.stats = await fetchJson("/api/embedding/stats");
        renderStats(state.stats);
    } catch (error) {
        renderStats(null, error);
    } finally {
        state.statsInFlight = false;
    }
}

function updateActionState() {
    const disabled = !state.apiOnline;
    document.getElementById("search-submit").disabled = disabled;
    document.getElementById("ingest-submit").disabled = disabled;
}

function renderStats(stats, error) {
    if (!stats) {
        const message = error ? extractShortError(error) : "Waiting for API stats.";
        setDefinitionList(
            "document-progress",
            [["status", message]]
        );
        setDefinitionList(
            "embedding-progress",
            [["status", message]]
        );
        setDefinitionList(
            "model-stats",
            [["status", message]]
        );
        setDefinitionList("config-stats", [["status", message]]);
        document.getElementById("file-type-table").innerHTML =
            `<p class="muted">${escapeHtml(message)}</p>`;
        return;
    }

    setDefinitionList("document-progress", [
        ["total_documents", stats.total_documents],
        ["embedded_documents", stats.embedded_documents],
        ["remaining_documents", stats.remaining_documents],
        ["failed_locked_documents", stats.failed_locked_documents],
        ["progress_percent", formatPercent(stats.progress_percent)],
        [
            "estimated_hours_remaining",
            formatNullableNumber(stats.estimated_hours_remaining, 2)
        ],
        [
            "documents_embedded_last_five_minutes",
            stats.documents_embedded_last_five_minutes
        ],
        [
            "estimated_documents_per_hour",
            formatNumber(stats.estimated_documents_per_hour, 2)
        ]
    ]);

    setDefinitionList("embedding-progress", [
        ["document_embeddings_total", stats.document_embeddings_total],
        ["document_embeddings_embedded", stats.document_embeddings_embedded],
        ["document_embeddings_pending", stats.document_embeddings_pending],
        ["document_embeddings_stale", stats.document_embeddings_stale]
    ]);

    setDefinitionList("model-stats", [
        ["name", stats.model.name],
        ["id", stats.model.id],
        ["dimensions", stats.model.dimensions],
        ["chunk_size", stats.model.chunk_size],
        ["chunk_overlap", stats.model.chunk_overlap],
        ["created_at", stats.model.created_at]
    ]);

    setDefinitionList("config-stats", [
        ["data_dir", stats.config.data_dir],
        ["resource_dir", stats.config.resource_dir],
        ["embedding_mode", stats.config.embedding_mode],
        ["embedding_base_url", stats.config.embedding_base_url],
        ["embedding_max_batch_size", stats.config.embedding_max_batch_size],
        ["embedding_worker_count", stats.config.embedding_worker_count]
    ]);

    renderFileTypeTable(stats.documents_by_file_type || []);
}

function renderFileTypeTable(rows) {
    if (!rows.length) {
        document.getElementById("file-type-table").innerHTML =
            '<p class="muted">No documents indexed yet.</p>';
        return;
    }

    const orderedRows = [...rows].sort((left, right) => {
        if (right.document_count !== left.document_count) {
            return right.document_count - left.document_count;
        }
        return left.file_type.localeCompare(right.file_type);
    });

    const body = orderedRows
        .map(
            (row) =>
                `<tr><td>${escapeHtml(row.file_type)}</td><td>${escapeHtml(
                    String(row.document_count)
                )}</td></tr>`
        )
        .join("");
    document.getElementById("file-type-table").innerHTML = `
        <table>
            <thead>
                <tr>
                    <th scope="col">File type</th>
                    <th scope="col">Documents</th>
                </tr>
            </thead>
            <tbody>${body}</tbody>
        </table>
    `;
}

function setDefinitionList(elementId, entries) {
    const markup = entries
        .map(
            ([label, value]) =>
                `<dt>${escapeHtml(String(label))}</dt><dd>${escapeHtml(String(value))}</dd>`
        )
        .join("");
    document.getElementById(elementId).innerHTML = markup;
}

function setStatus(badge, detail, kind, text, message) {
    badge.className = `status-badge status-${kind}`;
    badge.textContent = text;
    detail.textContent = message;
}

async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const text = await response.text();
    const payload = parseJsonBody(text);
    if (!response.ok) {
        throw new Error(payload.error?.message || `${response.status} ${response.statusText}`);
    }
    return payload;
}

function parseJsonBody(text) {
    if (!text) {
        return {};
    }

    try {
        return JSON.parse(text);
    } catch {
        return {};
    }
}

function formatError(error) {
    return JSON.stringify({ error: extractShortError(error) }, null, 2);
}

function extractShortError(error) {
    return error instanceof Error ? error.message : "Unknown error";
}

function formatPercent(value) {
    return `${formatNumber(value, 2)}%`;
}

function formatNumber(value, digits) {
    return Number(value).toFixed(digits);
}

function formatNullableNumber(value, digits) {
    return value === null || value === undefined ? "n/a" : formatNumber(value, digits);
}

function escapeHtml(value) {
    return value
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}
