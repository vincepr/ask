import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";

function loadAppSandbox() {
    const source = fs
        .readFileSync(path.join(process.cwd(), "frontend", "www", "app.js"), "utf8")
        .replace("bootstrap();", "");
    const elements = new Map();

    function makeElement() {
        return {
            hidden: false,
            disabled: false,
            className: "",
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            dataset: {},
            addEventListener() {},
            classList: {
                toggle() {}
            }
        };
    }

    const document = {
        documentElement: { dataset: {} },
        getElementById(id) {
            if (!elements.has(id)) {
                elements.set(id, makeElement());
            }
            return elements.get(id);
        },
        querySelectorAll() {
            return [];
        }
    };

    const sandbox = {
        document,
        window: {
            ASK_FRONTEND_CONFIG: { embeddingMode: "tei" },
            localStorage: {
                getItem() {
                    return null;
                },
                setItem() {}
            },
            matchMedia() {
                return { matches: false };
            },
            setInterval() {
                return 0;
            }
        },
        fetch: async () => ({
            ok: true,
            status: 200,
            statusText: "OK",
            text: async () => '{"status":"healthy"}'
        }),
        console,
        JSON,
        Error,
        Number,
        String,
        Object,
        Array,
        Promise
    };

    sandbox.globalThis = sandbox;
    sandbox.window.document = document;

    vm.runInNewContext(source, sandbox, { filename: "frontend/www/app.js" });
    return sandbox;
}

function createBootstrapSandbox() {
    const elements = new Map();

    function makeElement() {
        return {
            hidden: false,
            disabled: false,
            className: "",
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            dataset: {},
            addEventListener() {},
            classList: {
                toggle() {}
            }
        };
    }

    const document = {
        documentElement: { dataset: {} },
        getElementById(id) {
            if (!elements.has(id)) {
                elements.set(id, makeElement());
            }
            return elements.get(id);
        },
        querySelectorAll() {
            return [];
        }
    };

    const intervals = [];
    const sandbox = {
        document,
        window: {
            ASK_FRONTEND_CONFIG: { embeddingMode: "tei" },
            localStorage: {
                getItem() {
                    return null;
                },
                setItem() {}
            },
            matchMedia() {
                return { matches: false };
            },
            setInterval(callback, delay) {
                intervals.push({ callback: callback.name, delay });
                return intervals.length;
            }
        },
        fetch: async () => ({
            ok: true,
            status: 200,
            statusText: "OK",
            text: async () => '{"status":"healthy"}'
        }),
        console,
        JSON,
        Error,
        Number,
        String,
        Object,
        Array,
        Promise
    };

    sandbox.setInterval = sandbox.window.setInterval;
    sandbox.globalThis = sandbox;
    sandbox.window.document = document;
    sandbox.intervals = intervals;
    return sandbox;
}

test("fetchJson keeps structured API errors", async () => {
    const sandbox = loadAppSandbox();
    sandbox.fetch = async () => ({
        ok: false,
        status: 400,
        statusText: "Bad Request",
        text: async () => '{"error":{"message":"bad input"}}'
    });

    await assert.rejects(
        sandbox.fetchJson("/api/search"),
        (error) => error instanceof Error && error.message === "bad input"
    );
});

test("fetchJson turns HTML proxy failures into HTTP status errors", async () => {
    const sandbox = loadAppSandbox();
    sandbox.fetch = async () => ({
        ok: false,
        status: 502,
        statusText: "Bad Gateway",
        text: async () => "<html><body>bad gateway</body></html>"
    });

    await assert.rejects(
        sandbox.fetchJson("/tei/health"),
        (error) => error instanceof Error && error.message === "502 Bad Gateway"
    );
});

test("renderFileTypeTable orders file types by document count descending", () => {
    const sandbox = loadAppSandbox();

    sandbox.renderFileTypeTable([
        { file_type: "md", document_count: 4 },
        { file_type: "py", document_count: 12 },
        { file_type: "rs", document_count: 12 },
        { file_type: "txt", document_count: 1 }
    ]);

    const html = sandbox.document.getElementById("file-type-table").innerHTML;
    const pyIndex = html.indexOf("<td>py</td><td>12</td>");
    const rsIndex = html.indexOf("<td>rs</td><td>12</td>");
    const mdIndex = html.indexOf("<td>md</td><td>4</td>");
    const txtIndex = html.indexOf("<td>txt</td><td>1</td>");

    assert.notEqual(pyIndex, -1);
    assert.notEqual(rsIndex, -1);
    assert.notEqual(mdIndex, -1);
    assert.notEqual(txtIndex, -1);
    assert.ok(pyIndex < rsIndex);
    assert.ok(rsIndex < mdIndex);
    assert.ok(mdIndex < txtIndex);
});

test("bootstrap does not schedule embedding health polling", () => {
    const source = fs.readFileSync(path.join(process.cwd(), "frontend", "www", "app.js"), "utf8");
    const sandbox = createBootstrapSandbox();

    vm.runInNewContext(source, sandbox, { filename: "frontend/www/app.js" });

    assert.deepEqual(
        sandbox.intervals.map((entry) => entry.callback),
        ["pollHealth", "pollStats"]
    );
});
