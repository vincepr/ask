import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs";
import path from "node:path";

function readNginxTemplate() {
    return fs.readFileSync(
        path.join(process.cwd(), "frontend", "nginx", "default.conf.template"),
        "utf8"
    );
}

function locationBlock(config, location) {
    const escaped = location.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const pattern = new RegExp(`location ${escaped} \\{([\\s\\S]*?)\\n    \\}`);
    const match = config.match(pattern);
    assert.notEqual(match, null, `missing nginx location ${location}`);
    return match[1];
}

test("nginx gives search requests a 120 second proxy timeout", () => {
    const config = readNginxTemplate();

    const searchBlock = locationBlock(config, "= /api/search");
    const apiBlock = locationBlock(config, "/api/");

    assert.match(searchBlock, /proxy_read_timeout 120s;/);
    assert.match(apiBlock, /proxy_read_timeout 10s;/);
});
