// test-render.js
// Robust MCP SSE + JSON-RPC smoke test for validate_and_fix.

import EventSourceModule from "eventsource";

const BASE_URL = process.env.BASE_URL ?? "https://fhir-validator-mcp-1.onrender.com";
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS ?? 120000);

// eventsource package export shape differs by version (default vs named export).
const EventSourceCtor = EventSourceModule?.EventSource ?? EventSourceModule;

const brokenPatient = {
  resourceType: "Patient",
  id: "example-broken",
  name: [{ use: "primary", family: "Smith", given: ["John"] }],
  gender: "male_patient",
  birthDate: "01/15/1985",
};

let sessionId = null;
let initialized = false;
let finished = false;
let timeoutHandle = null;

function safeExit(es, code = 0) {
  if (finished) return;
  finished = true;
  if (timeoutHandle) clearTimeout(timeoutHandle);
  if (es && typeof es.close === "function") es.close();
  process.exit(code);
}

async function getFetch() {
  if (typeof globalThis.fetch === "function") return globalThis.fetch.bind(globalThis);
  const mod = await import("node-fetch");
  return (mod.default ?? mod).bind(globalThis);
}

async function post(fetchImpl, body) {
  if (!sessionId) throw new Error("Cannot POST before sessionId is available from SSE endpoint event.");

  const response = await fetchImpl(`${BASE_URL}/messages?sessionId=${encodeURIComponent(sessionId)}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new Error(`POST ${body.method ?? "<unknown>"} failed (${response.status}): ${detail}`);
  }
}

async function run() {
  const fetchImpl = await getFetch();

  const healthRes = await fetchImpl(`${BASE_URL}/`);
  if (!healthRes.ok) throw new Error(`Health check failed: ${healthRes.status}`);
  const health = await healthRes.json();
  console.log("Health:", health.hapi_endpoint ?? "(unknown)");

  const es = new EventSourceCtor(`${BASE_URL}/sse`);

  timeoutHandle = setTimeout(() => {
    console.error(`Timeout after ${REQUEST_TIMEOUT_MS}ms waiting for MCP response`);
    safeExit(es, 1);
  }, REQUEST_TIMEOUT_MS);

  es.onopen = () => {
    console.log("SSE connected");
  };

  es.addEventListener("endpoint", async (e) => {
    try {
      const endpointUrl = new URL(e.data, BASE_URL);
      sessionId = endpointUrl.searchParams.get("sessionId");
      if (!sessionId) throw new Error(`Missing sessionId in endpoint event payload: ${e.data}`);

      console.log("Session:", sessionId);

      await post(fetchImpl, {
        jsonrpc: "2.0",
        id: 1,
        method: "initialize",
        params: {
          protocolVersion: "2024-11-05",
          capabilities: {},
          clientInfo: { name: "test-render", version: "1.1" },
        },
      });
      console.log("Sent initialize");
    } catch (err) {
      console.error("Endpoint/init error:", err.message);
      safeExit(es, 1);
    }
  });

  es.onmessage = async (e) => {
    try {
      const msg = JSON.parse(e.data);
      const id = msg?.id;

      if (id === 1 && !initialized) {
        initialized = true;
        console.log("Initialize acknowledged; sending initialized + tools/call");

        await post(fetchImpl, { jsonrpc: "2.0", method: "notifications/initialized", params: {} });
        await post(fetchImpl, {
          jsonrpc: "2.0",
          id: 2,
          method: "tools/call",
          params: {
            name: "validate_and_fix",
            arguments: { resource: JSON.stringify(brokenPatient) },
          },
        });
        console.log("Sent tools/call(validate_and_fix)");
        return;
      }

      if (id === 2) {
        console.log("\n=== RESULT ===");
        const text = msg?.result?.content?.[0]?.text;
        if (typeof text === "string") {
          try {
            console.log(JSON.stringify(JSON.parse(text), null, 2));
          } catch {
            console.log(text);
          }
        } else {
          console.log(JSON.stringify(msg, null, 2));
        }
        safeExit(es, 0);
      }
    } catch (err) {
      console.error("Message handling error:", err.message, "payload:", e.data);
    }
  };

  es.onerror = (err) => {
    // EventSource emits sparse errors; keep this visible for Render/SSE debugging.
    console.error("SSE error:", err?.message ?? err?.type ?? "unknown");
  };
}

run().catch((err) => {
  console.error("Fatal:", err.message);
  process.exit(1);
});
