"use strict";

const $ = (id) => document.getElementById(id);
const state = {
  runId: safeStorageGet("retrieval-demo-run-id"),
  preflightWorkflowId: safeStorageGet("retrieval-demo-preflight-id"),
  snapshot: null,
  events: [],
  operations: new Map(),
  proofQuery: null,
  pollTimer: null,
};

function safeStorageGet(key) { try { return window.localStorage.getItem(key); } catch { return null; } }
function safeStorageSet(key, value) { try { window.localStorage.setItem(key, value); } catch { /* optional */ } }
function idempotencyKey(action) { return `retrieval-demo:${action}:${window.crypto?.randomUUID?.() ?? Date.now()}`; }
function setText(id, value) { const node = $(id); if (node) node.textContent = value ?? "—"; }
function get(object, ...paths) { for (const path of paths) { const found = path.split(".").reduce((current, key) => current?.[key], object); if (found !== undefined && found !== null) return found; } return undefined; }
function safeHttpUrl(value) { try { const parsed = new URL(value); return ["http:", "https:"].includes(parsed.protocol) && !parsed.username && !parsed.password ? parsed.href : null; } catch { return null; } }
function hasActiveOperation(type) { return [...state.operations.values()].some((operation) => operation.operation_type === type && !["completed", "failed", "canceled", "rejected"].includes(String(operation.status).toLowerCase())); }

async function api(path, options = {}) {
  const response = await fetch(path, { ...options, headers: { Accept: "application/json", ...(options.body ? { "Content-Type": "application/json" } : {}), ...(options.headers ?? {}) } });
  const payload = await response.json().catch(() => null);
  if (!response.ok) throw new Error(payload?.error?.message ?? payload?.detail?.message ?? `Request failed (${response.status})`);
  return payload;
}

function showError(error) { setText("error-banner", error instanceof Error ? error.message : String(error)); $("error-banner").hidden = false; }
function clearError() { $("error-banner").hidden = true; }
function setBusy(button, busy, label) { if (busy) { button.dataset.label = button.textContent; button.textContent = label; } else if (button.dataset.label) button.textContent = button.dataset.label; button.disabled = busy; }

function database(snapshot) { return get(snapshot, "database", "store", "store_snapshot") ?? snapshot?.store ?? snapshot ?? {}; }
function controls(snapshot) { return get(snapshot, "controls", "demo_controls") ?? {}; }
function lifecycle(snapshot = state.snapshot) { return String(get(database(snapshot), "lifecycle_state", "state") ?? "unknown").toLowerCase(); }
function generation(snapshot = state.snapshot) { return Number(get(database(snapshot), "lifecycle_generation", "generation") ?? -1); }
function activeSyncIds(snapshot = state.snapshot) { const ids = get(snapshot, "controller.active_sync_ids") ?? []; return Array.isArray(ids) ? ids.filter((id) => typeof id === "string" && id) : []; }

function renderWorkflowManager(snapshot) {
  const controller = snapshot?.controller ?? {};
  const syncIds = activeSyncIds(snapshot);
  const life = lifecycle(snapshot);
  const gen = generation(snapshot);
  const hasRun = Boolean(state.runId);
  const deactivationActive = Boolean(controller.active_deactivation_id) || hasActiveOperation("deactivation");
  setText("manager-controller", controller.controller_workflow_id ?? "not started");
  setText("manager-generation", gen >= 0 ? gen : "—");
  setText("manager-active-ingestion", syncIds.length);
  setText("manager-state", hasRun ? life : "No run selected");
  $("manager-end").disabled = !hasRun || syncIds.length === 0;
  $("manager-increment").disabled = !hasRun || deactivationActive || !["active", "syncing", "deactivation_failed"].includes(life);
  $("manager-scan").disabled = !hasRun || life !== "active" || syncIds.length > 0 || deactivationActive;
  if (!hasRun) setText("manager-status", "Create a run to enable workflow controls.");
  else if (deactivationActive) setText("manager-status", "Generation advancement is in progress.");
  else if (syncIds.length) setText("manager-status", `${syncIds.length} ingestion workflow${syncIds.length === 1 ? " is" : "s are"} active.`);
  else if (life === "active") setText("manager-status", "Ready for a fresh ingestion scan.");
  else setText("manager-status", `Workflow controls are limited while the store is ${life}.`);
}

function activateStep(name) {
  document.querySelectorAll(".story-rail li").forEach((item) => item.classList.toggle("active", item.dataset.step === name));
  document.querySelectorAll(".story-card").forEach((card) => card.classList.remove("active-card"));
  $(`${name}-card`)?.classList.add("active-card");
}

function inferredStep() {
  if (!state.runId) return "connect";
  const story = state.snapshot?.story_state;
  if (["complete", "late_write_rejected"].includes(story)) return "recap";
  if (["deactivating", "fenced"].includes(story)) return "reject";
  if (["held", "retrievable"].includes(story)) return "retrieve";
  if (story === "syncing") return "sync";
  const life = lifecycle();
  if (state.events.some((event) => event.event_type === "stale_generation_rejected")) return "recap";
  if (["deactivating", "inactive", "deactivation_failed"].includes(life)) return "reject";
  if (Number(get(database(state.snapshot), "document_count") ?? 0) > 0) return "retrieve";
  return "sync";
}

function renderSnapshot(snapshot) {
  state.snapshot = snapshot;
  const store = database(snapshot);
  const control = controls(snapshot);
  const life = lifecycle(snapshot);
  const gen = generation(snapshot);
  setText("run-label", state.runId ?? "not created");
  setText("display-name", get(store, "display_name") ?? "Drive retrieval");
  setText("database-generation", gen >= 0 ? gen : "—");
  setText("authority-generation", gen >= 0 ? `generation ${gen}` : "generation —");
  setText("document-count", get(store, "document_count") ?? 0);
  setText("chunk-count", get(store, "chunk_count") ?? 0);
  setText("user-count", get(store, "active_user_count") ?? 0);
  setText("lifecycle-badge", life);
  setText("controller-state", get(snapshot, "controller.lifecycle_state", "controller.phase") ?? "connected");
  setText("controller-generation", get(snapshot, "controller.lifecycle_generation") ?? gen);
  setText("temporal-status", snapshot.temporal_available === false ? "unavailable" : "connected");
  setText("backend-badge", get(snapshot, "search_backend") ?? "lakebase_hybrid");
  const quotaStarted = state.events.some((event) => event.event_type === "quota_wait_started");
  const quotaDone = state.events.some((event) => event.event_type === "quota_wait_completed");
  setText("quota-state", quotaDone ? "recovered" : quotaStarted ? "retrying" : "armed");
  const held = state.events.some((event) => event.event_type === "document_commit_held");
  const released = Boolean(control.release_requested);
  const fenced = gen >= Number(get(snapshot, "run.baseline_generation") ?? 7) + 1;
  $("sync").disabled = life !== "active" || hasActiveOperation("sync");
  $("ask").disabled = life !== "active" || Number(get(store, "document_count") ?? 0) < 1;
  $("deactivate").disabled = !(held && ["active", "deactivation_failed"].includes(life)) || hasActiveOperation("deactivation");
  $("deactivate").textContent = life === "deactivation_failed" ? "Retry deactivation" : "Commit generation fence";
  $("release").disabled = !(held && fenced && !released);
  $("load-proof").disabled = !state.runId;
  $("copy-proof").disabled = !state.proofQuery;
  setText("hold-state", released ? "released" : held ? "held before commit" : "armed");
  setText("release-hint", fenced ? "Fence committed" : "Waiting for generation fence");
  setText("late-write-result", state.events.some((event) => event.event_type === "stale_generation_rejected") ? "Lakebase rejected expected generation 7; actual generation 8." : released ? "Late writer released; waiting for Lakebase verdict." : held ? "Document is held immediately before commit." : "Waiting for the held document and fence.");
  setText("last-updated", `Updated ${new Date().toLocaleTimeString()}`);
  renderWorkflows(snapshot);
  renderWorkflowManager(snapshot);
  activateStep(inferredStep());
}

function renderWorkflows(snapshot) {
  const controller = snapshot?.controller ?? {};
  const groups = {
    controller: controller.controller_workflow_id ? [controller.controller_workflow_id] : [],
    sync: controller.active_sync_ids ?? [],
    quota: controller.quota_workflow_ids ?? [],
    ingestion: [...new Set(state.events.map((event) => event.workflow_id).filter(Boolean))],
    deactivation: controller.active_deactivation_id ? [controller.active_deactivation_id] : [],
  };
  const container = $("workflow-groups"); container.replaceChildren();
  for (const [name, ids] of Object.entries(groups)) {
    if (!ids.length) continue;
    const group = document.createElement("section"); group.className = "workflow-group";
    const title = document.createElement("b"); title.textContent = name; group.append(title);
    for (const id of ids) {
      const link = document.createElement(snapshot.workflow_links?.[id] ? "a" : "span");
      link.className = snapshot.workflow_links?.[id] ? "workflow-link" : "workflow-id";
      link.textContent = id;
      if (link instanceof HTMLAnchorElement) { link.href = snapshot.workflow_links[id]; link.target = "_blank"; link.rel = "noreferrer"; }
      group.append(link);
    }
    container.append(group);
  }
  if (!container.children.length) container.textContent = "No workflows yet.";
}

function renderEvents(events) {
  state.events = events;
  const list = $("event-timeline"); list.replaceChildren();
  for (const event of events) {
    const item = document.createElement("li"); item.className = `event ${event.event_type === "stale_generation_rejected" ? "event-stale" : ""}`;
    const labels = { quota_injected: "Demo-injected Drive throttle", quota_wait_started: "Temporal waiting durably", quota_wait_completed: "Drive scan resumed", document_commit_held: "Late writer held before commit", deactivation_fenced: "Generation 8 became authoritative", stale_generation_rejected: "Lakebase rejected the stale write", store_inactive: "Cleanup complete; store inactive" };
    const title = document.createElement("p"); title.className = "event-title"; title.textContent = labels[event.event_type] ?? String(event.event_type ?? "event").replaceAll("_", " ");
    const meta = document.createElement("p"); meta.className = "event-meta"; meta.textContent = `${event.created_at ? new Date(event.created_at).toLocaleTimeString() : "event"}${event.expected_generation !== null && event.actual_generation !== null ? ` · expected ${event.expected_generation}, actual ${event.actual_generation}` : ""}`;
    item.append(title, meta); list.append(item);
  }
  if (!events.length) { const item = document.createElement("li"); item.textContent = "No events yet."; list.append(item); }
  setText("event-count", `${events.length} event${events.length === 1 ? "" : "s"}`);
}

function renderFiles(files) {
  const list = $("source-files"); list.replaceChildren();
  for (const file of files) {
    const item = document.createElement("li");
    const details = document.createElement("span");
    const sourceUrl = safeHttpUrl(file.source_uri);
    const name = document.createElement(sourceUrl ? "a" : "b"); name.textContent = file.name;
    if (sourceUrl && name instanceof HTMLAnchorElement) { name.href = sourceUrl; name.target = "_blank"; name.rel = "noreferrer"; }
    const metadata = document.createElement("small"); metadata.textContent = `${file.mime_type ?? "unknown type"} · ${file.modified_time ?? "unknown version"}`;
    details.append(name, metadata);
    const status = document.createElement("span"); status.textContent = file.held_for_demo ? "held writer" : file.searchable ? "searchable" : "skipped"; if (file.held_for_demo) status.className = "held";
    item.append(details, status); list.append(item);
  }
  if (!files.length) { const item = document.createElement("li"); item.textContent = "No searchable files found."; list.append(item); }
}

function renderAnswer(answer) {
  setText("answer-result", answer.answer ?? "No evidence-backed answer was returned.");
  setText("backend-badge", answer.backend ?? "lakebase_hybrid");
  const list = $("citations"); list.replaceChildren();
  for (const hit of answer.hits ?? []) {
    const item = document.createElement("li"); item.className = "citation";
    const title = document.createElement("p"); title.className = "citation-title";
    const sourceUrl = safeHttpUrl(hit.source_uri);
    const titleNode = document.createElement(sourceUrl ? "a" : "span"); titleNode.textContent = hit.title;
    if (sourceUrl && titleNode instanceof HTMLAnchorElement) { titleNode.href = sourceUrl; titleNode.target = "_blank"; titleNode.rel = "noreferrer"; }
    title.append(titleNode);
    const meta = document.createElement("p"); meta.className = "citation-meta"; meta.textContent = `BM25 ${hit.keyword_rank ?? "—"} · vector ${hit.vector_rank ?? "—"} · RRF ${Number(hit.score).toFixed(4)} · generation ${hit.committed_generation ?? answer.lifecycle_generation}`;
    const text = document.createElement("p"); text.className = "citation-text"; text.textContent = hit.text;
    item.append(title, meta, text); list.append(item);
  }
}

function setReadiness(id, ready) { const node = $(id); node.textContent = ready ? "ready" : "blocked"; node.className = ready ? "ready" : "blocked"; }
async function loadPlatformReadiness() {
  try {
    const response = await fetch("/readyz", { headers: { Accept: "application/json" } });
    const payload = await response.json();
    const databaseReady = Boolean(get(payload, "database_ready", "database.ready") && get(payload, "migrations_ready", "migrations.current"));
    const temporalReady = Boolean(get(payload, "temporal_ready", "temporal.ready"));
    const searchReady = Boolean(get(payload, "search_ready") ?? (payload.ready && get(payload, "details.search_backend") === "lakebase_hybrid"));
    const embeddingsReady = Boolean(get(payload, "embeddings_ready") ?? payload.ready);
    setReadiness("ready-lakebase", databaseReady); setReadiness("ready-search", searchReady); setReadiness("ready-embeddings", embeddingsReady); setReadiness("ready-temporal", temporalReady);
    $("preflight").disabled = !(response.ok && payload.ready);
    setText("preflight-status", response.ok && payload.ready ? "Platform ready. Inspect the stable folder." : "Preflight blocked until every dependency is ready.");
  } catch {
    for (const id of ["ready-lakebase", "ready-search", "ready-embeddings", "ready-temporal"]) setReadiness(id, false);
    $("preflight").disabled = true;
    setText("preflight-status", "Platform readiness is unavailable.");
  }
}

async function pollPreflight() {
  if (!state.preflightWorkflowId) return;
  const payload = await api(`/api/preflight/${encodeURIComponent(state.preflightWorkflowId)}`);
  setText("preflight-status", payload.status === "completed" ? `Connected · ${payload.result?.files?.length ?? 0} files · ${payload.result?.folders_scanned ?? 0} folders` : `Temporal preflight: ${payload.status}`);
  if (payload.status === "completed") { renderFiles(payload.result?.files ?? []); $("new-run").disabled = false; activateStep("sync"); }
}

async function refresh() {
  try {
    if (state.preflightWorkflowId) await pollPreflight();
    if (!state.runId) return;
    const [snapshot, eventPayload] = await Promise.all([api(`/api/demo/runs/${state.runId}/snapshot`), api(`/api/demo/runs/${state.runId}/events?limit=500`)]);
    renderEvents(eventPayload.events ?? []); renderSnapshot(snapshot);
    for (const [id] of state.operations) {
      const operation = await api(`/api/operations/${encodeURIComponent(id)}`); state.operations.set(id, operation);
    }
    renderOperations();
  } catch (error) { showError(error); }
}

function renderOperations() {
  const container = $("operations"); container.replaceChildren();
  for (const operation of state.operations.values()) { const row = document.createElement("div"); row.className = "operation-row"; const name = document.createElement("span"); name.textContent = operation.operation_type; const status = document.createElement("b"); status.textContent = operation.status; row.append(name, status); container.append(row); }
  if (!container.children.length) container.textContent = "No operations yet.";
}

async function runAction(button, label, action) { clearError(); setBusy(button, true, label); try { await action(); } catch (error) { showError(error); } finally { setBusy(button, false); await refresh(); } }

$("preflight").addEventListener("click", () => runAction($("preflight"), "Inspecting…", async () => { const result = await api("/api/preflight", { method: "POST", headers: { "Idempotency-Key": idempotencyKey("preflight") } }); state.preflightWorkflowId = result.workflow_id; safeStorageSet("retrieval-demo-preflight-id", result.workflow_id); setText("preflight-status", "Temporal preflight started."); }));
$("new-run").addEventListener("click", () => runAction($("new-run"), "Creating…", async () => { const run = await api("/api/demo/runs", { method: "POST", headers: { "Idempotency-Key": idempotencyKey("new-run") } }); state.runId = run.run_id; state.events = []; state.operations.clear(); safeStorageSet("retrieval-demo-run-id", run.run_id); activateStep("sync"); }));
$("sync").addEventListener("click", () => runAction($("sync"), "Starting…", async () => { const operation = await api(`/api/demo/runs/${state.runId}/sync`, { method: "POST", headers: { "Idempotency-Key": idempotencyKey("sync") } }); state.operations.set(operation.operation_id, operation); }));
$("manager-end").addEventListener("click", () => runAction($("manager-end"), "Ending…", async () => { const workflowId = activeSyncIds()[0]; if (!workflowId) throw new Error("No active ingestion workflow to end."); await api(`/api/demo/runs/${state.runId}/workflows/end`, { method: "POST", headers: { "Idempotency-Key": idempotencyKey("end-workflow") }, body: JSON.stringify({ workflow_id: workflowId }) }); setText("manager-status", "Cancellation requested. Waiting for the workflow to close."); }));
$("manager-increment").addEventListener("click", () => runAction($("manager-increment"), "Advancing…", async () => { const operation = await api(`/api/demo/runs/${state.runId}/deactivate`, { method: "POST", headers: { "Idempotency-Key": idempotencyKey("increment-generation") } }); state.operations.set(operation.operation_id, operation); setText("manager-status", "Generation fence submitted."); }));
$("manager-scan").addEventListener("click", () => runAction($("manager-scan"), "Scanning…", async () => { const operation = await api(`/api/demo/runs/${state.runId}/sync`, { method: "POST", headers: { "Idempotency-Key": idempotencyKey("fresh-ingestion-scan") } }); state.operations.set(operation.operation_id, operation); setText("manager-status", "Fresh ingestion scan submitted."); }));
$("ask").addEventListener("click", () => runAction($("ask"), "Retrieving…", async () => { const answer = await api(`/api/demo/runs/${state.runId}/ask`, { method: "POST", headers: { "Idempotency-Key": idempotencyKey("ask") }, body: JSON.stringify({ question: $("question").value }) }); renderAnswer(answer); activateStep("retrieve"); }));
$("deactivate").addEventListener("click", () => runAction($("deactivate"), "Fencing…", async () => { const operation = await api(`/api/demo/runs/${state.runId}/deactivate`, { method: "POST", headers: { "Idempotency-Key": idempotencyKey("deactivate") } }); state.operations.set(operation.operation_id, operation); activateStep("deactivate"); }));
$("release").addEventListener("click", () => runAction($("release"), "Releasing…", async () => { await api(`/api/demo/runs/${state.runId}/controls/release`, { method: "POST", headers: { "Idempotency-Key": idempotencyKey("release") } }); activateStep("reject"); }));
$("load-proof").addEventListener("click", () => runAction($("load-proof"), "Loading…", async () => { const proof = await api(`/api/demo/runs/${state.runId}/proof`); state.proofQuery = proof.proof_query ?? null; $("copy-proof").disabled = !state.proofQuery; $("proof-result").textContent = JSON.stringify(proof, null, 2); activateStep("recap"); }));
$("copy-proof").addEventListener("click", () => runAction($("copy-proof"), "Copying…", async () => { if (!state.proofQuery) throw new Error("Load Lakebase proof first."); await navigator.clipboard.writeText(state.proofQuery); }));

for (const item of document.querySelectorAll(".story-rail li")) item.addEventListener("click", () => { activateStep(item.dataset.step); $(`${item.dataset.step}-card`)?.scrollIntoView({ behavior: "smooth", block: "center" }); });
function setDrawer(open) { $("technical-drawer").classList.toggle("open", open); $("technical-drawer").setAttribute("aria-hidden", String(!open)); $("drawer-toggle").setAttribute("aria-expanded", String(open)); }
$("drawer-toggle").addEventListener("click", () => setDrawer(!$("technical-drawer").classList.contains("open")));
$("drawer-close").addEventListener("click", () => setDrawer(false));

async function loadTooling() { try { const links = await api("/api/demo/tooling"); for (const [id, key] of [["drive-link", "google_drive"], ["temporal-link", "temporal"], ["lakebase-link", "lakebase"]]) { if (links[key]) $(id).href = links[key]; else { $(id).removeAttribute("href"); $(id).setAttribute("aria-disabled", "true"); } } } catch { /* drawer still works */ } }
loadPlatformReadiness(); loadTooling(); refresh(); state.pollTimer = window.setInterval(refresh, 1200);
