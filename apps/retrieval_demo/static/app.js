"use strict";

function persistedRunId() {
  try {
    return window.localStorage.getItem("retrieval-demo-run-id");
  } catch {
    return null;
  }
}

function persistRunId(runId) {
  try {
    window.localStorage.setItem("retrieval-demo-run-id", runId);
  } catch {
    // Storage can be disabled by browser policy; the current page still works without it.
  }
}

const state = {
  runId: persistedRunId(),
  snapshot: null,
  events: [],
  operations: new Map(),
  polling: false,
};

const element = (id) => document.getElementById(id);

function idempotencyKey(action) {
  const random = window.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random()}`;
  return `retrieval-demo:${action}:${random}`;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers ?? {}),
    },
  });
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }
  if (!response.ok) {
    const message =
      payload?.error?.message ?? payload?.detail?.message ?? "The request could not be completed.";
    const error = new Error(message);
    error.status = response.status;
    throw error;
  }
  return payload;
}

function value(object, ...paths) {
  for (const path of paths) {
    const result = path.split(".").reduce((current, part) => current?.[part], object);
    if (result !== undefined && result !== null) return result;
  }
  return undefined;
}

function setText(id, text) {
  element(id).textContent = text === undefined || text === null ? "—" : String(text);
}

function showError(error) {
  const banner = element("error-banner");
  banner.textContent = error instanceof Error ? error.message : String(error);
  banner.hidden = false;
}

function clearError() {
  element("error-banner").hidden = true;
}

function setBusy(button, busy, label) {
  if (busy) {
    button.dataset.label = button.textContent;
    button.textContent = label;
  } else if (button.dataset.label) {
    button.textContent = button.dataset.label;
  }
  button.disabled = busy;
}

function databaseSnapshot(snapshot) {
  return value(snapshot, "database", "store", "store_snapshot") ?? snapshot ?? {};
}

function controlsSnapshot(snapshot) {
  return value(snapshot, "controls", "demo_controls") ?? {};
}

function controllerSnapshot(snapshot) {
  return value(snapshot, "controller", "controller_snapshot") ?? {};
}

function lifecycleState(snapshot) {
  return String(
    value(databaseSnapshot(snapshot), "lifecycle_state", "state") ?? "unknown",
  ).toLowerCase();
}

function generation(snapshot) {
  return Number(value(databaseSnapshot(snapshot), "lifecycle_generation", "generation") ?? -1);
}

function renderSnapshot(snapshot) {
  state.snapshot = snapshot;
  const database = databaseSnapshot(snapshot);
  const controller = controllerSnapshot(snapshot);
  const controls = controlsSnapshot(snapshot);
  const lifecycle = lifecycleState(snapshot);
  const databaseGeneration = generation(snapshot);

  setText("display-name", value(database, "display_name") ?? "Northstar AI");
  setText("database-generation", databaseGeneration >= 0 ? databaseGeneration : "—");
  setText("user-count", value(database, "active_user_count", "user_count") ?? 0);
  setText("document-count", value(database, "document_count") ?? 0);
  setText("chunk-count", value(database, "chunk_count") ?? 0);
  setText(
    "controller-state",
    value(controller, "lifecycle_state", "state", "phase", "status") ?? "unavailable",
  );
  const controllerGeneration = value(controller, "lifecycle_generation", "generation");
  setText(
    "controller-generation",
    controllerGeneration === undefined ? "generation unavailable" : `generation ${controllerGeneration}`,
  );

  const badge = element("lifecycle-badge");
  badge.textContent = lifecycle;
  badge.className = `badge ${
    lifecycle === "active"
      ? "badge-active"
      : lifecycle === "deactivating" || lifecycle === "inactive"
        ? "badge-danger"
        : "badge-neutral"
  }`;

  const controllerError =
    value(snapshot, "controller_error", "temporal_error", "temporal_warning") ??
    (value(snapshot, "temporal_available") === false ? "temporarily_unavailable" : null);
  const temporalBadge = element("temporal-status");
  temporalBadge.textContent = controllerError ? "Temporal unavailable" : "Temporal connected";
  temporalBadge.className = `badge ${controllerError ? "badge-warning" : "badge-active"}`;

  const quotaPending = value(controls, "quota_once_pending");
  const quotaWaitStarted = state.events.some(
    (event) => value(event, "event_type", "type") === "quota_wait_started",
  );
  const quotaWaitCompleted = state.events.some(
    (event) => value(event, "event_type", "type") === "quota_wait_completed",
  );
  const quotaState = quotaWaitCompleted
    ? "quota recovered"
    : quotaWaitStarted || quotaPending === false
      ? "quota waiting"
      : "quota pending";
  element("quota-badge").textContent = quotaState;
  element("quota-badge").className = `badge ${quotaWaitCompleted ? "badge-active" : "badge-warning"}`;

  const heldEventObserved = state.events.some(
    (event) => value(event, "event_type", "type") === "document_commit_held",
  );
  const held = Boolean(
    value(controls, "commit_held", "held", "hold_active") ?? heldEventObserved,
  );
  const holdRequested = Boolean(value(controls, "hold_requested", "hold_before_commit"));
  const released = Boolean(value(controls, "release_requested", "released"));
  setText(
    "hold-state",
    released ? "Release requested" : held ? "Processed and held before commit" : holdRequested ? "Armed" : "Not armed",
  );

  const hasRun = Boolean(state.runId);
  element("sync").disabled = !hasRun || lifecycle !== "active";
  element("ask").disabled = !hasRun || lifecycle !== "active" || Number(value(database, "document_count") ?? 0) < 1;
  const deactivateButton = element("deactivate");
  const canDeactivate = ["active", "deactivation_failed"].includes(lifecycle);
  deactivateButton.disabled = !hasRun || !canDeactivate;
  deactivateButton.textContent =
    lifecycle === "deactivation_failed" ? "Retry deactivation" : "Deactivate store";
  element("hold").disabled = !hasRun || lifecycle !== "active" || holdRequested || held;
  const fencedLifecycle = ["deactivating", "inactive", "deactivation_failed"].includes(lifecycle);
  const canRelease =
    hasRun && fencedLifecycle && Number(databaseGeneration) >= 8 && held && !released;
  element("release").disabled = !canRelease;
  setText(
    "release-hint",
    canRelease
      ? "Generation 8 is authoritative. Releasing now demonstrates stale generation rejection."
      : "Release unlocks only after Lakebase commits the generation 8 fence.",
  );

  renderWorkflowGroups(workflowGroups(snapshot), snapshot);
  const backend = value(snapshot, "search_backend", "backend") ?? "postgres_text";
  setText("backend-badge", backend);
  setText("last-updated", `Updated ${new Date().toLocaleTimeString()}`);
  setText("connection-banner", `Lakebase authoritative · ${lifecycle} · generation ${databaseGeneration}`);
}

function workflowGroups(snapshot) {
  const explicit = value(
    snapshot,
    "workflows",
    "workflow_groups",
    "active_workflows",
    "controller.workflows",
    "controller.active_workflows",
  );
  if (explicit) return explicit;
  const controller = controllerSnapshot(snapshot);
  const groups = {};
  const controllerId = value(controller, "controller_workflow_id");
  const syncIds = value(controller, "active_sync_ids") ?? [];
  const remediationIds = value(controller, "active_remediation_ids") ?? [];
  const deactivationId = value(controller, "active_deactivation_id");
  const quotaIds = value(controller, "quota_workflow_ids") ?? [];
  const ingestionIds = [
    ...new Set(
      state.events
        .filter((event) => String(value(event, "event_type", "type") ?? "").includes("document"))
        .map((event) => value(event, "workflow_id"))
        .filter(Boolean),
    ),
  ];
  if (controllerId) groups.controller = [controllerId];
  if (syncIds.length) groups["sync fan-out"] = syncIds;
  if (quotaIds.length) groups.quota = quotaIds;
  if (ingestionIds.length) groups.ingestion = ingestionIds;
  if (remediationIds.length) groups.remediation = remediationIds;
  if (deactivationId) groups.deactivation = [deactivationId];
  return groups;
}

function renderWorkflowGroups(groups, snapshot) {
  const container = element("workflow-groups");
  container.replaceChildren();
  const entries = Array.isArray(groups)
    ? [["workflows", groups]]
    : Object.entries(groups).filter(([, workflows]) => Array.isArray(workflows) && workflows.length);
  if (!entries.length) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "Workflow activity will appear after sync begins.";
    container.append(empty);
    return;
  }
  for (const [groupName, workflows] of entries) {
    const section = document.createElement("section");
    section.className = "workflow-group";
    const heading = document.createElement("h3");
    heading.textContent = String(groupName).replaceAll("_", " ");
    section.append(heading);
    for (const workflow of workflows) {
      const workflowId = typeof workflow === "string" ? workflow : value(workflow, "workflow_id", "id");
      if (!workflowId) continue;
      const href =
        (typeof workflow === "object" ? value(workflow, "temporal_url", "url") : null) ??
        snapshot?.workflow_links?.[workflowId];
      const item = safeWorkflowLink(workflowId, href);
      section.append(item);
    }
    container.append(section);
  }
}

function safeWorkflowLink(workflowId, candidate) {
  if (candidate) {
    try {
      const url = new URL(candidate, window.location.origin);
      if (url.protocol === "https:" || url.protocol === "http:") {
        const anchor = document.createElement("a");
        anchor.className = "workflow-link";
        anchor.textContent = workflowId;
        anchor.href = url.href;
        anchor.target = "_blank";
        anchor.rel = "noreferrer";
        return anchor;
      }
    } catch {
      // Fall through to plain text when a service returns a malformed deep link.
    }
  }
  const text = document.createElement("span");
  text.className = "workflow-id";
  text.textContent = workflowId;
  return text;
}

function renderEvents(events) {
  state.events = events;
  const timeline = element("event-timeline");
  timeline.replaceChildren();
  setText("event-count", `${events.length} event${events.length === 1 ? "" : "s"}`);
  if (!events.length) {
    const empty = document.createElement("li");
    empty.className = "empty-state";
    empty.textContent = "Presentation events will appear here.";
    timeline.append(empty);
    return;
  }
  for (const event of events) {
    const eventType = String(value(event, "event_type", "type") ?? "event");
    const item = document.createElement("li");
    item.className = "event";
    if (
      eventType.includes("generation") ||
      eventType.includes("deactivation_started") ||
      eventType === "deactivation_fenced"
    ) {
      item.classList.add("event-fence");
    }
    if (eventType.includes("stale_generation_rejected")) item.classList.add("event-stale");
    const title = document.createElement("p");
    title.className = "event-title";
    title.textContent = eventType.replaceAll("_", " ");
    const metadata = document.createElement("p");
    metadata.className = "event-meta";
    const timestamp = value(event, "occurred_at", "created_at", "timestamp");
    const expected = value(event, "details.expected_generation", "expected_generation");
    const actual = value(event, "details.actual_generation", "actual_generation");
    const details = expected !== undefined && actual !== undefined ? ` · expected ${expected}, actual ${actual}` : "";
    metadata.textContent = `${timestamp ? new Date(timestamp).toLocaleTimeString() : "event"}${details}`;
    item.append(title, metadata);
    timeline.append(item);
  }
  timeline.scrollTop = timeline.scrollHeight;
}

function renderAnswer(answer) {
  const answerBox = element("answer-result");
  answerBox.replaceChildren();
  const text = document.createElement("p");
  text.textContent = value(answer, "answer", "text") ?? "No evidence-backed answer was returned.";
  answerBox.append(text);
  const backend = value(answer, "backend", "search_backend");
  if (backend) setText("backend-badge", backend);
  const citedGeneration = value(answer, "committed_generation", "lifecycle_generation", "generation");
  if (citedGeneration !== undefined) {
    const note = document.createElement("p");
    note.className = "hint";
    note.textContent = `Evidence committed at generation ${citedGeneration}`;
    answerBox.append(note);
  }
  renderCitations(value(answer, "hits", "citations", "evidence") ?? []);
}

function renderCitations(citations) {
  const list = element("citations");
  list.replaceChildren();
  for (const [index, citation] of citations.entries()) {
    const item = document.createElement("li");
    item.className = "citation";
    const heading = document.createElement("div");
    heading.className = "citation-title";
    const titleText = `${index + 1}. ${value(citation, "title", "document_title", "document_key") ?? "Evidence"}`;
    const title = safeCitationTitle(titleText, value(citation, "source_uri"));
    const score = document.createElement("span");
    const rawScore = value(citation, "score", "rank");
    score.textContent = rawScore === undefined ? "" : Number(rawScore).toFixed(3);
    heading.append(title, score);
    const snippet = document.createElement("p");
    snippet.textContent = value(citation, "snippet", "text") ?? "";
    item.append(heading, snippet);
    list.append(item);
  }
}

function safeCitationTitle(title, candidate) {
  if (candidate) {
    try {
      const url = new URL(candidate);
      if (url.protocol === "https:" || url.protocol === "http:") {
        const anchor = document.createElement("a");
        anchor.textContent = title;
        anchor.href = url.href;
        anchor.target = "_blank";
        anchor.rel = "noreferrer";
        return anchor;
      }
    } catch {
      // Render a text label when an adapter returns an invalid or unsafe citation URI.
    }
  }
  const label = document.createElement("span");
  label.textContent = title;
  return label;
}

function trackOperation(operation) {
  const operationId = value(operation, "operation_id", "id", "workflow_id");
  if (!operationId) return;
  state.operations.set(operationId, operation);
  renderOperations();
}

function renderOperations() {
  const container = element("operations");
  container.replaceChildren();
  for (const [operationId, operation] of state.operations) {
    const row = document.createElement("div");
    row.className = "operation-row";
    const id = document.createElement("span");
    id.textContent = operationId;
    const status = document.createElement("strong");
    status.textContent = value(operation, "status", "state") ?? "accepted";
    row.append(id, status);
    container.append(row);
  }
}

async function refreshOperation(operationId) {
  try {
    const operation = await api(`/api/operations/${encodeURIComponent(operationId)}`);
    state.operations.set(operationId, operation);
  } catch (error) {
    if (error.status !== 404) showError(error);
  }
}

async function refresh() {
  if (!state.runId || state.polling) return;
  state.polling = true;
  try {
    const [snapshot, events] = await Promise.all([
      api(`/api/demo/runs/${encodeURIComponent(state.runId)}/snapshot`),
      api(`/api/demo/runs/${encodeURIComponent(state.runId)}/events?limit=200`),
    ]);
    renderEvents(events.events ?? []);
    renderSnapshot(snapshot);
    await Promise.all([...state.operations.keys()].map(refreshOperation));
    renderOperations();
    clearError();
  } catch (error) {
    showError(error);
    setText("connection-banner", "Latest state unavailable; showing the last authoritative snapshot.");
  } finally {
    state.polling = false;
  }
}

async function postAction(buttonId, path, action, busyLabel, body = {}) {
  const button = element(buttonId);
  setBusy(button, true, busyLabel);
  clearError();
  try {
    const result = await api(path, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey(action) },
      body: JSON.stringify(body),
    });
    trackOperation(result);
    await refresh();
    return result;
  } catch (error) {
    showError(error);
    return null;
  } finally {
    setBusy(button, false);
    if (state.snapshot) renderSnapshot(state.snapshot);
  }
}

async function createRun() {
  const button = element("new-run");
  setBusy(button, true, "Creating…");
  clearError();
  try {
    const run = await api("/api/demo/runs", {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey("create-run") },
      body: "{}",
    });
    state.runId = value(run, "run_id", "id");
    if (!state.runId) throw new Error("The service did not return a run identifier.");
    state.events = [];
    state.operations.clear();
    persistRunId(state.runId);
    setText("run-label", `run ${state.runId}`);
    await refresh();
  } catch (error) {
    showError(error);
  } finally {
    setBusy(button, false);
  }
}

function runPath(suffix) {
  return `/api/demo/runs/${encodeURIComponent(state.runId)}${suffix}`;
}

element("new-run").addEventListener("click", createRun);
element("sync").addEventListener("click", () =>
  postAction("sync", runPath("/sync"), "sync", "Submitting…"),
);
element("deactivate").addEventListener("click", () =>
  postAction("deactivate", runPath("/deactivate"), "deactivate", "Submitting…"),
);
element("hold").addEventListener("click", () =>
  postAction("hold", runPath("/controls/hold"), "hold", "Arming…"),
);
element("release").addEventListener("click", () =>
  postAction("release", runPath("/controls/release"), "release", "Releasing…"),
);
element("ask").addEventListener("click", async () => {
  const question = element("question").value.trim();
  if (question.length < 3) {
    showError(new Error("Enter a question with at least three characters."));
    return;
  }
  const answer = await postAction("ask", runPath("/ask"), "ask", "Searching…", { question });
  if (answer) renderAnswer(answer);
});

if (state.runId) {
  setText("run-label", `run ${state.runId}`);
  void refresh();
}
window.setInterval(() => void refresh(), 1000);
