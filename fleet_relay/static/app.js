/* Fleet Relay: dependency-free, tenant-scoped synthetic demo console. */
(() => {
  'use strict';
  const $ = (id) => document.getElementById(id);
  const TENANTS = {
    north: { name: 'Northstar Lab', token: 'fleet-demo-north', letter: 'N' },
    south: { name: 'Solstice Lab', token: 'fleet-demo-south', letter: 'S' },
  };
  const state = {
    tenant: 'north', epoch: 0, readId: 0, controller: null,
    busy: null, loading: false, summary: null, config: null, etag: null,
    configDirty: false, configStale: false, events: [], cursor: null,
    nextCursor: null, page: 1, batches: new Map(), toastTimer: null,
  };
  const integer = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
  const timeFormatter = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  const count = (n) => Number.isFinite(Number(n)) && n !== null && n !== undefined ? integer.format(Number(n)) : '—';
  const fullTime = (value) => value ? new Date(value).toLocaleString() : 'Not set';
  const shortTime = (value) => value && !Number.isNaN(new Date(value).getTime()) ? timeFormatter.format(new Date(value)) : '—';
  const shortId = (value) => String(value || 'Unknown').length > 18 ? String(value).slice(0, 8) + '…' + String(value).slice(-4) : String(value || 'Unknown');
  const niceStatus = (value) => ({ inflight: 'In flight', in_flight: 'In flight', dead: 'Dead letter', retry: 'Retry', retried: 'Retried', delivered: 'Delivered', pending: 'Pending', claimed: 'Claimed' }[value] || String(value || 'Unknown'));
  const allowedStatus = new Set(['delivered', 'success', 'pending', 'retry', 'retried', 'inflight', 'in_flight', 'claimed', 'dead', 'failed', 'error']);

  function element(tag, className, text) {
    const el = document.createElement(tag);
    if (className) el.className = className;
    if (text !== undefined && text !== null) el.textContent = String(text);
    return el;
  }
  function icon(name) {
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    const use = document.createElementNS('http://www.w3.org/2000/svg', 'use');
    svg.classList.add('icon'); svg.setAttribute('aria-hidden', 'true');
    use.setAttribute('href', '#i-' + name); svg.append(use); return svg;
  }
  function pill(status) {
    return element('span', 'status-pill' + (allowedStatus.has(status) ? ' ' + status : ''), niceStatus(status));
  }
  function apiErrorMessage(data, fallback) {
    if (typeof data?.detail === 'string') return data.detail;
    if (Array.isArray(data?.detail)) return data.detail.map((issue) => issue.msg || 'Invalid input').join('; ');
    return typeof data?.message === 'string' ? data.message : fallback;
  }
  async function api(path, options = {}) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 15000);
    const abort = () => controller.abort();
    if (options.signal) {
      if (options.signal.aborted) controller.abort();
      else options.signal.addEventListener('abort', abort, { once: true });
    }
    try {
      const response = await fetch(path, {
        method: options.method || 'GET', cache: 'no-store', signal: controller.signal,
        headers: {
          Authorization: 'Bearer ' + TENANTS[options.tenant || state.tenant].token,
          ...(options.body === undefined ? {} : { 'Content-Type': 'application/json' }),
          ...options.headers,
        },
        ...(options.body === undefined ? {} : { body: JSON.stringify(options.body) }),
      });
      let data;
      try { data = await response.json(); } catch { data = null; }
      if (!response.ok) {
        const error = new Error(apiErrorMessage(data, 'The API returned HTTP ' + response.status + '.'));
        error.status = response.status;
        throw error;
      }
      if (!data || typeof data !== 'object') throw new Error('The API did not return a JSON object.');
      return { data, etag: response.headers.get('ETag') };
    } catch (error) {
      if (error.name === 'AbortError' && !options.signal?.aborted) {
        const timeout = new Error('The API took too long to respond.'); timeout.uncertain = true; throw timeout;
      }
      if (error instanceof TypeError) {
        const network = new Error('The local API could not be reached. Check that the server is running.'); network.uncertain = true; throw network;
      }
      throw error;
    } finally {
      clearTimeout(timer);
      options.signal?.removeEventListener('abort', abort);
    }
  }
  function toast(message, error = false) {
    clearTimeout(state.toastTimer);
    $('toast-region').replaceChildren(element('div', 'toast' + (error ? ' error' : ''), message));
    state.toastTimer = setTimeout(() => $('toast-region').replaceChildren(), error ? 14000 : 8500);
  }
  function controls() {
    const blocked = Boolean(state.busy || state.loading);
    const ready = Boolean(state.summary);
    $('refresh-button').disabled = blocked;
    $('retry-connection').disabled = blocked;
    $('ingest-button').disabled = blocked || !ready;
    $('replay-button').disabled = blocked || !state.batches.has(state.tenant) || !ready;
    $('dispatch-button').disabled = blocked || !ready;
    $('next-page').disabled = blocked || !state.nextCursor;
    $('reset-page').disabled = blocked || state.page === 1;
    $('tenant-select').disabled = Boolean(state.busy);
    $('config-fields').disabled = blocked || !state.config;
    $('save-config').disabled = blocked || !state.config || state.configStale;
    $('reload-config').disabled = blocked;
    $('ingest-button').querySelector('span').textContent = state.busy === 'ingest' ? 'Accepting signals…' : 'Ingest sample batch';
    $('dispatch-button').querySelector('span').textContent = state.busy === 'dispatch' ? 'Dispatching…' : 'Run dispatch';
    $('save-config').querySelector('span').textContent = state.busy === 'config' ? 'Saving…' : 'Save configuration';
    $('refresh-button').querySelector('span').textContent = state.loading ? 'Refreshing…' : 'Refresh';
    $('replay-button').textContent = state.busy === 'replay' ? 'Replaying batch…' : 'Replay last batch ↻';
  }
  function resetSummary() {
    for (const id of ['metric-accepted', 'metric-awaiting', 'metric-pending', 'metric-inflight', 'metric-receipts', 'metric-dead', 'event-total', 'queue-pending', 'queue-inflight', 'queue-delivered']) $(id).textContent = '—';
    $('attempts-list').replaceChildren(element('div', 'activity-empty', 'Fetching worker activity…'));
    $('metrics').setAttribute('aria-busy', 'true');
    $('attempts-list').setAttribute('aria-busy', 'true');
  }
  function renderSummary(data) {
    const outbox = data.outbox || {};
    $('metric-accepted').textContent = count(data.event_count);
    $('metric-awaiting').textContent = Number.isFinite(outbox.pending) && Number.isFinite(outbox.inflight) ? count(outbox.pending + outbox.inflight) : '—';
    $('metric-pending').textContent = count(outbox.pending);
    $('metric-inflight').textContent = count(outbox.inflight);
    $('metric-receipts').textContent = count(data.receipt_count);
    $('metric-dead').textContent = count(outbox.dead);
    $('event-total').textContent = count(data.event_count);
    $('queue-pending').textContent = count(outbox.pending);
    $('queue-inflight').textContent = count(outbox.inflight);
    $('queue-delivered').textContent = count(outbox.delivered);
    renderAttempts(data.recent_attempts || []);
  }
  function renderAttempts(attempts) {
    const target = $('attempts-list'); target.replaceChildren();
    if (!attempts.length) {
      target.append(element('div', 'activity-empty', 'No delivery attempts yet. Ingest a sample batch, then run dispatch to watch the worker accept, retry, and reject synthetic deliveries.'));
      return;
    }
    for (const attempt of attempts) {
      const row = element('div', 'attempt-row');
      row.append(element('span', 'attempt-dot' + (allowedStatus.has(attempt.outcome) ? ' ' + attempt.outcome : '')));
      const status = element('div'); status.append(pill(attempt.outcome)); row.append(status);
      const eventId = element('span', 'attempt-event', shortId(attempt.event_id)); eventId.title = attempt.event_id; row.append(eventId);
      const description = element('span', 'attempt-description');
      description.append(element('strong', '', 'Attempt ' + attempt.attempt + ' · '), document.createTextNode(attempt.detail || niceStatus(attempt.outcome)));
      row.append(description);
      const time = element('time', 'attempt-time', shortTime(attempt.at)); time.dateTime = attempt.at; time.title = fullTime(attempt.at); row.append(time);
      target.append(row);
    }
  }
  function showEventState(title, description, loading = false, failed = false) {
    $('events-body').replaceChildren(); $('events-state').hidden = false;
    $('events-state').replaceChildren(loading ? element('span', 'loading-ring') : icon(failed ? 'alert' : 'box'), element('h3', '', title), element('p', '', description));
  }
  function renderEvents(data) {
    state.events = Array.isArray(data.items) ? data.items : [];
    state.nextCursor = data.next_cursor || null;
    $('events-body').replaceChildren(); $('events-state').hidden = true;
    $('ledger-label').textContent = TENANTS[state.tenant].name;
    $('page-description').textContent = state.events.length ? 'Page ' + state.page + ' · ' + state.events.length + ' event' + (state.events.length === 1 ? '' : 's') : 'No events on this page';
    if (!state.events.length) {
      showEventState('A clean slate for ' + TENANTS[state.tenant].name, state.page > 1 ? 'There are no more events on this page. Return to the newest events.' : 'Ingest the sample batch to create three synthetic events. Every number here comes from the local API.');
      return;
    }
    for (const event of state.events) {
      const row = element('tr');
      const identifier = element('td');
      const id = element('span', 'event-id', shortId(event.event_id)); id.title = event.event_id;
      identifier.append(id, element('span', 'device-id', event.device_id));
      const kind = element('td'); const kindLabel = element('span', 'kind-label');
      kindLabel.append(icon(event.kind === 'alert' ? 'alert' : 'pulse'), document.createTextNode(event.kind || 'Event'));
      kind.append(kindLabel, element('span', 'severity-label ' + (event.severity === 'critical' ? 'critical' : event.severity === 'warning' ? 'warning' : ''), event.severity));
      const status = element('td'); status.append(pill(event.delivery?.status));
      const tries = element('td', 'numeric', count(event.delivery?.attempts));
      const at = element('td', 'time-cell', shortTime(event.received_at)); at.title = fullTime(event.received_at);
      const action = element('td'); const inspect = element('button', 'icon-button');
      inspect.type = 'button'; inspect.setAttribute('aria-label', 'Inspect event ' + event.event_id); inspect.append(icon('arrow')); inspect.addEventListener('click', () => showEvent(event)); action.append(inspect);
      row.append(identifier, kind, status, tries, at, action); $('events-body').append(row);
    }
  }
  function showEvent(event) {
    const facts = element('dl', 'event-facts');
    const values = [
      ['Event ID', event.event_id], ['Device', event.device_id], ['Sequence', event.sequence],
      ['Signal', event.kind + ' / ' + event.severity], ['Occurred', fullTime(event.occurred_at)],
      ['Accepted', fullTime(event.received_at)], ['Config version', 'v' + event.config_version],
      ['Delivery', niceStatus(event.delivery?.status)], ['Attempts', event.delivery?.attempts],
      ['Next eligible', fullTime(event.delivery?.next_attempt_at)], ['Last error', event.delivery?.last_error || 'None'],
    ];
    for (const [label, value] of values) facts.append(element('dt', '', label), element('dd', '', value));
    $('event-detail').replaceChildren(facts, element('div', 'payload-label', 'SYNTHETIC PAYLOAD'), element('pre', 'payload-code', JSON.stringify(event.payload, null, 2)));
    $('event-dialog').showModal();
  }
  function renderConfig(data, etag) {
    state.config = data.config;
    state.etag = etag || '"' + data.version + '"';
    state.configDirty = false; state.configStale = false;
    $('sample-interval').value = data.config.sample_interval_seconds;
    $('collection-enabled').checked = data.config.enabled;
    $('config-version').textContent = 'v' + data.version;
    $('config-conflict').hidden = true;
    $('config-hint').textContent = '5–3,600 seconds. Saved with If-Match ' + state.etag + ' to prevent overwriting a newer version. Existing labels are preserved.';
  }
  async function refresh({ reset = false, forceConfig = false } = {}) {
    if (reset) { state.cursor = null; state.page = 1; }
    state.controller?.abort();
    const controller = new AbortController(); state.controller = controller;
    const epoch = state.epoch; const readId = ++state.readId; const tenant = state.tenant;
    state.loading = true; controls();
    $('events-region').setAttribute('aria-busy', 'true');
    $('metrics').setAttribute('aria-busy', 'true');
    $('attempts-list').setAttribute('aria-busy', 'true');
    $('sync-status').textContent = 'Reading local API…';
    const getConfig = forceConfig || !state.config || (!state.configDirty && !state.configStale);
    const jobs = [
      ['Summary', api('/v1/summary', { tenant, signal: controller.signal })],
      ['Events', api('/v1/events?limit=10' + (state.cursor ? '&cursor=' + encodeURIComponent(state.cursor) : ''), { tenant, signal: controller.signal })],
    ];
    if (getConfig) jobs.push(['Configuration', api('/v1/config', { tenant, signal: controller.signal })]);
    const results = await Promise.allSettled(jobs.map((job) => job[1]));
    if (epoch !== state.epoch || readId !== state.readId) return;
    const errors = [];
    for (let index = 0; index < results.length; index++) {
      const result = results[index]; const name = jobs[index][0];
      if (result.status === 'rejected') {
        if (result.reason.name === 'AbortError') continue;
        errors.push(name + ': ' + result.reason.message);
        if (name === 'Events') {
          state.events = []; state.nextCursor = null;
          showEventState('The ledger is unavailable', 'Refresh to try again. No event data has been invented or replaced with a sample.', false, true);
          $('ledger-label').textContent = 'Unavailable';
          $('page-description').textContent = 'Could not load this page';
        }
        if (name === 'Configuration' && !state.config) $('config-hint').textContent = 'Configuration could not be loaded. Refresh to try again.';
        if (name === 'Summary' && !state.summary) $('attempts-list').replaceChildren(element('div', 'activity-empty', 'Worker activity is unavailable. Refresh once the API is reachable.'));
        continue;
      }
      if (name === 'Summary') { state.summary = result.value.data; renderSummary(result.value.data); }
      if (name === 'Events') renderEvents(result.value.data);
      if (name === 'Configuration') renderConfig(result.value.data, result.value.etag);
    }
    $('connection-alert').hidden = errors.length === 0;
    $('connection-detail').textContent = errors.join(' ');
    $('sync-status').textContent = errors.length ? 'Some data unavailable' : 'Updated ' + shortTime(new Date().toISOString());
    state.loading = false;
    for (const id of ['events-region', 'metrics', 'attempts-list']) $(id).setAttribute('aria-busy', 'false');
    controls();
  }
  function createBatch() {
    const id = 'run-' + (globalThis.crypto?.randomUUID ? crypto.randomUUID() : Date.now().toString(36) + '-' + Math.random().toString(36).slice(2));
    const occurred_at = new Date().toISOString();
    return ['normal', 'retry_once', 'poison'].map((mode, index) => ({
      key: id + '-' + index,
      body: {
        device_id: 'sim-' + state.tenant + '-' + String(index + 1).padStart(2, '0'),
        kind: ['heartbeat', 'metric', 'alert'][index], severity: ['info', 'warning', 'critical'][index], occurred_at,
        payload: { synthetic: true, run_id: id, delivery_mode: mode, note: ['Normal delivery', 'One synthetic transient failure', 'Synthetic permanent rejection'][index] },
      },
    }));
  }
  async function ingest(replay = false) {
    if (state.busy || state.loading) return;
    const batch = replay ? state.batches.get(state.tenant) : createBatch();
    if (!batch) return;
    state.batches.set(state.tenant, batch);
    state.busy = replay ? 'replay' : 'ingest'; controls();
    let created = 0; let duplicates = 0;
    try {
      for (const item of batch) {
        const { data } = await api('/v1/events', { method: 'POST', body: item.body, headers: { 'Idempotency-Key': item.key } });
        if (data.duplicate) duplicates++; else created++;
      }
      toast(replay ? 'Batch replayed: ' + created + ' new events, ' + duplicates + ' existing events reused. Idempotency keys stay paired with the same payload.' : 'Accepted ' + created + ' synthetic events. Run dispatch to observe normal delivery, a retry, and a poison event that exhausts all 3 attempts.');
    } catch (error) {
      toast((created || duplicates ? 'Batch stopped after ' + (created + duplicates) + ' confirmed responses. ' : '') + error.message + ' Replay the last batch to safely resume with the same idempotency keys.', true);
    } finally {
      await refresh({ reset: true }); state.busy = null; controls();
    }
  }
  async function runDispatch() {
    if (state.busy || state.loading) return;
    state.busy = 'dispatch'; controls();
    try {
      const { data } = await api('/v1/dispatch', { method: 'POST', body: { limit: 20 } });
      const result = $('dispatch-result'); result.hidden = false;
      result.replaceChildren(element('strong', '', count(data.processed) + ' processed'), document.createTextNode(' · ' + count(data.delivered) + ' delivered · ' + count(data.retried) + ' retried · ' + count(data.dead) + ' dead · ' + count(data.lease_lost) + ' lease lost'));
      $('dispatch-hint').textContent = data.retried > 0 ? 'Failed deliveries are waiting for backoff: 2s, then 4s. The poison event becomes a dead letter after its third failed attempt.' : 'Retry after 2s, then 4s (backoff capped at 30s). Poison events fail all 3 attempts, then become dead letters.';
      toast(data.processed === 0 ? 'No deliveries are ready right now. Pending retries may still be waiting for backoff.' : 'Dispatch finished: ' + data.delivered + ' delivered, ' + data.retried + ' returned for retry, ' + data.dead + ' dead letters.');
    } catch (error) {
      toast(error.message + (error.uncertain ? ' The worker outcome was not confirmed. Refresh and inspect attempts before running again.' : ''), true);
    } finally {
      await refresh(); state.busy = null; controls();
    }
  }
  async function saveConfig(event) {
    event.preventDefault();
    if (state.busy || state.loading || !state.config || state.configStale) return;
    if (!$('config-form').reportValidity()) return;
    state.busy = 'config'; controls();
    try {
      const { data, etag } = await api('/v1/config', { method: 'PUT', headers: { 'If-Match': state.etag }, body: {
        sample_interval_seconds: Number($('sample-interval').value), enabled: $('collection-enabled').checked,
        labels: state.config.labels || {},
      } });
      renderConfig(data, etag); toast('Configuration saved as version ' + data.version + '. Newly accepted events will reference this version.');
      await refresh();
    } catch (error) {
      if (error.status === 409 || error.status === 412) {
        state.configStale = true; $('config-conflict').hidden = false;
        $('config-hint').textContent = 'Save blocked: the server has a newer version than ' + state.etag + '. Your unsaved values have been kept.';
        toast('Configuration changed since you loaded it. Your draft was not saved. Load the latest version, then apply your edit.', true);
      } else toast(error.message + (error.uncertain ? ' The save outcome was not confirmed. Reload the latest configuration before trying again.' : ''), true);
    } finally { state.busy = null; controls(); }
  }
  function switchTenant() {
    if (state.busy) return;
    const tenant = $('tenant-select').value;
    if (!TENANTS[tenant]) return;
    state.controller?.abort(); state.epoch++; state.tenant = tenant;
    state.summary = null; state.config = null; state.etag = null; state.configDirty = false; state.configStale = false;
    state.cursor = null; state.nextCursor = null; state.page = 1; state.events = [];
    $('tenant-avatar').textContent = TENANTS[tenant].letter;
    $('config-version').textContent = 'v—'; $('sample-interval').value = ''; $('collection-enabled').checked = false;
    $('config-hint').textContent = 'Loading this tenant’s configuration and its ETag.';
    $('config-conflict').hidden = true; $('dispatch-result').hidden = true; $('connection-alert').hidden = true;
    $('dispatch-hint').textContent = 'Retry after 2s, then 4s (backoff capped at 30s). Poison events fail all 3 attempts, then become dead letters.';
    $('ledger-label').textContent = 'Loading'; $('page-description').textContent = 'Up to 10 events per page';
    $('toast-region').replaceChildren(); $('event-dialog').close();
    resetSummary(); showEventState('Reading ' + TENANTS[tenant].name, 'Fetching this tenant’s persisted events.', true);
    refresh({ reset: true, forceConfig: true });
  }
  $('refresh-button').addEventListener('click', () => refresh());
  $('retry-connection').addEventListener('click', () => refresh());
  $('ingest-button').addEventListener('click', () => ingest(false));
  $('replay-button').addEventListener('click', () => ingest(true));
  $('dispatch-button').addEventListener('click', runDispatch);
  $('tenant-select').addEventListener('change', switchTenant);
  $('config-form').addEventListener('submit', saveConfig);
  $('config-form').addEventListener('input', () => {
    state.configDirty = true;
    if (!state.configStale) $('config-hint').textContent = 'Unsaved changes. Save uses If-Match ' + state.etag + '; existing labels are preserved. Allowed interval: 5–3,600 seconds.';
  });
  $('reload-config').addEventListener('click', () => refresh({ forceConfig: true }));
  $('next-page').addEventListener('click', () => {
    if (!state.nextCursor || state.busy || state.loading) return;
    state.cursor = state.nextCursor; state.page++;
    refresh();
  });
  $('reset-page').addEventListener('click', () => refresh({ reset: true }));
  $('close-dialog').addEventListener('click', () => $('event-dialog').close());
  $('event-dialog').addEventListener('click', (event) => {
    if (event.target !== $('event-dialog')) return;
    const bounds = $('event-dialog').getBoundingClientRect();
    if (event.clientX < bounds.left || event.clientX > bounds.right || event.clientY < bounds.top || event.clientY > bounds.bottom) $('event-dialog').close();
  });
  function updateNavigation() {
    const hash = location.hash || '#overview';
    for (const link of document.querySelectorAll('.nav-link')) {
      const active = link.getAttribute('href') === hash;
      link.classList.toggle('active', active);
      if (active) link.setAttribute('aria-current', 'location'); else link.removeAttribute('aria-current');
    }
  }
  window.addEventListener('hashchange', updateNavigation);
  updateNavigation(); controls(); refresh({ forceConfig: true });
})();
