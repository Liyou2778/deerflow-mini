'use strict';

/* deerflow-mini single-page client: no build step, no framework. */

const EVENT_TYPES = [
  'run_start',
  'node_start',
  'node_end',
  'token',
  'tool_call',
  'tool_result',
  'citation',
  'context_trim',
  'run_end',
  'run_cancelled',
  'stream_reset',
];

const $ = (selector) => document.querySelector(selector);

const state = {
  health: null,
  sessions: [],
  sessionId: null,
  sessionTitle: 'deerflow-mini',
  runId: null,
  lastSeq: 0,
  expectedSeq: 1,
  es: null,
  reconnectTimer: null,
  reconnectAttempts: 0,
  reconnects: 0,
  seqGaps: 0,
  nodes: new Map(),
  citations: [],
  answerText: '',
  status: 'idle',
  terminal: true,
  runStartedAt: null,
  contextBudget: null,
  trace: null,
  bufferCapacity: 500,
  evicted: 0,
};

/* ---------- helpers ---------- */

function escapeHtml(value) {
  return String(value == null ? '' : value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function fmtMs(ms) {
  if (ms == null || Number.isNaN(ms)) return '—';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

function fmtSeconds(seconds) {
  if (!seconds) return '—';
  return `${seconds.toFixed(1)}s`;
}

function nowSeconds() {
  return Date.now() / 1000;
}

function relTime(ts) {
  if (!ts) return '—';
  const diff = Math.max(0, Date.now() / 1000 - ts);
  if (diff < 60) return `${Math.round(diff)}s ago`;
  if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
  return `${Math.round(diff / 3600)}h ago`;
}

function isBusy() {
  return state.status === 'running' || state.status === 'connecting' || state.status === 'cancelling';
}

async function api(path, options = {}) {
  const init = { ...options, headers: { 'Content-Type': 'application/json', ...(options.headers || {}) } };
  const response = await fetch(path, init);
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = await response.json();
      detail = body.detail || JSON.stringify(body);
    } catch (_) {
      /* ignore */
    }
    throw new Error(`${response.status} ${detail}`);
  }
  return response.json();
}

function setHint(text, isError = false) {
  const hint = $('#composer-hint');
  hint.textContent = text;
  hint.classList.toggle('error', Boolean(isError));
}

function setConn(kind, label) {
  const node = $('#conn-state');
  node.dataset.state = kind;
  $('#conn-label').textContent = label;
}

/* ---------- render: sessions ---------- */

function renderSessions() {
  const list = $('#session-list');
  if (!state.sessions.length) {
    list.innerHTML = '<li class="session-item"><div class="s-meta">No sessions yet</div></li>';
    return;
  }
  list.innerHTML = state.sessions
    .map(
      (session) => `
      <li class="session-item ${session.id === state.sessionId ? 'active' : ''}" data-session-id="${escapeHtml(session.id)}">
        <div class="s-title">${escapeHtml(session.title)}</div>
        <div class="s-meta">${session.run_count} run${session.run_count === 1 ? '' : 's'} · ${relTime(session.created_at)}</div>
      </li>`
    )
    .join('');
}

function renderSessionHeader() {
  $('#session-title').textContent = state.sessionTitle;
  const llm = state.health ? state.health.llm : null;
  const provider = llm ? `${llm.provider} · ${llm.model}` : 'mock';
  const researchers = state.health ? state.health.researcher_count : 3;
  $('#session-meta').textContent = `planner → ${researchers} parallel researchers → writer · ${provider}`;
}

/* ---------- render: timeline ---------- */

function nodesByKind(kind) {
  const nodes = [...state.nodes.values()].filter((node) => node.kind === kind);
  if (kind === 'researcher') {
    nodes.sort((a, b) => {
      const na = parseInt(String(a.id).replace(/\D/g, ''), 10) || 0;
      const nb = parseInt(String(b.id).replace(/\D/g, ''), 10) || 0;
      return na - nb;
    });
  }
  return nodes;
}

function nodeCard(node) {
  const icon = { planner: 'P', researcher: 'R', writer: 'W' }[node.kind] || '·';
  const tokens = `${node.tokensIn || 0}→${node.tokensOut || 0}`;
  const toolChips = (node.tools || [])
    .map((tool) => `<span class="tool-chip ${tool.state}">${escapeHtml(tool.name)}</span>`)
    .join('');
  return `
    <div class="node ${node.status}" data-node-card="${escapeHtml(node.id)}">
      <div class="node-top">
        <span class="node-kind">${icon}</span>
        <span class="node-label" title="${escapeHtml(node.label)}">${escapeHtml(node.label)}</span>
        <span class="pill ${node.status}">${node.status}</span>
      </div>
      <div class="node-meta">
        <span class="node-dur" data-node-dur="${escapeHtml(node.id)}">${node.status === 'running' ? '…' : fmtMs(node.durationMs)}</span>
        <span title="estimated tokens in → out">${tokens} tok</span>
      </div>
      ${node.detail || toolChips ? `<div class="node-detail">${escapeHtml(node.detail || '')} ${toolChips}</div>` : ''}
    </div>`;
}

function renderTimeline() {
  const timeline = $('#timeline');
  const groups = [
    { kind: 'planner', label: 'Plan' },
    { kind: 'researcher', label: 'Research' },
    { kind: 'writer', label: 'Write' },
  ];
  const blocks = groups
    .map((group) => {
      const nodes = nodesByKind(group.kind);
      if (!nodes.length) return '';
      const count = group.kind === 'researcher' ? ` ×${nodes.length}` : '';
      return `<div class="tl-group">
        <span class="tl-group-label">${group.label}${count}</span>
        <div class="tl-cards">${nodes.map(nodeCard).join('')}</div>
      </div>`;
    })
    .filter(Boolean);

  if (!blocks.length) {
    timeline.innerHTML = '<div class="empty">Run a query to watch the planner, researchers and writer execute.</div>';
  } else {
    timeline.innerHTML = blocks.join('');
  }

  const runMeta = $('#run-meta');
  if (!state.runId) {
    runMeta.textContent = '';
  } else {
    const elapsed = state.status === 'running' ? fmtSeconds(nowSeconds() - state.runStartedAt) : '';
    runMeta.textContent = `${state.runId} · seq ${state.lastSeq}${elapsed ? ` · ${elapsed}` : ''}`;
  }
  updateComposer();
}

/* ---------- render: answer + citations ---------- */

function inline(text) {
  let output = escapeHtml(text);
  output = output.replace(/`([^`]+)`/g, '<code>$1</code>');
  output = output.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  output = output.replace(/\[(\d+)\]/g, '<sup class="cite" data-index="$1">$1</sup>');
  return output;
}

function renderMarkdown(text) {
  const lines = String(text).split('\n');
  const out = [];
  let list = null;
  const closeList = () => {
    if (list) {
      out.push(`</${list}>`);
      list = null;
    }
  };
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) {
      closeList();
      continue;
    }
    let match;
    if ((match = trimmed.match(/^(#{1,4})\s+(.*)$/))) {
      closeList();
      const level = Math.min(4, match[1].length + 1);
      out.push(`<h${level}>${inline(match[2])}</h${level}>`);
    } else if ((match = trimmed.match(/^[-*]\s+(.*)$/))) {
      if (list !== 'ul') {
        closeList();
        out.push('<ul>');
        list = 'ul';
      }
      out.push(`<li>${inline(match[1])}</li>`);
    } else if ((match = trimmed.match(/^\d+\.\s+(.*)$/))) {
      if (list !== 'ol') {
        closeList();
        out.push('<ol>');
        list = 'ol';
      }
      out.push(`<li>${inline(match[1])}</li>`);
    } else {
      closeList();
      out.push(`<p>${inline(trimmed)}</p>`);
    }
  }
  closeList();
  return out.join('');
}

function renderAnswer() {
  const element = $('#answer');
  if (!state.answerText) {
    element.innerHTML = state.runId
      ? '<div class="answer-placeholder"><p>Waiting for the first writer token…</p></div>'
      : `<div class="answer-placeholder">
          <h3>Ask a research question</h3>
          <p>The mock LLM is deterministic and fully offline. Try <code>How do multi-agent research pipelines handle context budgets and resumable streams?</code></p>
        </div>`;
    return;
  }
  const elementBottom = element.scrollHeight - element.scrollTop - element.clientHeight < 80;
  const streaming = state.status === 'running';
  element.innerHTML = `<div class="${streaming ? 'streaming-caret' : ''}">${renderMarkdown(state.answerText)}</div>`;
  if (elementBottom) element.scrollTop = element.scrollHeight;

  const answerStreamState = $('#answer-stream-state');
  if (streaming) answerStreamState.textContent = `streaming · seq ${state.lastSeq}`;
  else if (state.status === 'cancelled') answerStreamState.textContent = 'cancelled — partial answer';
  else if (state.status === 'failed') answerStreamState.textContent = 'run failed';
  else if (state.status === 'done') answerStreamState.textContent = 'complete';
  else answerStreamState.textContent = 'idle';
}

function renderCitations() {
  const element = $('#citations');
  if (!state.citations.length) {
    element.innerHTML = '';
    return;
  }
  element.innerHTML = state.citations
    .map((citation) => {
      let host = citation.url;
      try {
        host = new URL(citation.url).host;
      } catch (_) {
        /* keep raw */
      }
      return `
        <a class="citation-card" id="cite-${citation.index}" href="${escapeHtml(citation.url)}" target="_blank" rel="noopener noreferrer">
          <div class="c-top"><span class="c-idx">[${citation.index}]</span><span class="c-title">${escapeHtml(citation.title)}</span></div>
          <div class="c-snip">${escapeHtml(citation.snippet)}</div>
          <div class="c-url">${escapeHtml(host)}</div>
        </a>`;
    })
    .join('');
}

/* ---------- render: trace ---------- */

function nodeStatusClass(status) {
  return ['running', 'done', 'failed', 'cancelled'].includes(status) ? `st-${status}` : '';
}

function renderTrace() {
  const nodes = state.trace ? state.trace.nodes || [] : [...state.nodes.values()];
  const tokensOut = nodes.reduce((sum, node) => sum + (node.tokens_out || node.tokensOut || 0), 0);

  let durationMs = null;
  if (state.trace && state.trace.duration_ms != null) durationMs = state.trace.duration_ms;
  else if (state.runStartedAt && isBusy()) durationMs = (nowSeconds() - state.runStartedAt) * 1000;
  else if (state.runStartedAt && state.status === 'cancelled') durationMs = null;

  $('#tile-events').textContent = state.trace ? state.trace.event_count : state.lastSeq;
  $('#tile-duration').textContent = state.status === 'running' ? fmtSeconds(nowSeconds() - state.runStartedAt) : fmtMs(durationMs);
  $('#tile-tokens').textContent = tokensOut;
  $('#tile-citations').textContent = state.citations.length;

  const body = $('#trace-body');
  if (!nodes.length) {
    body.innerHTML = '<tr class="trace-empty"><td colspan="4">no run yet</td></tr>';
  } else {
    body.innerHTML = nodes
      .map((node) => {
        const duration = node.duration_ms != null ? node.duration_ms : node.durationMs;
        const tokens = node.tokens_out != null ? node.tokens_out : node.tokensOut || 0;
        return `<tr>
          <td title="${escapeHtml(node.label || node.id)}">${escapeHtml(node.id)}</td>
          <td class="${nodeStatusClass(node.status)}">${escapeHtml(node.status)}</td>
          <td>${duration != null ? Math.round(duration) : '—'}</td>
          <td>${tokens}</td>
        </tr>`;
      })
      .join('');
  }

  const statusLabel = state.status === 'idle' ? '—' : state.status;
  $('#trace-status').textContent = statusLabel;
  renderBudget();
  renderStreamInfo();
}

function renderBudget() {
  const panel = $('#budget-panel');
  const budget = state.trace ? state.trace.context_budget : state.contextBudget;
  if (!budget || !budget.budget_tokens) {
    panel.innerHTML = '<div class="muted">Waiting for researcher fan-in…</div>';
    $('#budget-strategy').textContent = '—';
    return;
  }
  const ratio = Math.min(1, (budget.tokens_after || 0) / budget.budget_tokens);
  $('#budget-strategy').textContent = budget.strategy || '—';
  panel.innerHTML = `
    <div class="budget-row"><span>budget</span><span>${budget.budget_tokens} tok</span></div>
    <div class="budget-row"><span>before → after</span><span>${budget.tokens_before} → ${budget.tokens_after}</span></div>
    <div class="budget-row"><span>per child</span><span>${budget.per_item_tokens} tok</span></div>
    <div class="budget-bar"><div style="width:${(ratio * 100).toFixed(1)}%"></div></div>
    <div class="budget-note">${budget.trimmed_items || 0} of ${budget.items || 0} child summaries trimmed · saved ${budget.tokens_saved || 0} tokens (${escapeHtml(budget.token_estimator || 'estimate')})</div>`;
}

function renderStreamInfo() {
  $('#last-event-id').textContent = state.lastSeq;
  $('#seq-gaps').textContent = state.seqGaps;
  $('#reconnects').textContent = state.reconnects;
  $('#buffer-label').textContent = `buffer ${state.bufferCapacity}${state.evicted ? ` · reset ${state.evicted}` : ''}`;
}

function pushEventLog(event) {
  if (event.type === 'token') return;
  const log = $('#event-log');
  const placeholder = log.querySelector('.event-log-empty');
  if (placeholder) placeholder.remove();
  const item = document.createElement('li');
  item.innerHTML = `<span class="ev-seq">#${event.seq}</span><span class="ev-type">${escapeHtml(event.type)}</span><span class="ev-node">${escapeHtml(event.node_id || '')}</span>`;
  log.prepend(item);
  while (log.children.length > 14) log.removeChild(log.lastChild);
}

function resetEventLog(message) {
  $('#event-log').innerHTML = `<li class="event-log-empty">${escapeHtml(message)}</li>`;
}

/* ---------- SSE ---------- */

function closeStream() {
  if (state.es) {
    state.es.close();
    state.es = null;
  }
  if (state.reconnectTimer) {
    clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
  }
}

function scheduleReconnect() {
  if (state.terminal || !state.runId) return;
  state.reconnectAttempts += 1;
  if (state.reconnectAttempts > 8) {
    state.status = 'failed';
    state.terminal = true;
    setConn('failed', 'stream lost');
    updateComposer();
    return;
  }
  setConn('resuming', `resume @${state.lastSeq} in 0.8s`);
  state.reconnectTimer = setTimeout(() => {
    state.reconnectTimer = null;
    if (!state.terminal && state.runId) {
      state.reconnects += 1;
      renderStreamInfo();
      connectStream();
    }
  }, 800);
}

function connectStream() {
  closeStream();
  if (!state.runId) return;
  const url = `/api/runs/${encodeURIComponent(state.runId)}/events?last_event_id=${state.lastSeq}`;
  const source = new EventSource(url);
  state.es = source;
  for (const type of EVENT_TYPES) {
    source.addEventListener(type, (event) => {
      try {
        handleEvent(type, JSON.parse(event.data));
      } catch (error) {
        console.error('bad SSE payload', error);
      }
    });
  }
  source.onopen = () => {
    setConn('running', state.lastSeq > 0 ? `resumed @${state.lastSeq}` : 'live');
  };
  source.onerror = () => {
    if (state.es !== source) return;
    source.close();
    state.es = null;
    if (state.terminal) return;
    setConn('resuming', 'reconnecting…');
    scheduleReconnect();
  };
}

function handleEvent(type, event) {
  if (type === 'stream_reset') {
    state.evicted += 1;
    state.nodes = new Map();
    state.citations = [];
    state.answerText = '';
    state.lastSeq = Math.max(0, (event.oldest_seq || 1) - 1);
    renderTimeline();
    renderCitations();
    renderAnswer();
    renderTrace();
    return;
  }

  if (typeof event.seq === 'number') {
    if (event.seq <= state.lastSeq) return;
    if (state.lastSeq > 0 && event.seq > state.lastSeq + 1) {
      state.seqGaps += event.seq - state.lastSeq - 1;
    }
    state.lastSeq = event.seq;
  }

  pushEventLog(event);

  switch (type) {
    case 'run_start':
      state.runStartedAt = event.ts || nowSeconds();
      state.status = 'running';
      state.terminal = false;
      state.reconnectAttempts = 0;
      setConn('running', 'live');
      break;

    case 'node_start': {
      state.nodes.set(event.node_id, {
        id: event.node_id,
        label: event.label || event.node_id,
        kind: event.kind,
        parent: event.parent,
        status: 'running',
        startedAt: event.started_at || nowSeconds(),
        durationMs: null,
        tokensIn: 0,
        tokensOut: 0,
        detail: '',
        tools: [],
      });
      break;
    }

    case 'node_end': {
      const node = state.nodes.get(event.node_id);
      if (node) {
        node.status = event.status;
        node.durationMs = event.duration_ms;
        node.tokensIn = event.tokens_in || node.tokensIn;
        node.tokensOut = event.tokens_out || node.tokensOut;
        node.detail = event.detail || node.detail;
      }
      break;
    }

    case 'token': {
      const node = state.nodes.get(event.node_id);
      if (node) node.tokensOut = (node.tokensOut || 0) + (event.tokens || 0);
      if (event.channel === 'answer') {
        state.answerText += event.text || '';
        renderAnswer();
      }
      break;
    }

    case 'tool_call': {
      const node = state.nodes.get(event.node_id);
      if (node) {
        const args = event.args || {};
        const target = args.query || args.url || '';
        node.tools = node.tools || [];
        node.tools.push({ name: event.tool, state: 'running', target });
        node.detail = `${event.tool}: ${String(target).slice(0, 70)}`;
      }
      break;
    }

    case 'tool_result': {
      const node = state.nodes.get(event.node_id);
      if (node && node.tools && node.tools.length) {
        const tool = [...node.tools].reverse().find((item) => item.name === event.tool && item.state === 'running');
        if (tool) tool.state = 'done';
        node.detail = `${event.tool} · ${fmtMs(event.duration_ms)}${event.results ? ` · ${event.results.length} hits` : ''}`;
      }
      break;
    }

    case 'citation':
      if (!state.citations.some((item) => item.index === event.index)) {
        state.citations.push({
          index: event.index,
          title: event.title,
          url: event.url,
          snippet: event.snippet,
          researcher: event.researcher,
        });
        state.citations.sort((a, b) => a.index - b.index);
        renderCitations();
      }
      break;

    case 'context_trim':
      state.contextBudget = {
        strategy: event.strategy,
        budget_tokens: event.budget_tokens,
        per_item_tokens: event.per_item_tokens,
        tokens_before: event.tokens_before,
        tokens_after: event.tokens_after,
        trimmed_items: event.trimmed_items,
        tokens_saved: event.tokens_saved,
        items: event.items,
        token_estimator: event.token_estimator || 'ceil(len/4)',
      };
      break;

    case 'run_end':
      finishRun(event, 'done');
      break;

    case 'run_cancelled':
      finishRun(event, 'cancelled');
      break;

    default:
      break;
  }

  renderTimeline();
  renderTrace();
}

function finishRun(event, fallbackStatus) {
  state.terminal = true;
  state.status = event.status || fallbackStatus;
  if (fallbackStatus === 'cancelled') state.status = 'cancelled';
  if (typeof event.answer === 'string' && event.answer) state.answerText = event.answer;
  if (typeof event.partial_answer === 'string' && event.partial_answer) state.answerText = event.partial_answer;
  if (event.error) {
    state.answerText = state.answerText || `Run failed:\n\n${event.error}`;
  }
  closeStream();
  setConn(state.status === 'done' ? 'done' : state.status === 'cancelled' ? 'cancelled' : 'failed',
    state.status === 'done' ? 'complete' : state.status);
  setHint(`run ${state.runId} · ${state.status} · ${state.lastSeq} events`, state.status === 'failed');
  renderAnswer();
  renderTimeline();
  renderTrace();
  updateComposer();
  void refreshTrace();
  void refreshSessions();
}

/* ---------- data loading ---------- */

async function refreshTrace() {
  if (!state.runId) return;
  try {
    state.trace = await api(`/api/runs/${encodeURIComponent(state.runId)}/trace`);
    state.bufferCapacity = state.trace.buffer.capacity || state.bufferCapacity;
    renderTrace();
    renderTimeline();
  } catch (error) {
    console.warn('trace refresh failed', error);
  }
}

async function refreshSessions() {
  try {
    const data = await api('/api/sessions');
    state.sessions = data.sessions || [];
    renderSessions();
  } catch (error) {
    console.warn('session refresh failed', error);
  }
}

async function loadHealth() {
  try {
    state.health = await api('/api/health');
    state.bufferCapacity = state.health.event_buffer_size || 500;
    const llm = state.health.llm;
    $('#health-info').textContent = `${llm.provider} · ${llm.model} · buffer ${state.bufferCapacity}`;
    renderSessionHeader();
    renderStreamInfo();
  } catch (error) {
    $('#health-info').textContent = 'backend unreachable';
    console.warn('health failed', error);
  }
}

async function loadRunSnapshot(runId) {
  const data = await api(`/api/runs/${encodeURIComponent(runId)}`);
  const run = data.run;
  state.runId = run.id;
  state.status = run.status;
  state.terminal = ['done', 'cancelled', 'failed'].includes(run.status);
  state.answerText = run.answer || '';
  state.citations = (run.citations || []).slice();
  state.lastSeq = run.last_event_id || 0;
  state.expectedSeq = state.lastSeq + 1;
  state.runStartedAt = run.started_at || run.created_at;
  state.contextBudget = run.context_budget && run.context_budget.budget_tokens ? run.context_budget : null;
  state.trace = null;
  state.nodes = new Map(
    (run.nodes || []).map((node) => [
      node.id,
      {
        id: node.id,
        label: node.label,
        kind: node.kind,
        parent: node.parent,
        status: node.status,
        startedAt: node.started_at,
        durationMs: node.duration_ms,
        tokensIn: node.tokens_in || 0,
        tokensOut: node.tokens_out || 0,
        detail: node.detail || '',
        tools: [],
      },
    ])
  );
  const terminalRun = ['done', 'cancelled', 'failed'].includes(run.status);
  setHint(terminalRun ? `loaded ${run.id} · ${run.status}` : `resuming ${run.id}…`);
  renderAnswer();
  renderTimeline();
  renderCitations();
  renderTrace();
  await refreshTrace();
  const connKind = state.terminal
    ? run.status === 'done'
      ? 'done'
      : run.status
    : 'running';
  setConn(connKind, state.terminal ? run.status : 'resuming…');
  updateComposer();
  if (!state.terminal) {
    state.status = 'running';
    resetEventLog('resuming live stream from Last-Event-ID…');
    connectStream();
  } else {
    resetEventLog('loaded from snapshot · event log fills during a live run');
  }
}

async function selectSession(sessionId) {
  if (isBusy()) {
    setHint('finish or stop the current run before switching sessions', true);
    return;
  }
  closeStream();
  state.sessionId = sessionId;
  state.runId = null;
  state.lastSeq = 0;
  state.nodes = new Map();
  state.citations = [];
  state.answerText = '';
  state.trace = null;
  state.contextBudget = null;
  state.status = 'idle';
  state.terminal = true;
  state.runStartedAt = null;
  renderSessions();
  try {
    const data = await api(`/api/sessions/${encodeURIComponent(sessionId)}`);
    const session = data.session;
    state.sessionTitle = session.title;
    state.sessionId = session.id;
    renderSessionHeader();
    const runs = session.runs || [];
    if (runs.length) {
      await loadRunSnapshot(runs[runs.length - 1].id);
    } else {
      setHint('SSE stream · Last-Event-ID resume');
      renderAnswer();
      renderTimeline();
      renderCitations();
      renderTrace();
      setConn('idle', 'idle');
    }
  } catch (error) {
    setHint(`failed to load session: ${error.message}`, true);
  }
  renderSessions();
}

async function createSession() {
  try {
    const data = await api('/api/sessions', { method: 'POST', body: JSON.stringify({}) });
    const session = data.session;
    state.sessions.unshift(session);
    renderSessions();
    await selectSession(session.id);
  } catch (error) {
    setHint(`create session failed: ${error.message}`, true);
  }
}

/* ---------- actions ---------- */

function updateComposer() {
  const busy = isBusy();
  $('#send-btn').disabled = busy;
  $('#cancel-btn').hidden = !busy;
  $('#drop-btn').disabled = !(state.status === 'running' && state.es);
  $('#answer-stream-state').textContent =
    state.status === 'running'
      ? `streaming · seq ${state.lastSeq}`
      : state.status === 'connecting'
        ? 'connecting…'
        : state.status === 'cancelling'
          ? 'cancelling…'
          : state.runId
            ? `${state.status} · seq ${state.lastSeq}`
            : 'no active run';
}

async function submitQuery(event) {
  if (event) event.preventDefault();
  if (isBusy()) return;
  const input = $('#query-input');
  const query = input.value.trim();
  if (!query) {
    setHint('type a question first', true);
    return;
  }
  if (!state.sessionId) {
    await createSession();
    if (!state.sessionId) return;
  }
  try {
    setHint('creating run…');
    const data = await api('/api/runs', {
      method: 'POST',
      body: JSON.stringify({ session_id: state.sessionId, query }),
    });
    input.value = '';
    startRun(data.run);
  } catch (error) {
    setHint(`run failed to start: ${error.message}`, true);
  }
}

function startRun(run) {
  closeStream();
  state.runId = run.id;
  state.runStartedAt = nowSeconds();
  state.status = 'connecting';
  state.terminal = false;
  state.lastSeq = 0;
  state.expectedSeq = 1;
  state.seqGaps = 0;
  state.reconnects = 0;
  state.reconnectAttempts = 0;
  state.nodes = new Map();
  state.citations = [];
  state.answerText = '';
  state.contextBudget = null;
  state.trace = null;
  state.evicted = 0;
  resetEventLog('waiting for first event…');
  setHint(`run ${run.id} · connecting SSE…`);
  setConn('running', 'connecting…');
  renderTimeline();
  renderAnswer();
  renderCitations();
  renderTrace();
  updateComposer();
  connectStream();
}

async function cancelRun() {
  if (!state.runId || state.terminal) return;
  state.status = 'cancelling';
  updateComposer();
  setHint('cancel requested — propagating to asyncio task and LLM stream…');
  try {
    const data = await api(`/api/runs/${encodeURIComponent(state.runId)}/cancel`, { method: 'POST' });
    setHint(`cancel ack · status=${data.run.status} · latency=${data.run.metrics.cancel_latency_ms ?? data.run.metrics.cancel_observed_ms ?? '—'}ms`);
  } catch (error) {
    setHint(`cancel failed: ${error.message}`, true);
  }
}

function simulateDrop() {
  if (state.status !== 'running' || !state.es) return;
  state.es.close();
  state.es = null;
  setConn('resuming', 'simulated drop');
  setHint(`connection dropped after seq ${state.lastSeq} — replaying on reconnect`);
  state.reconnectTimer = setTimeout(() => {
    state.reconnectTimer = null;
    if (state.terminal) return;
    state.reconnects += 1;
    renderStreamInfo();
    connectStream();
  }, 900);
}

/* ---------- ticker ---------- */

function tick() {
  if (!isBusy()) return;
  for (const node of state.nodes.values()) {
    if (node.status !== 'running') continue;
    const element = document.querySelector(`[data-node-dur="${CSS.escape(node.id)}"]`);
    if (element) element.textContent = fmtMs((nowSeconds() - node.startedAt) * 1000);
  }
  if (state.status === 'running' && state.runStartedAt) {
    const meta = $('#run-meta');
    if (meta) meta.textContent = `${state.runId} · seq ${state.lastSeq} · ${fmtSeconds(nowSeconds() - state.runStartedAt)}`;
    if (state.terminal === false) {
      $('#tile-duration').textContent = fmtSeconds(nowSeconds() - state.runStartedAt);
    }
  }
  updateComposer();
}

/* ---------- events ---------- */

function bindEvents() {
  $('#new-session-btn').addEventListener('click', () => void createSession());
  $('#query-form').addEventListener('submit', (event) => void submitQuery(event));
  $('#cancel-btn').addEventListener('click', () => void cancelRun());
  $('#drop-btn').addEventListener('click', simulateDrop);

  $('#query-input').addEventListener('keydown', (event) => {
    if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) {
      event.preventDefault();
      void submitQuery(event);
    }
  });

  $('#session-list').addEventListener('click', (event) => {
    const item = event.target.closest('[data-session-id]');
    if (!item) return;
    void selectSession(item.dataset.sessionId);
  });

  $('#answer').addEventListener('click', (event) => {
    const cite = event.target.closest('sup.cite');
    if (!cite) return;
    const card = document.getElementById(`cite-${cite.dataset.index}`);
    if (!card) return;
    card.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
    card.classList.add('flash');
    setTimeout(() => card.classList.remove('flash'), 900);
  });
}

async function init() {
  bindEvents();
  renderAnswer();
  renderTrace();
  await loadHealth();
  await refreshSessions();
  if (state.sessions.length) {
    await selectSession(state.sessions[0].id);
  } else {
    await createSession();
  }
  setInterval(tick, 250);
}

document.addEventListener('DOMContentLoaded', () => {
  void init();
});
