/* PumpQuant research terminal.
 *
 * The vendored chart library is presentation-only; every visible value still
 * comes from the live API. The UI polls as well as listening to SSE, so a quiet
 * event stream can never make the screen look frozen.
 */

// Format numbers in the page language so figures and copy read consistently.
const LOCALE = document.documentElement.lang || undefined;
import { chartTime, initShell } from "./shell.js";

// The shared shell owns the sidebar, theme, section highlight and motion.
// On a theme change the chart re-reads its colours from the new tokens.
initShell({ onThemeChange: () => state.tokenChart?.applyOptions({
  layout: { textColor: css("--text-3", "#6e6e73") },
  grid: { horzLines: { color: css("--line", "rgba(255,255,255,0.08)") } },
}) });

const API = { snapshot: "/api/snapshot", events: "/api/events", health: "/api/health", fx: "/api/fx", tokenSupply: "/api/token-supply", candidateTrades: "/api/candidate-trades", tradeChart: "/api/trade-chart", atlasSummary: "/api/atlas-summary", atlasTokens: "/api/atlas-tokens", atlasPatterns: "/api/atlas-patterns", atlasIdeas: "/api/atlas-ideas", atlasVelocity: "/api/atlas-velocity" };
const state = {
  snapshot: null,
  health: null,
  rows: [],
  selectedKey: null,
  query: "",
  family: "all",
  fold: "all",
  status: "all",
  sort: "returnPct",
  direction: "desc",
  chartMode: "equity",
  tradeCandidate: null,
  trades: [],
  tradeTotal: 0,
  tradeOffset: 0,
  tradePageSize: 500,
  tradeLoading: false,
  tradeError: null,
  selectedTradeId: null,
  chartPayload: null,
  chartLoading: false,
  tokenChart: null,
  tokenSeries: null,
  tokenVolumeSeries: null,
  tokenMarkers: null,
  tokenChartViewKey: null,
  tokenSupply: null,
  tokenSupplySlot: null,
  supplyLoading: false,
  timeframeSeconds: 1,
  currencyMode: "SOL",
  solUsd: null,
  fxFetchedAt: null,
  fxStale: false,
  reportView: "performance",
  logLevel: "all",
  connection: "connecting",
  receivedAt: null,
  sourceUpdatedAt: null,
  lastCompletedAt: null,
  planSha256: null,
  progressSequence: null,
  events: [],
  eventSource: null,
  polling: false,
  atlasSummary: null,
  atlasTokens: [],
  atlasTotal: 0,
  atlasLoading: false,
  signalPatterns: [],
  signalsLoading: false,
  signalRequest: 0,
  opportunityIdeas: [],
  opportunityIdeaTotal: 0,
  ideasLoading: false,
  ideaRequest: 0,
  velocityRows: [],
  velocityLoading: false,
  velocityRequest: 0,
};

const $ = (id) => document.getElementById(id);
const nodes = {};
[
  "connection-badge", "connection-label", "sidebar-led", "sidebar-connection-label", "update-age", "refresh-button", "theme-button",
  "workspace-title", "workspace-subtitle", "run-status-dot", "run-status-label", "run-updated", "sidebar-run-name", "sidebar-run-stage", "sidebar-run-percent", "sidebar-progress",
  "currency-sol", "currency-usd", "fx-status",
  "hero-pnl", "hero-pnl-detail", "hero-return", "hero-return-detail", "hero-winrate", "hero-winrate-detail", "hero-pf", "hero-pf-detail", "hero-dd", "hero-dd-detail", "hero-sharpe", "hero-sharpe-detail", "hero-trades", "hero-trades-detail", "hero-capacity", "hero-capacity-detail",
  "hero-fees", "hero-volume", "hero-gross-profit", "hero-gross-loss", "hero-slippage", "hero-expectancy", "hero-return-dd", "hero-probability",
  "token-chart", "chart-title", "chart-subtitle", "chart-empty", "equity-summary", "pump-link", "chart-trade-detail", "chart-live-note", "custom-timeframe-toggle", "custom-timeframe", "custom-timeframe-value", "custom-timeframe-unit", "custom-timeframe-apply", "custom-timeframe-error", "chart-zoom-out", "chart-zoom-in", "chart-left", "chart-right", "chart-reset", "run-chip", "run-stage", "run-percent", "run-progress", "run-message", "run-workers", "run-elapsed", "run-eta", "run-work-age", "run-memory", "run-rows", "holdout-lock-detail", "brief-status", "brief-objective", "brief-grid", "brief-translation", "brief-sources", "selected-name", "selected-status", "selected-description", "selected-tags", "selected-source", "selected-mechanism",
  "research-search", "family-filter", "fold-filter", "status-filter", "clear-filters", "research-count", "research-body", "research-footnote",
  "wallet-chart", "wallet-empty", "wallet-seed-label", "wallet-summary", "wallet-equity", "wallet-cash", "wallet-positions", "wallet-contention", "capacity-currency-label", "capacity-summary", "capacity-ladder", "execution-state",
  "validation-state", "gates-summary", "gate-grid", "forward-badge", "forward-detail", "forward-list", "holdout-badge", "holdout-detail", "holdout-list", "log-level", "copy-logs", "log-stream", "footer-clock",
  "performance-basis", "profit-stats", "risk-stats", "execution-stats", "trade-prev", "trade-next", "trade-ledger-count", "trade-ledger-body", "parameter-grid", "rule-title", "rule-detail", "rule-source",
  "atlas-count", "atlas-launches", "atlas-active", "atlas-active-rate", "atlas-completed", "atlas-completion-rate", "atlas-2x", "atlas-2x-rate", "atlas-5x", "atlas-5x-rate", "atlas-10x", "atlas-10x-rate", "atlas-20x", "atlas-20x-rate", "atlas-minimum", "atlas-sort", "atlas-refresh", "atlas-body", "atlas-footnote", "signal-count", "signal-age", "signal-target", "signal-refresh", "signal-body", "signal-footnote", "idea-count", "idea-age", "idea-target", "idea-kind", "idea-refresh", "idea-body", "idea-footnote", "velocity-count", "velocity-threshold", "velocity-refresh", "velocity-body", "velocity-footnote",
].forEach((id) => { nodes[id.replaceAll("-", "_")] = $(id); });

const pathValue = (source, path) => path.split(".").reduce((value, key) => value && typeof value === "object" ? value[key] : undefined, source);
const first = (source, paths, fallback = null) => {
  for (const path of paths) { const value = pathValue(source, path); if (value !== undefined && value !== null && value !== "") return value; }
  return fallback;
};
const objectAt = (source, paths) => { const value = first(source, paths, {}); return value && typeof value === "object" && !Array.isArray(value) ? value : {}; };
const list = (value) => Array.isArray(value) ? value : value && typeof value === "object" ? Object.entries(value).map(([key, item]) => ({ key, ...(item && typeof item === "object" ? item : { value: item }) })) : [];
const num = (value) => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() && Number.isFinite(Number(value))) return Number(value);
  return null;
};
const text = (value, fallback = "—") => value === null || value === undefined || value === "" || typeof value === "object" ? fallback : String(value);
const pctNumber = (value) => { const n = num(value); if (n === null) return null; return Math.abs(n) <= 1.000001 ? n * 100 : n; };
const pct = (value, digits = 2) => { const n = pctNumber(value); return n === null ? "—" : `${new Intl.NumberFormat(LOCALE, { maximumFractionDigits: digits }).format(n)}%`; };
const number = (value, digits = 2) => { const n = num(value); return n === null ? "—" : new Intl.NumberFormat(LOCALE, { maximumFractionDigits: digits }).format(n); };
function convertedAmount(value, sourceCurrency) {
  const n = num(value); if (n === null) return null;
  const source = String(sourceCurrency || "USD").toUpperCase(); const target = state.currencyMode;
  if (source === target) return { value: n, currency: target };
  const rate = num(state.solUsd);
  if (rate === null || rate <= 0) return { value: n, currency: source };
  if (source === "SOL" && target === "USD") return { value: n * rate, currency: target };
  if (source === "USD" && target === "SOL") return { value: n / rate, currency: target };
  return { value: n, currency: source };
}
function formatCurrency(value, currency, digits = 2, compactValue = false) {
  const n = num(value); if (n === null) return "—";
  if (compactValue) return `${new Intl.NumberFormat(LOCALE, { notation: "compact", maximumFractionDigits: 2 }).format(n)} ${currency}`;
  if (currency === "USD") return new Intl.NumberFormat(LOCALE, { style: "currency", currency: "USD", maximumFractionDigits: digits }).format(n);
  return `${new Intl.NumberFormat(LOCALE, { maximumFractionDigits: digits }).format(n)} ${currency}`;
}
const money = (value, currency = "USD", digits = 2) => { const converted = convertedAmount(value, currency); return converted ? formatCurrency(converted.value, converted.currency, digits) : "—"; };
const sol = (value, digits = 6) => money(value, "SOL", digits);
const unitPrice = (value) => { const converted = convertedAmount(value, "SOL"); return converted ? `${new Intl.NumberFormat(LOCALE, { maximumFractionDigits: 12 }).format(converted.value)} ${converted.currency}/token` : "—"; };
const compact = (value) => { const n = num(value); return n === null ? "—" : new Intl.NumberFormat(LOCALE, { notation: "compact", maximumFractionDigits: 2 }).format(n); };
const finiteOr = (value, fallback = null) => num(value) === null ? fallback : num(value);
const has = (value) => value !== null && value !== undefined && value !== "";
function duration(value) {
  const seconds = num(value);
  if (seconds === null) return null;
  if (seconds <= 0) return "complete";
  if (seconds < 60) return `${Math.max(1, Math.ceil(seconds))}s`;
  const minutes = Math.ceil(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  const remaining = minutes % 60;
  return remaining ? `${hours}h ${remaining}m` : `${hours}h`;
}

function formatTime(value) {
  if (!has(value)) return "—";
  const date = typeof value === "number" && value < 1e12 ? new Date(value * 1000) : new Date(value);
  return Number.isNaN(date.getTime()) ? text(value) : date.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function formatDate(value) {
  if (!has(value)) return "—";
  const date = typeof value === "number" && value < 1e12 ? new Date(value * 1000) : new Date(value);
  return Number.isNaN(date.getTime()) ? text(value) : date.toLocaleString(LOCALE, { dateStyle: "medium", timeStyle: "short" });
}

function unwrap(value) {
  if (!value || typeof value !== "object") return null;
  let current = value;
  for (let i = 0; i < 3; i += 1) {
    const nested = first(current, ["snapshot", "state", "payload"], null);
    if (!nested || typeof nested !== "object" || Array.isArray(nested) || nested === current) break;
    current = nested;
  }
  return current;
}

function metricObject(raw) { return objectAt(raw, ["metrics", "performance", "stats", "diagnostics"]); }

function points(value, fallback = []) {
  return list(value).map((item, index) => {
    if (typeof item === "number") return { x: index, value: item, index };
    const x = first(item, ["x", "index", "timestamp", "time", "slot", "event_index"], index);
    const y = first(item, ["value", "y", "equity", "pnl", "return", "drawdown", "price", "amount"], null);
    const n = num(y);
    return n === null ? null : { x: has(x) ? x : index, value: n, index };
  }).filter(Boolean).length ? list(value).map((item, index) => {
    if (typeof item === "number") return { x: index, value: item, index };
    const x = first(item, ["x", "index", "timestamp", "time", "slot", "event_index"], index);
    const y = first(item, ["value", "y", "equity", "pnl", "return", "drawdown", "price", "amount"], null);
    const n = num(y);
    return n === null ? null : { x: has(x) ? x : index, value: n, index };
  }).filter(Boolean) : fallback;
}

function normalizeCandidate(raw, index) {
  const metrics = metricObject(raw);
  const candidate = objectAt(raw, ["candidate", "research", "idea"]);
  const merged = { ...candidate, ...raw, ...metrics };
  const name = text(first(merged, ["name", "label", "title", "signal", "id"], null), `Research ${index + 1}`);
  const family = text(first(merged, ["family", "family_name", "strategy_family", "group"], null), "Unclassified");
  const fold = text(first(merged, ["fold", "window", "holding_events", "horizon", "test_window"], null), "All windows");
  const status = text(first(merged, ["status", "state", "result"], null), "reported");
  const netPnl = finiteOr(first(merged, ["net_pnl", "net_profit", "pnl", "profit", "total_net_profit", "total_quote_pnl", "total_pnl", "net_quote_pnl", "after_cost_pnl"], null));
  const returnPct = pctNumber(first(merged, ["return_pct", "return_percent", "return", "roi", "net_return"], null));
  const winRate = pctNumber(first(merged, ["win_rate", "winning_percentage", "percent_profitable", "win_percent", "hit_rate"], null));
  const profitFactor = finiteOr(first(merged, ["profit_factor", "pf", "profitFactor"], null));
  const sharpe = finiteOr(first(merged, ["sharpe", "sharpe_ratio", "sharpe_per_trade"], null));
  const sortino = finiteOr(first(merged, ["sortino", "sortino_ratio", "sortino_per_trade"], null));
  const maxDrawdown = finiteOr(first(merged, ["max_drawdown", "max_drawdown_quote", "sequence_max_drawdown", "maximum_drawdown", "drawdown"], null));
  const maxDrawdownPct = pctNumber(first(merged, ["max_drawdown_pct", "maximum_drawdown_pct", "drawdown_pct", "relative_drawdown"], null));
  const trades = finiteOr(first(merged, ["trades", "trade_count", "test_trade_count", "total_trades", "number_of_trades", "outcomes"], null));
  const expectancy = finiteOr(first(merged, ["expectancy_quote", "expectancy", "mean_event_pnl_quote", "average_trade", "avg_trade", "r_expectancy"], null));
  const averageTrade = finiteOr(first(merged, ["average_trade", "avg_trade", "mean_event_pnl_quote", "expectancy_quote", "expectancy"], null));
  const grossProfit = finiteOr(first(merged, ["gross_profit_quote", "gross_profit", "total_gross_profit"], null));
  const grossLoss = finiteOr(first(merged, ["gross_loss_quote", "gross_loss", "total_gross_loss"], null));
  const fees = finiteOr(first(merged, ["total_fees", "fees", "commission", "total_commission", "costs.fees", "costs.commission"], null));
  const volume = finiteOr(first(merged, ["volume", "traded_volume", "quote_volume", "turnover_quote", "sales_volume"], null));
  const turnover = finiteOr(first(merged, ["turnover", "turnover_quote", "total_turnover"], null));
  const slippage = finiteOr(first(merged, ["slippage", "total_slippage", "slippage_quote", "costs.slippage"], null));
  const explicitReturnDrawdown = finiteOr(first(merged, ["return_drawdown", "return_dd_ratio", "return_to_drawdown", "return_over_drawdown"], null));
  const returnDrawdown = explicitReturnDrawdown !== null ? explicitReturnDrawdown : returnPct !== null && maxDrawdownPct !== null && maxDrawdownPct !== 0 ? returnPct / Math.abs(maxDrawdownPct) : null;
  const probability = pctNumber(first(merged, ["deflated_sharpe_probability", "probabilistic_sharpe", "psr", "probability_of_sharpe", "probability"], null));
  const rSquared = finiteOr(first(merged, ["r_squared", "r2", "equity_r_squared"], null));
  const ulcerIndex = finiteOr(first(merged, ["ulcer_index", "ulcer"], null));
  const wins = finiteOr(first(merged, ["wins", "winning_trades", "number_winning_trades"], null));
  const losses = finiteOr(first(merged, ["losses", "losing_trades", "number_losing_trades"], null));
  const breakevens = finiteOr(first(merged, ["breakevens", "breakeven_trades", "even_trades"], null));
  const avgWin = finiteOr(first(merged, ["average_win_quote", "average_win", "avg_win", "average_winning_trade"], null));
  const avgLoss = finiteOr(first(merged, ["average_loss_quote", "average_loss", "avg_loss", "average_losing_trade"], null));
  const payoffRatio = finiteOr(first(merged, ["payoff_ratio", "avg_win_loss_ratio", "ratio_avg_win_avg_loss"], null));
  const largestWin = finiteOr(first(merged, ["largest_win", "largest_winning_trade", "max_win"], null));
  const largestLoss = finiteOr(first(merged, ["largest_loss", "largest_losing_trade", "max_loss"], null));
  const maxConsecutiveWins = finiteOr(first(merged, ["max_consecutive_wins", "maximum_consecutive_wins", "win_streak"], null));
  const maxConsecutiveLosses = finiteOr(first(merged, ["max_consecutive_losses", "maximum_consecutive_losses", "loss_streak"], null));
  const exposure = pctNumber(first(merged, ["exposure", "exposure_pct", "time_in_market"], null));
  const fillRate = pctNumber(first(merged, ["fill_rate", "filled_order_rate", "execution.fill_rate"], null));
  const rejectedFills = finiteOr(first(merged, ["rejected_fills", "rejected_orders", "execution.rejected_fills"], null));
  const latencyCost = finiteOr(first(merged, ["latency_cost", "latency_cost_quote", "costs.latency"], null));
  const mae = finiteOr(first(merged, ["mae", "maximum_adverse_excursion", "average_mae"], null));
  const mfe = finiteOr(first(merged, ["mfe", "maximum_favorable_excursion", "average_mfe"], null));
  const volatility = finiteOr(first(merged, ["volatility", "pnl_std_quote", "standard_deviation"], null));
  const downsideDeviation = finiteOr(first(merged, ["downside_deviation", "downside_deviation_quote"], null));
  const source = text(first(merged, ["source", "provenance", "paper", "reference", "citation"], null), "Provenance not reported");
  const mechanism = text(first(merged, ["mechanism", "market_translation", "rationale", "description", "rule"], null), "Mechanism not reported.");
  const equity = points(first(merged, ["equity", "equity_curve", "equity_series", "series.equity", "curve"], []));
  const benchmark = points(first(merged, ["benchmark", "benchmark_curve", "benchmark_series", "series.benchmark"], []));
  const drawdown = points(first(merged, ["drawdown_curve", "drawdown_series", "series.drawdown"], []));
  const price = points(first(merged, ["price", "price_series", "series.price", "bars"], []));
  const tradeEvents = list(first(merged, ["trade_events", "trades_detail", "fills", "orders"], []));
  const parameters = { ...objectAt(candidate, ["parameters", "params", "axis", "config"]), ...objectAt(raw, ["parameters", "params", "axis", "config"]) };
  const key = text(first(merged, ["sha256", "candidate_sha256", "id", "key"], null), `${family}:${name}:${fold}:${index}`);
  const currency = text(first(merged, ["currency", "quote_currency"], "USD"), "USD");
  return { raw, key, name, family, fold, status, netPnl, returnPct, winRate, profitFactor, sharpe, sortino, maxDrawdown, maxDrawdownPct, trades, expectancy, averageTrade, grossProfit, grossLoss, fees, volume, turnover, slippage, returnDrawdown, probability, rSquared, ulcerIndex, wins, losses, breakevens, avgWin, avgLoss, payoffRatio, largestWin, largestLoss, maxConsecutiveWins, maxConsecutiveLosses, exposure, fillRate, rejectedFills, latencyCost, mae, mfe, volatility, downsideDeviation, source, mechanism, equity, benchmark, drawdown, price, tradeEvents, parameters, currency };
}

function candidateRows(snapshot) {
  const live = list(first(snapshot, ["strategy.candidates", "candidates", "research.candidates", "strategy.results"], []));
  const registered = list(first(snapshot, ["progress.registered_candidates", "progress.research_announcement.registered_candidates"], []));
  if (!registered.length) return live.map((item, index) => normalizeCandidate(item, index));
  const candidateId = (item) => text(first(item, ["candidate.sha256", "candidate.candidate_sha256", "sha256", "candidate_sha256"], null), "");
  const liveIds = new Set(live.map(candidateId).filter(Boolean));
  const pending = registered
    .filter((item) => !liveIds.has(candidateId(item)))
    .map((item) => ({ ...item, status: "registered_pending" }));
  return [...live, ...pending].map((item, index) => normalizeCandidate(item, index));
}

function progressFrom(snapshot) {
  const progress = objectAt(snapshot, ["progress", "data.progress"]);
  const pipeline = objectAt(snapshot, ["pipeline"]);
  const reported = objectAt(pipeline, ["reported"]);
  const runtime = objectAt(progress, ["runtime"]);
  const nestedProgress = objectAt(progress, ["progress"]);
  const elapsed = finiteOr(first(runtime, ["elapsed_seconds"], null));
  const processedStrategies = finiteOr(first(nestedProgress, ["processed_strategies"], null));
  const totalStrategies = finiteOr(first(nestedProgress, ["total_strategies"], null));
  const reportedEta = finiteOr(first(runtime, ["eta_seconds"], null));
  return {
    ...progress,
    stage: text(first(progress, ["pipeline.current_stage", "current_stage", "stage"], first(pipeline, ["current_stage", "stage"], null)), "Waiting"),
    message: text(first(progress, ["message", "pipeline.detail"], first(pipeline, ["reported.detail"], null)), "No live progress checkpoint has been published."),
    runStatus: text(first(progress, ["run_status", "status"], null), ""),
    percent: finiteOr(first(nestedProgress, ["percent"], first(progress, ["percent"], 0)), 0),
    completed: finiteOr(first(nestedProgress, ["completed_passes"], null)),
    total: finiteOr(first(nestedProgress, ["total_passes"], null)),
    processedStrategies,
    totalStrategies,
    enginePercent: finiteOr(first(nestedProgress, ["engine_query_percent"], null)),
    elapsed,
    etaSeconds: reportedEta,
    etaMode: text(first(runtime, ["eta_source", "eta_mode"], null), reportedEta !== null ? "reported" : "unavailable"),
    lastCompletedAt: first(progress, ["liveness.last_completed_at_epoch"], null),
    threads: finiteOr(first(runtime, ["effective_duckdb_threads", "threads"], first(snapshot, ["resources.workers"], null))),
    memory: text(first(runtime, ["memory_limit"], null), "—"),
    rows: finiteOr(first(progress, ["input_rows", "row_count", "rows"], first(snapshot, ["data.attested_input_summary.row_count"], null))),
    reported,
  };
}

// Typographic minus for negative figures ("−0.13", not "-0.13"); dates are untouched.
const typesetMinus = (value) => value.replace(/(^|[\s(])-(?=\d)/g, "$1\u2212");
function setText(node, value, fallback = "—") { if (node) node.textContent = has(value) ? typesetMinus(String(value)) : fallback; }
function setClass(node, name, value) { if (node) node.className = `${name} ${value || ""}`.trim(); }
function statusKind(value) { const s = String(value || "").toLowerCase(); if (/running|resuming|starting|in-progress|pass|complete|ready|sealed|live|ok|positive/.test(s)) return "ready"; if (/error|fail|blocked|negative/.test(s)) return "error"; return "warn"; }
// Windows are counted from 1 for people ("Test period 1" runs in window 1).
const displayWindow = (fold) => (typeof fold === "number" || /^\d+$/.test(String(fold)) ? String(Number(fold) + 1) : fold);
function normalizeStatus(value) { return text(value, "pending").replace(/[_-]+/g, " ").toLowerCase(); }
// Plain words for the engine's exit reasons, e.g. "stop_loss" → "stop-loss".
const EXIT_WORDS = { stop_loss: "stop-loss", take_profit: "take-profit", trailing_stop: "trailing stop", max_holding: "time limit", exit_momentum: "momentum faded" };
const exitWords = (reason) => EXIT_WORDS[reason] || text(reason, "").replace(/_/g, " ");

function renderRun(snapshot) {
  const progress = progressFrom(snapshot);
  const status = text(progress.runStatus, first(snapshot, ["status", "pipeline.status"], null) || "pending");
  const kind = statusKind(status);
  const isActive = /running|resuming|starting|in-progress/.test(String(progress.runStatus || status).toLowerCase());
  const isIndeterminate = isActive && progress.percent <= 0;
  const percentLabel = isIndeterminate ? `LIVE · ${number(progress.percent, 1)}%` : `${number(progress.percent, 1)}%`;
  const etaModeLabel = progress.etaMode === "historical_bootstrap" ? "bootstrap" : progress.etaMode === "measured_hybrid" ? "measured" : null;
  const etaLabel = progress.etaSeconds === 0 ? "Complete" : progress.etaSeconds === null ? "—" : `~${duration(progress.etaSeconds)}${etaModeLabel ? ` · ${etaModeLabel}` : ""}`;
  setText(nodes.workspace_title, first(snapshot, ["experiment_id", "progress.experiment_id"], null), "Research run");
  setText(nodes.workspace_subtitle, progress.message, "Waiting for the first sealed or live research snapshot.");
  setText(nodes.run_status_label, normalizeStatus(status));
  setClass(nodes.run_status_dot, "status-dot", kind);
  setText(nodes.run_updated, `updated ${formatTime(first(snapshot, ["progress.heartbeat_at_epoch", "progress.updated_at_epoch", "generated_at"], null))}`);
  setText(nodes.sidebar_run_name, first(snapshot, ["progress.experiment_id", "experiment_id"], null), "Waiting for research");
  const operation = progress.enginePercent === null ? progress.stage : `${progress.stage} · SQL ${number(progress.enginePercent, 1)}%`;
  const strategyCount = progress.processedStrategies === null || progress.totalStrategies === null ? null : `${number(progress.processedStrategies, 0)}/${number(progress.totalStrategies, 0)} strategies`;
  setText(nodes.sidebar_run_stage, operation);
  setText(nodes.sidebar_run_percent, percentLabel);
  if (nodes.sidebar_progress) {
    nodes.sidebar_progress.style.width = `${isIndeterminate ? 34 : Math.max(0, Math.min(100, progress.percent))}%`;
    nodes.sidebar_progress.classList.toggle("indeterminate", isIndeterminate);
  }
  setText(nodes.run_stage, operation);
  setText(nodes.run_percent, percentLabel);
  if (nodes.run_progress) {
    nodes.run_progress.style.width = `${isIndeterminate ? 34 : Math.max(0, Math.min(100, progress.percent))}%`;
    nodes.run_progress.classList.toggle("indeterminate", isIndeterminate);
  }
  const liveMessage = isIndeterminate ? " · live heartbeat" : "";
  setText(nodes.run_message, `${strategyCount ? `${progress.message} · ${strategyCount}` : progress.message}${liveMessage}`);
  setText(nodes.run_workers, progress.threads === null ? first(snapshot, ["resources.workers"], null) : `${number(progress.threads, 0)} threads`);
  setText(nodes.run_elapsed, progress.elapsed === null ? null : `${number(progress.elapsed, 0)}s`);
  setText(nodes.run_eta, etaLabel);
  setText(nodes.run_memory, progress.memory);
  setText(nodes.run_rows, progress.rows === null ? null : compact(progress.rows));
  setText(nodes.holdout_lock_detail, first(snapshot, ["holdout.accessed", "validation.holdout.accessed"], false) ? "Opened for the final test" : "Untouched until a strategy passes every check");
  setText(nodes.run_chip, normalizeStatus(status));
  setClass(nodes.run_chip, "state-chip", kind);
}

function renderResearchBrief(snapshot) {
  const announcement = objectAt(snapshot, ["progress.research_announcement", "research_announcement"]);
  const grid = objectAt(announcement, ["parameter_grid"]);
  const sources = list(first(announcement, ["sources"], []));
  const profiles = list(first(grid, ["exit_profiles"], []));
  const tails = objectAt(grid, ["train_tail_quantiles"]);
  const features = list(first(grid, ["signal_features"], []));
  const status = first(snapshot, ["progress.run_status"], first(announcement, ["status"], "pending"));
  setText(nodes.brief_status, normalizeStatus(status));
  setClass(nodes.brief_status, "state-chip", statusKind(status));
  setText(nodes.brief_objective, first(announcement, ["objective"], null), "The next development experiment has not been announced.");
  const continuation = list(first(tails, ["continuation"], [])).join(" / ");
  const reversal = list(first(tails, ["reversal"], [])).join(" / ");
  const gridText = features.length ? `${number(first(announcement, ["candidate_count"], first(grid, ["candidate_count"], null)), 0)} candidates · ${number(first(announcement, ["cumulative_trial_denominator"], null), 0)} cumulative trials · ${features.length} mechanisms · ${profiles.length} exits · tails ${continuation} / ${reversal}` : null;
  setText(nodes.brief_grid, gridText, "—");
  setText(nodes.brief_translation, first(announcement, ["causal_translation"], null), "—");
  nodes.brief_sources.replaceChildren();
  if (!sources.length) {
    nodes.brief_sources.innerHTML = `<div class="empty-state"><span class="empty-glyph">⌁</span><strong>Waiting for the preregistration</strong><span>Every source and mechanism will appear before outcomes.</span></div>`;
    return;
  }
  sources.forEach((source) => {
    const card = document.createElement("article"); card.className = "brief-source";
    const theory = document.createElement("strong"); theory.textContent = text(first(source, ["theory", "title"], null), "Research source");
    const link = document.createElement("a"); link.href = text(first(source, ["source", "url"], "#"), "#"); link.target = "_blank"; link.rel = "noreferrer"; link.textContent = text(first(source, ["source", "url"], null), "Source not reported");
    const mechanism = document.createElement("p"); mechanism.textContent = text(first(source, ["mechanism", "rationale"], null), "Mechanism not reported.");
    const translation = document.createElement("small"); translation.textContent = `Pump / PumpSwap: ${text(first(source, ["pump_translation", "translation"], null), "Translation not reported.")}`;
    card.append(theory, link, mechanism, translation); nodes.brief_sources.append(card);
  });
}

function renderHeadline(snapshot, rows) {
  const selected = rows.find((row) => row.key === state.selectedKey) || rows[0] || null;
  const capacity = list(first(snapshot, ["execution.capacity", "capacity"], [])).map((item) => num(first(item, ["net_usd_per_day", "usd_per_day", "daily_net", "capacity_usd_per_day", "value"], null))).filter((value) => value !== null);
  const bestCapacity = capacity.length ? Math.max(...capacity) : null;
  setText(nodes.hero_pnl, !selected || selected.netPnl === null ? null : money(selected.netPnl, selected.currency));
  setText(nodes.hero_pnl_detail, selected ? `${selected.name} · after costs` : "After costs");
  setText(nodes.hero_return, !selected || selected.returnPct === null ? null : pct(selected.returnPct / 100));
  setText(nodes.hero_return_detail, "On the capital put to work");
  setText(nodes.hero_winrate, !selected || selected.winRate === null ? null : pct(selected.winRate / 100));
  setText(nodes.hero_winrate_detail, !selected || selected.trades === null ? "Share of winning trades" : `Across ${number(selected.trades, 0)} outcomes`);
  setText(nodes.hero_pf, !selected || selected.profitFactor === null ? null : number(selected.profitFactor, 2));
  setText(nodes.hero_pf_detail, "Gains divided by losses");
  setText(nodes.hero_dd, !selected || selected.maxDrawdown === null ? null : money(selected.maxDrawdown, selected.currency));
  setText(nodes.hero_dd_detail, "Largest fall from a peak");
  setText(nodes.hero_sharpe, !selected || selected.sharpe === null ? null : number(selected.sharpe, 2));
  setText(nodes.hero_sharpe_detail, "Return for each unit of risk");
  setText(nodes.hero_trades, !selected || selected.trades === null ? null : number(selected.trades, 0));
  setText(nodes.hero_trades_detail, selected ? `${text(selected.family, "").replace(/_/g, " ")} rule` : "Closed positions");
  setText(nodes.hero_capacity, bestCapacity === null ? null : money(bestCapacity));
  setText(nodes.hero_capacity_detail, bestCapacity === null ? "How much it could trade, after costs" : `Best measured, ${state.currencyMode} per day`);
  setText(nodes.hero_fees, selected?.fees === null || !selected ? "—" : money(selected.fees, selected.currency));
  setText(nodes.hero_volume, selected?.volume === null || !selected ? "—" : money(selected.volume, selected.currency));
  setText(nodes.hero_gross_profit, selected?.grossProfit === null || !selected ? "—" : money(selected.grossProfit, selected.currency));
  setText(nodes.hero_gross_loss, selected?.grossLoss === null || !selected ? "—" : money(selected.grossLoss, selected.currency));
  setText(nodes.hero_slippage, selected?.slippage === null || !selected ? "—" : money(selected.slippage, selected.currency));
  setText(nodes.hero_expectancy, selected?.expectancy === null || !selected ? "—" : money(selected.expectancy, selected.currency));
  setText(nodes.hero_return_dd, selected?.returnDrawdown === null || !selected ? "—" : number(selected.returnDrawdown, 2));
  setText(nodes.hero_probability, selected?.probability === null || !selected ? "—" : pct(selected.probability / 100));
}

function filteredRows() {
  const query = state.query.trim().toLowerCase();
  const rows = state.rows.filter((row) => {
    if (state.family !== "all" && row.family !== state.family) return false;
    if (state.fold !== "all" && row.fold !== state.fold) return false;
    if (state.status === "positive" && !(row.returnPct !== null && row.returnPct > 0)) return false;
    if (state.status === "reported" && row.returnPct === null && row.netPnl === null) return false;
    return !query || [row.name, row.family, row.fold, row.source, row.mechanism].join(" ").toLowerCase().includes(query);
  });
  const direction = state.direction === "asc" ? 1 : -1;
  return rows.sort((a, b) => {
    const av = a[state.sort]; const bv = b[state.sort];
    if (typeof av === "number" || typeof bv === "number") return ((av ?? -Infinity) - (bv ?? -Infinity)) * direction;
    return text(av).localeCompare(text(bv)) * direction;
  });
}

function updateFilters() {
  const families = [...new Set(state.rows.map((row) => row.family))].sort();
  const folds = [...new Set(state.rows.map((row) => row.fold))].sort();
  const fill = (node, values, current, label) => { if (!node) return; const old = current; node.replaceChildren(new Option(`All ${label}`, "all"), ...values.map((value) => new Option(value, value))); node.value = values.includes(old) ? old : "all"; };
  fill(nodes.family_filter, families, state.family, "families"); state.family = nodes.family_filter?.value || "all";
  fill(nodes.fold_filter, folds, state.fold, "windows"); state.fold = nodes.fold_filter?.value || "all";
}

function renderResearch() {
  updateFilters();
  const rows = filteredRows();
  setText(nodes.research_count, `${rows.length} of ${state.rows.length} ideas`);
  setText(nodes.research_footnote, rows.length ? `Showing ${rows.length} research results · click to pin` : "No research result matches the current filters.");
  nodes.research_body.replaceChildren();
  if (!rows.length) {
    const tr = document.createElement("tr"); tr.className = "empty-row"; tr.innerHTML = `<td colspan="15"><span class="empty-glyph">⌁</span><strong>${state.rows.length ? "No matching research" : "Research is not published yet"}</strong><span>${state.rows.length ? "Clear a filter to see the full queue." : "Live rows appear here as the sealed ledger is written."}</span></td>`; nodes.research_body.append(tr); return;
  }
  rows.forEach((row) => {
    const tr = document.createElement("tr"); if (row.key === state.selectedKey) tr.className = "selected"; tr.addEventListener("click", () => { selectIdea(row); });
    const cell = (value, className = "") => { const td = document.createElement("td"); td.className = className; td.textContent = value; return td; };
    const family = cell(row.family, "family-cell");
    const idea = document.createElement("td"); idea.className = "idea-cell"; idea.innerHTML = `<span></span><div><strong></strong><small></small></div>`; idea.querySelector("strong").textContent = row.name; idea.querySelector("small").textContent = row.source; 
    const status = cell(normalizeStatus(row.status)); status.className = statusKind(row.status);
    tr.append(family, idea, cell(displayWindow(row.fold)), status, cell(row.returnPct === null ? "—" : pct(row.returnPct / 100), "num"), cell(row.netPnl === null ? "—" : money(row.netPnl, row.currency), `num ${row.netPnl !== null && row.netPnl >= 0 ? "positive" : "negative"}`), cell(row.winRate === null ? "—" : pct(row.winRate / 100), "num"), cell(row.profitFactor === null ? "—" : number(row.profitFactor), "num"), cell(row.sharpe === null ? "—" : number(row.sharpe), "num"), cell(row.sortino === null ? "—" : number(row.sortino), "num"), cell(row.returnDrawdown === null ? "—" : number(row.returnDrawdown), "num"), cell(row.expectancy === null ? "—" : money(row.expectancy, row.currency), "num"), cell(row.probability === null ? "—" : pct(row.probability / 100), "num"), cell(row.maxDrawdown === null ? "—" : money(row.maxDrawdown, row.currency), "num negative"), cell(row.trades === null ? "—" : number(row.trades, 0), "num"));
    nodes.research_body.append(tr);
  });
}

function renderSelected(rows) {
  const row = rows.find((item) => item.key === state.selectedKey) || rows[0] || null;
  if (row && state.selectedKey !== row.key) state.selectedKey = row.key;
  setText(nodes.selected_name, row?.name, "No research selected");
  setText(nodes.selected_status, row ? normalizeStatus(row.status) : "—");
  setClass(nodes.selected_status, "state-chip", row ? statusKind(row.status) : "neutral");
  setText(nodes.selected_description, row ? row.mechanism : "Choose a strategy from the list to see it here.");
  nodes.selected_tags.replaceChildren();
  if (row) [text(row.family, "").replace(/_/g, " "), row.trades === null ? null : `${number(row.trades, 0)} trades`, row.sortino === null ? null : `Sortino ${number(row.sortino)}`].filter(Boolean).forEach((value) => { const span = document.createElement("span"); span.className = "tag"; span.textContent = value; nodes.selected_tags.append(span); });
  setText(nodes.selected_source, row?.source, "—"); setText(nodes.selected_mechanism, row ? row.mechanism : "Source provenance will appear with the sealed result.");
  return row;
}

function canvasSize(canvas) { const rect = canvas.getBoundingClientRect(); const width = Math.max(280, rect.width || canvas.clientWidth || 600); const height = Math.max(170, rect.height || canvas.clientHeight || 260); const dpr = Math.min(2, window.devicePixelRatio || 1); canvas.width = Math.floor(width * dpr); canvas.height = Math.floor(height * dpr); return { ctx: canvas.getContext("2d"), width, height, dpr }; }
function css(name, fallback) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback; }
function chartPoints(series) { return Array.isArray(series) ? series.filter((point) => point && Number.isFinite(point.value)) : []; }

function drawLine(canvas, series, options = {}) {
  const pointsList = chartPoints(series); const secondary = chartPoints(options.secondary); const { ctx, width, height, dpr } = canvasSize(canvas); ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, width, height); if (pointsList.length < 2) return false;
  const pad = { left: 49, right: 16, top: 14, bottom: 28 }; const plotW = width - pad.left - pad.right; const plotH = height - pad.top - pad.bottom;
  const allValues = [...pointsList, ...secondary].map((point) => point.value); let min = Math.min(...allValues); let max = Math.max(...allValues); if (options.zeroBaseline) { min = Math.min(min, 0); max = Math.max(max, 0); } if (min === max) { min -= 1; max += 1; }
  const xAt = (index, count = pointsList.length) => pad.left + (index / Math.max(1, count - 1)) * plotW; const yAt = (value) => pad.top + (1 - ((value - min) / (max - min))) * plotH;
  ctx.font = "9px SFMono-Regular, monospace"; ctx.lineWidth = 1; ctx.strokeStyle = css("--line", "#223344"); ctx.fillStyle = css("--text-3", "#637b91"); ctx.textAlign = "right";
  for (let i = 0; i < 5; i += 1) { const y = pad.top + (plotH * i) / 4; ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(width - pad.right, y); ctx.stroke(); const value = max - ((max - min) * i) / 4; ctx.fillText(number(value, 2), pad.left - 7, y + 3); }
  ctx.textAlign = "left"; ctx.fillText("start", pad.left, height - 8); ctx.textAlign = "right"; ctx.fillText("latest", width - pad.right, height - 8);
  const color = options.color || css("--cyan", "#35d1c5"); ctx.beginPath(); pointsList.forEach((point, index) => { const x = xAt(index); const y = yAt(point.value); if (index === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y); }); ctx.lineWidth = 2; ctx.strokeStyle = color; ctx.stroke();
  if (options.fill) { const gradient = ctx.createLinearGradient(0, pad.top, 0, height); gradient.addColorStop(0, `${color}38`); gradient.addColorStop(1, `${color}00`); ctx.lineTo(xAt(pointsList.length - 1), height - pad.bottom); ctx.lineTo(xAt(0), height - pad.bottom); ctx.closePath(); ctx.fillStyle = gradient; ctx.fill(); }
  if (secondary.length >= 2) { ctx.beginPath(); secondary.forEach((point, index) => { const x = xAt(index, secondary.length); const y = yAt(point.value); if (index === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y); }); ctx.setLineDash([5, 3]); ctx.lineWidth = 1.6; ctx.strokeStyle = options.secondaryColor || css("--amber", "#f5b942"); ctx.stroke(); ctx.setLineDash([]); }
  if (options.zeroBaseline && min < 0 && max > 0) { const y = yAt(0); ctx.strokeStyle = css("--line-bright", "#2e455a"); ctx.setLineDash([4, 4]); ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(width - pad.right, y); ctx.stroke(); ctx.setLineDash([]); }
  (options.markers || []).forEach((marker) => { const index = Math.max(0, Math.min(pointsList.length - 1, num(marker.index) ?? 0)); const x = xAt(index); const y = yAt(pointsList[index].value); ctx.beginPath(); ctx.arc(x, y, 4, 0, Math.PI * 2); ctx.fillStyle = marker.kind === "exit" ? css("--red", "#ff6478") : css("--green", "#43d58a"); ctx.fill(); ctx.strokeStyle = css("--surface", "#101923"); ctx.lineWidth = 2; ctx.stroke(); });
  return true;
}

function secondsFromNs(value) { const n = num(value); return n === null ? null : Math.floor(n / 1_000_000_000); }
function formatNs(value) { const n = num(value); return n === null ? "—" : formatDate(n / 1_000_000); }
function bucketSeconds(value) { const seconds = num(value); return seconds === null ? null : Math.floor(seconds / state.timeframeSeconds) * state.timeframeSeconds; }
function timeframeLabel(seconds = state.timeframeSeconds) { return seconds < 60 ? `${seconds}s` : seconds % 60 === 0 ? `${seconds / 60}m` : `${seconds}s`; }
function marketCapMultiplier() {
  if (window.PUMPQUANT_PREVIEW) return 1;
  const supply = num(state.tokenSupply); if (supply === null || supply <= 0) return null;
  if (state.currencyMode === "SOL") return supply;
  const rate = num(state.solUsd); return rate !== null && rate > 0 ? supply * rate : null;
}
function marketCapText(price) { if (window.PUMPQUANT_PREVIEW) return `${Number(price).toExponential(3)} SOL/token`; const multiplier = marketCapMultiplier(); const value = num(price); return multiplier === null || value === null ? "—" : formatCurrency(value * multiplier, state.currencyMode, 2, true); }
function aggregateBars(rawBars) {
  const buckets = new Map();
  rawBars.forEach((bar) => {
    const time = bucketSeconds(bar.time); if (time === null) return;
    const volume = num(bar.quote_volume);
    const current = buckets.get(time);
    if (!current) {
      buckets.set(time, { time, open: bar.open, high: bar.high, low: bar.low, close: bar.close, quoteVolume: volume || 0, hasVolume: volume !== null });
      return;
    }
    current.high = Math.max(current.high, bar.high); current.low = Math.min(current.low, bar.low); current.close = bar.close;
    if (volume !== null) { current.quoteVolume += volume; current.hasVolume = true; }
  });
  return Array.from(buckets.values());
}

function ensureTokenChart() {
  if (state.tokenChart) return true;
  const library = window.LightweightCharts;
  if (!library || !nodes.token_chart) return false;
  state.tokenChart = library.createChart(nodes.token_chart, {
    autoSize: true,
    layout: { background: { type: "solid", color: "rgba(0, 0, 0, 0)" }, textColor: css("--text-3", "#6e6e73"), fontFamily: "Mona Sans, -apple-system, system-ui, sans-serif", fontSize: 11 },
    grid: { vertLines: { visible: false }, horzLines: { color: css("--line", "rgba(255,255,255,0.08)") } },
    crosshair: { mode: library.CrosshairMode?.Normal ?? 0, vertLine: { color: css("--line-bright", "rgba(255,255,255,0.16)"), style: 2, labelBackgroundColor: css("--surface-3", "#2c2c2e") }, horzLine: { color: css("--line-bright", "rgba(255,255,255,0.16)"), style: 2, labelBackgroundColor: css("--surface-3", "#2c2c2e") } },
    timeScale: { timeVisible: true, secondsVisible: true, borderVisible: false, tickMarkFormatter: chartTime },
    localization: { timeFormatter: chartTime },
    // Simulated prices are tiny raw numbers; show the move in percent, as the overview does.
    rightPriceScale: { borderVisible: false, autoScale: true, mode: window.PUMPQUANT_PREVIEW ? (library.PriceScaleMode?.Percentage ?? 2) : 0 },
  });
  state.tokenSeries = state.tokenChart.addSeries(library.CandlestickSeries, { upColor: css("--green", "#43d58a"), downColor: css("--red", "#ff6478"), borderVisible: false, wickUpColor: css("--green", "#43d58a"), wickDownColor: css("--red", "#ff6478"), priceLineColor: css("--text-3", "#6e6e73"), priceLineStyle: 2, priceFormat: { type: "price", precision: 12, minMove: 0.000000000001 } });
  state.tokenVolumeSeries = state.tokenChart.addSeries(library.HistogramSeries, { priceScaleId: "volume", priceFormat: { type: "volume" }, lastValueVisible: false, priceLineVisible: false });
  state.tokenChart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.84, bottom: 0 } });
  state.tokenMarkers = library.createSeriesMarkers(state.tokenSeries, []);
  state.tokenChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
    if (!range) { delete nodes.token_chart.dataset.visibleLogicalFrom; delete nodes.token_chart.dataset.visibleLogicalTo; return; }
    nodes.token_chart.dataset.visibleLogicalFrom = String(range.from);
    nodes.token_chart.dataset.visibleLogicalTo = String(range.to);
  });
  return true;
}

function renderTokenChart(row) {
  const payload = state.chartPayload;
  const sourceBars = list(payload?.bars).map((bar) => ({ time: num(bar.time), open: num(bar.open), high: num(bar.high), low: num(bar.low), close: num(bar.close), quote_volume: num(bar.quote_volume) })).filter((bar) => bar.time !== null && bar.open !== null && bar.high !== null && bar.low !== null && bar.close !== null);
  const multiplier = marketCapMultiplier();
  const aggregated = aggregateBars(sourceBars);
  const bars = multiplier === null ? [] : aggregated.map((bar) => ({ time: bar.time, open: bar.open * multiplier, high: bar.high * multiplier, low: bar.low * multiplier, close: bar.close * multiplier }));
  const volumeMultiplier = state.currencyMode === "USD" ? num(state.solUsd) : 1;
  const volume = volumeMultiplier === null ? [] : aggregated.filter((bar) => bar.hasVolume).map((bar) => ({ time: bar.time, value: bar.quoteVolume * volumeMultiplier, color: bar.close >= bar.open ? `${css("--green", "#43d58a")}38` : `${css("--red", "#ff6478")}38` }));
  const selected = list(payload?.trades).find((trade) => num(trade.trade_id) === num(state.selectedTradeId)) || null;
  const ready = bars.length > 1 && ensureTokenChart();
  nodes.chart_empty.hidden = Boolean(ready);
  nodes.token_chart.hidden = !ready;
  nodes.pump_link.hidden = !payload?.pump_fun_url;
  if (payload?.pump_fun_url) nodes.pump_link.href = payload.pump_fun_url;
  setText(nodes.chart_title, window.PUMPQUANT_PREVIEW && payload?.mint ? `Practice token ${payload.mint.replace(/^SYNTH-/, "#")}` : payload?.mint ? `Market cap · ${payload.mint.slice(0, 8)}…${payload.mint.slice(-6)} · ${state.currencyMode}` : "Pump.fun token market cap");
  setText(nodes.chart_subtitle, row && selected ? `${row.name} · trade ${num(selected.trade_id) + 1} · closed by ${exitWords(selected.exit_reason)}` : state.chartLoading ? "Loading token history…" : "Pick a strategy, then open one of its trades.");
  setText(nodes.equity_summary, selected ? sol(selected.net_pnl_quote) : "—");
  setText(nodes.chart_trade_detail, selected ? (window.PUMPQUANT_PREVIEW
    ? `Price moved ${pct(num(selected.exit_price) / num(selected.entry_price) - 1, 1)} from buy to sell · result ${sol(selected.net_pnl_quote)} after costs`
    : `Bought at ${marketCapText(selected.entry_price)} · sold at ${marketCapText(selected.exit_price)} · result ${sol(selected.net_pnl_quote)} after costs`) : "No trade selected");
  nodes.token_chart.dataset.timeframeSeconds = String(state.timeframeSeconds); nodes.token_chart.dataset.renderedBars = String(bars.length); nodes.token_chart.dataset.volumeBars = String(volume.length); nodes.token_chart.dataset.marketCapCurrency = state.currencyMode;
  setText(nodes.chart_live_note, `${window.PUMPQUANT_PREVIEW ? "Simulated data · " : ""}${timeframeLabel()} candles · Charts by TradingView`);
  if (!ready && payload?.mint) {
    const strong = nodes.chart_empty.querySelector("strong"); const detail = nodes.chart_empty.querySelector("span:last-child");
    if (strong) strong.textContent = state.supplyLoading ? "Loading verified token supply" : state.currencyMode === "USD" && num(state.solUsd) === null ? "Waiting for live SOL/USD" : "Market cap unavailable";
    if (detail) detail.textContent = "Market cap is rendered only from a validated SPL supply and decoded reserve prices.";
  }
  if (!ready) return;
  const viewKey = `${row?.key || "unknown"}:${payload?.mint || "unknown"}:${state.selectedTradeId ?? "none"}:${state.timeframeSeconds}`;
  const sameView = state.tokenChartViewKey === viewKey;
  const timeScale = state.tokenChart.timeScale();
  const visibleRange = sameView ? timeScale.getVisibleLogicalRange() : null;
  state.tokenSeries.applyOptions(window.PUMPQUANT_PREVIEW
    ? { lastValueVisible: false, priceLineVisible: false }
    : { priceFormat: { type: "custom", minMove: 0.01, formatter: (value) => formatCurrency(value, state.currencyMode, 2, true) } });
  state.tokenSeries.setData(bars);
  state.tokenVolumeSeries.setData(volume);
  const markers = [];
  list(payload.trades).forEach((trade) => {
    const active = num(trade.trade_id) === num(state.selectedTradeId);
    const signalTime = bucketSeconds(secondsFromNs(trade.signal_time_ns)); const entryTime = bucketSeconds(secondsFromNs(trade.entry_time_ns)); const exitTime = bucketSeconds(secondsFromNs(trade.exit_time_ns));
    if (signalTime !== null) markers.push({ time: signalTime, position: "aboveBar", color: css("--amber", "#f5b942"), shape: "circle", text: active ? "Signal" : "", size: active ? 2 : 1 });
    if (entryTime !== null) markers.push({ time: entryTime, position: "belowBar", color: css("--green", "#43d58a"), shape: "arrowUp", text: active ? "Buy" : "", size: active ? 2 : 1 });
    if (exitTime !== null) markers.push({ time: exitTime, position: "aboveBar", color: css("--red", "#ff6478"), shape: "arrowDown", text: active ? "Sell" : "", size: active ? 2 : 1 });
  });
  markers.sort((a, b) => a.time - b.time);
  state.tokenMarkers.setMarkers(markers);
  // The chart may have been sized while hidden; fit again once it has real width.
  if (!sameView) { timeScale.fitContent(); requestAnimationFrame(() => timeScale.fitContent()); }
  else if (visibleRange && Number.isFinite(visibleRange.from) && Number.isFinite(visibleRange.to)) timeScale.setVisibleLogicalRange(visibleRange);
  state.tokenChartViewKey = viewKey;
}

function renderStatRows(parent, rows) {
  if (!parent) return;
  parent.replaceChildren();
  rows.forEach(([label, value, tone = ""]) => {
    const item = document.createElement("div"); const dt = document.createElement("dt"); const dd = document.createElement("dd");
    dt.textContent = label; dd.textContent = has(value) && value !== "—" ? String(value) : "—"; dd.className = `${!has(value) || value === "—" ? "missing" : ""} ${tone}`.trim(); item.append(dt, dd); parent.append(item);
  });
}

function tradeValue(trade, paths) { return first(trade, paths, null); }
function durationText(value) { const n = num(value); if (n === null) return text(value, "—"); if (n < 60) return `${number(n, 0)}s`; if (n < 3600) return `${number(n / 60, 1)}m`; return `${number(n / 3600, 1)}h`; }
function renderTradeLedger(row) {
  if (!nodes.trade_ledger_body) return;
  const matches = row && state.tradeCandidate === row.key;
  const trades = matches ? state.trades : [];
  const pageStart = state.tradeTotal && trades.length ? state.tradeOffset + 1 : 0; const pageEnd = state.tradeOffset + trades.length;
  setText(nodes.trade_ledger_count, state.tradeLoading ? "Loading trades…" : state.tradeError ? "Evidence replay pending" : matches ? `${pageStart}–${pageEnd} of ${state.tradeTotal}` : "Trade evidence pending"); nodes.trade_ledger_body.replaceChildren();
  if (nodes.trade_prev) nodes.trade_prev.disabled = state.tradeLoading || state.tradeOffset <= 0;
  if (nodes.trade_next) nodes.trade_next.disabled = state.tradeLoading || pageEnd >= state.tradeTotal;
  if (!trades.length) { const tr = document.createElement("tr"); tr.className = "empty-row"; tr.innerHTML = `<td colspan="12"><span class="empty-glyph">⌁</span><strong>${state.tradeLoading ? "Loading individual trades" : state.tradeError ? "Trade evidence is not sealed yet" : "No reconciled trade evidence yet"}</strong><span>${state.tradeLoading ? "Reading the sealed candidate ledger…" : state.tradeError ? "The evidence replay is still required before this ledger can open." : "The evidence replay must reconcile before trades can appear."}</span></td>`; nodes.trade_ledger_body.append(tr); return; }
  trades.forEach((trade, index) => {
    const values = [
      number(state.tradeOffset + index + 1, 0), text(tradeValue(trade, ["mint", "token", "symbol", "asset"]), "—"),
      number(trade.fold, 0), formatNs(trade.entry_time_ns), unitPrice(trade.entry_price),
      formatNs(trade.exit_time_ns), unitPrice(trade.exit_price), sol(trade.quote_in, 4),
      sol(trade.quote_out), sol(trade.fixed_cost_quote), sol(trade.net_pnl_quote),
      text(trade.exit_reason, "—"),
    ];
    const tr = document.createElement("tr"); if (num(trade.trade_id) === num(state.selectedTradeId)) tr.className = "selected"; tr.addEventListener("click", () => loadTradeChart(row, trade, { reveal: true })); values.forEach((value, cellIndex) => { const td = document.createElement("td"); td.textContent = value; if (cellIndex >= 7 && cellIndex <= 10) td.className = "num"; tr.append(td); }); nodes.trade_ledger_body.append(tr);
  });
}

function selectIdea(row) {
  if (!row) return;
  const changed = state.selectedKey !== row.key;
  state.selectedKey = row.key;
  if (changed) { state.tradeCandidate = null; state.trades = []; state.tradeTotal = 0; state.tradeOffset = 0; state.tradeError = null; state.selectedTradeId = null; state.chartPayload = null; state.tokenSupply = null; state.tokenSupplySlot = null; }
  renderAll(state.snapshot);
  ensureCandidateTrades(row);
}

async function ensureCandidateTrades(row) {
  if (!row || state.tradeLoading || state.tradeCandidate === row.key) return;
  await loadTradePage(row, 0, true);
}

async function loadTradePage(row, offset, openFirst = false) {
  if (!row || state.tradeLoading) return;
  state.tradeLoading = true; state.tradeError = null; renderTradeLedger(row); renderTokenChart(row);
  const requestedKey = row.key;
  try {
    const selectedOffset = Math.max(0, Number(offset) || 0);
    const query = new URLSearchParams({ candidate: row.key, limit: String(state.tradePageSize), offset: String(selectedOffset) });
    const payload = await fetchJson(`${API.candidateTrades}?${query}`);
    if (state.selectedKey !== requestedKey) return;
    state.tradeCandidate = requestedKey; state.tradeOffset = selectedOffset; state.trades = list(payload.trades); state.tradeTotal = num(payload.total) ?? state.trades.length;
    renderTradeLedger(row);
    if (openFirst && state.trades.length) await loadTradeChart(row, state.trades[0]);
  } catch (error) {
    if (state.selectedKey === requestedKey) { state.tradeCandidate = requestedKey; state.tradeError = error instanceof Error ? error.message : String(error); appendLocalLog("warn", `Trade evidence unavailable: ${state.tradeError}`); }
  } finally {
    state.tradeLoading = false;
    if (state.selectedKey === requestedKey) renderTradeLedger(row);
  }
}

// reveal: scroll the chart into view; only for a trade the visitor picked, never on load.
async function loadTradeChart(row, trade, { reveal = false } = {}) {
  if (!row || !trade || state.chartLoading) return;
  state.selectedTradeId = trade.trade_id; state.chartLoading = true; state.supplyLoading = true; state.chartPayload = null; state.tokenSupply = null; state.tokenSupplySlot = null; renderTradeLedger(row); renderTokenChart(row);
  const requestedTrade = num(trade.trade_id);
  try {
    const query = new URLSearchParams({ candidate: row.key, trade_id: String(trade.trade_id) });
    const supplyQuery = new URLSearchParams({ mint: String(trade.mint) });
    const [chartResult, supplyResult] = await Promise.allSettled([fetchJson(`${API.tradeChart}?${query}`), fetchJson(`${API.tokenSupply}?${supplyQuery}`)]);
    if (chartResult.status !== "fulfilled") throw chartResult.reason;
    if (state.selectedKey !== row.key || num(state.selectedTradeId) !== requestedTrade) return;
    state.chartPayload = chartResult.value;
    if (supplyResult.status === "fulfilled") { state.tokenSupply = num(supplyResult.value.token_supply); state.tokenSupplySlot = num(supplyResult.value.slot); }
    else appendLocalLog("warn", `Token supply unavailable: ${supplyResult.reason instanceof Error ? supplyResult.reason.message : String(supplyResult.reason)}`);
    renderTradeLedger(row); renderTokenChart(row);
    if (reveal) document.querySelector("#workspace")?.scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (error) {
    appendLocalLog("warn", `Token chart unavailable: ${error instanceof Error ? error.message : String(error)}`);
  } finally {
    state.chartLoading = false; state.supplyLoading = false;
    if (state.selectedKey === row.key) renderTokenChart(row);
  }
}

function primitiveEntries(source, prefix = "", output = []) {
  if (!source || typeof source !== "object" || output.length >= 36) return output;
  Object.entries(source).forEach(([key, value]) => {
    if (output.length >= 36) return;
    const label = prefix ? `${prefix}.${key}` : key;
    if (value === null || ["string", "number", "boolean"].includes(typeof value)) output.push([label, value]);
    else if (!Array.isArray(value) && typeof value === "object") primitiveEntries(value, label, output);
  });
  return output;
}

function renderParameters(row) {
  if (!nodes.parameter_grid) return;
  nodes.parameter_grid.replaceChildren(); const entries = primitiveEntries(row?.parameters || {});
  if (!entries.length) { const item = document.createElement("div"); item.innerHTML = `<dt>Parameters</dt><dd>Not reported</dd>`; nodes.parameter_grid.append(item); }
  entries.forEach(([label, value]) => { const item = document.createElement("div"); const dt = document.createElement("dt"); const dd = document.createElement("dd"); dt.textContent = label.replaceAll("_", " "); dd.textContent = text(value, "—"); item.append(dt, dd); nodes.parameter_grid.append(item); });
  setText(nodes.rule_title, row?.name, "No research selected"); setText(nodes.rule_detail, row?.mechanism, "The exact rule and market translation will appear here."); setText(nodes.rule_source, row ? row.source : null, "Source not reported");
}

function renderProfessionalReport(row) {
  setText(nodes.performance_basis, row ? `${row.name} · after-cost fields only where published; PSR/DSR is robustness evidence, R² is diagnostic only` : "Select research to inspect its complete report");
  renderStatRows(nodes.profit_stats, row ? [
    ["Net profit", row.netPnl === null ? null : money(row.netPnl, row.currency), row.netPnl >= 0 ? "positive" : "negative"], ["Gross profit", row.grossProfit === null ? null : money(row.grossProfit, row.currency)], ["Gross loss", row.grossLoss === null ? null : money(row.grossLoss, row.currency)], ["Profit factor", row.profitFactor === null ? null : number(row.profitFactor)], ["Expectancy / trade", row.expectancy === null ? null : money(row.expectancy, row.currency)], ["Average trade", row.averageTrade === null ? null : money(row.averageTrade, row.currency)], ["Outcomes counted", row.trades === null ? null : number(row.trades, 0)], ["Winning / losing / even", row.wins === null && row.losses === null ? null : `${number(row.wins, 0)} / ${number(row.losses, 0)} / ${number(row.breakevens, 0)}`], ["Win rate", row.winRate === null ? null : pct(row.winRate / 100)], ["Average win / loss", row.avgWin === null && row.avgLoss === null ? null : `${money(row.avgWin, row.currency)} / ${money(row.avgLoss, row.currency)}`], ["Payoff ratio", row.payoffRatio === null ? null : number(row.payoffRatio)], ["Largest win / loss", row.largestWin === null && row.largestLoss === null ? null : `${money(row.largestWin, row.currency)} / ${money(row.largestLoss, row.currency)}`], ["Max win / loss streak", row.maxConsecutiveWins === null && row.maxConsecutiveLosses === null ? null : `${number(row.maxConsecutiveWins, 0)} / ${number(row.maxConsecutiveLosses, 0)}`],
  ] : []);
  renderStatRows(nodes.risk_stats, row ? [
    ["Return", row.returnPct === null ? null : pct(row.returnPct / 100)], ["Maximum drawdown", row.maxDrawdown === null ? null : money(row.maxDrawdown, row.currency)], ["Maximum drawdown %", row.maxDrawdownPct === null ? null : pct(row.maxDrawdownPct / 100)], ["Return / drawdown", row.returnDrawdown === null ? null : number(row.returnDrawdown)], ["Sharpe ratio", row.sharpe === null ? null : number(row.sharpe)], ["Sortino ratio", row.sortino === null ? null : number(row.sortino)], ["PSR / DSR probability", row.probability === null ? null : pct(row.probability / 100)], ["Equity R² (diagnostic)", row.rSquared === null ? null : number(row.rSquared, 3)], ["Ulcer index", row.ulcerIndex === null ? null : number(row.ulcerIndex)], ["Volatility", row.volatility === null ? null : number(row.volatility)], ["Downside deviation", row.downsideDeviation === null ? null : number(row.downsideDeviation)], ["Average MAE / MFE", row.mae === null && row.mfe === null ? null : `${money(row.mae, row.currency)} / ${money(row.mfe, row.currency)}`],
  ] : []);
  renderStatRows(nodes.execution_stats, row ? [
    ["Fees / commissions", row.fees === null ? null : money(row.fees, row.currency)], ["Slippage / price impact", row.slippage === null ? null : money(row.slippage, row.currency)], ["Executable volume", row.volume === null ? null : money(row.volume, row.currency)], ["Turnover", row.turnover === null ? null : money(row.turnover, row.currency)], ["Market exposure", row.exposure === null ? null : pct(row.exposure / 100)], ["Fill rate", row.fillRate === null ? null : pct(row.fillRate / 100)], ["Rejected fills", row.rejectedFills === null ? null : number(row.rejectedFills, 0)], ["Latency cost", row.latencyCost === null ? null : money(row.latencyCost, row.currency)], ["Benchmark curve", row.benchmark.length ? `${row.benchmark.length} observations` : null], ["Execution basis", "Exact fields shown only when emitted"],
  ] : []);
  renderTradeLedger(row); renderParameters(row);
}

function renderWallet(snapshot) {
  const walletRaw = first(snapshot, ["execution.wallet", "execution.shared_10_wallet", "wallet"], null); const wallet = Array.isArray(walletRaw) ? { timeline: walletRaw } : objectAt(snapshot, ["execution.wallet", "wallet"]); const timeline = points(first(wallet, ["timeline", "equity", "events"], [])); const drawn = drawLine(nodes.wallet_chart, timeline, { color: css("--green", "#43d58a"), fill: true, zeroBaseline: true }); nodes.wallet_empty.hidden = drawn; nodes.wallet_chart.hidden = drawn ? false : true;
  const equity = first(wallet, ["equity", "ending_equity", "final_equity", "current_equity"], timeline.length ? timeline[timeline.length - 1].value : null); const cash = first(wallet, ["available_cash", "cash", "available"], null); const positions = first(wallet, ["open_positions", "positions", "position_count"], null); const contention = first(wallet, ["contention", "capital_contention", "contention_fraction"], null);
  setText(nodes.wallet_seed_label, `SHARED STARTING WALLET · ${money(10, "USD", 4)}`); setText(nodes.capacity_currency_label, `${state.currencyMode} / day ladder`); setText(nodes.wallet_summary, equity === null ? null : money(equity)); setText(nodes.wallet_equity, equity === null ? null : money(equity)); setText(nodes.wallet_cash, cash === null ? null : money(cash)); setText(nodes.wallet_positions, positions === null ? null : number(positions, 0)); setText(nodes.wallet_contention, contention === null ? null : pct(contention));
  const capacity = list(first(snapshot, ["execution.capacity", "capacity"], [])); const values = capacity.map((item) => ({ label: text(first(item, ["day", "window", "label", "bucket"], "day")), value: num(first(item, ["net_usd_per_day", "usd_per_day", "daily_net", "capacity_usd_per_day", "value"], null)) })).filter((item) => item.value !== null); nodes.capacity_ladder.replaceChildren(); if (!values.length) { nodes.capacity_ladder.innerHTML = `<div class="empty-state"><span class="empty-glyph">⌁</span><strong>Capacity pending</strong><span>Only measured, net-of-cost capacity is shown.</span></div>`; setText(nodes.capacity_summary, null); } else { const max = Math.max(...values.map((item) => Math.abs(item.value)), 1); values.slice(0, 12).forEach((item) => { const row = document.createElement("div"); row.className = "ladder-row"; row.innerHTML = `<span></span><div class="ladder-track"><i></i></div><strong></strong>`; row.querySelector("span").textContent = item.label; row.querySelector("i").style.width = `${Math.min(100, Math.max(0, Math.abs(item.value) / max * 100))}%`; row.querySelector("strong").textContent = money(item.value); nodes.capacity_ladder.append(row); }); setText(nodes.capacity_summary, money(Math.max(...values.map((item) => item.value)))); }
  const status = first(snapshot, ["execution.status"], null); setText(nodes.execution_state, normalizeStatus(status || (values.length || timeline.length ? "reported" : "pending"))); setClass(nodes.execution_state, "section-state", statusKind(status || (values.length || timeline.length ? "ready" : "pending")));
}

function details(parent, values) { parent.replaceChildren(); Object.entries(values).forEach(([label, value]) => { const row = document.createElement("div"); row.innerHTML = `<dt></dt><dd></dd>`; row.querySelector("dt").textContent = label; row.querySelector("dd").textContent = text(value, "—"); parent.append(row); }); }
function renderValidation(snapshot) {
  const validation = objectAt(snapshot, ["validation"]); const gates = list(first(validation, ["gates"], first(snapshot, ["gates"], []))); nodes.gate_grid.replaceChildren(); if (!gates.length) { nodes.gate_grid.innerHTML = `<div class="empty-state"><span class="empty-glyph">⌁</span><strong>Gate results pending</strong><span>Nothing is promoted from a running screen.</span></div>`; setText(nodes.gates_summary, "No decisions yet"); } else { gates.forEach((gate) => { const result = text(first(gate, ["status", "result", "state"], "pending")); const card = document.createElement("div"); card.className = `gate-card ${statusKind(result)}`; card.innerHTML = `<strong></strong><span></span>`; card.querySelector("strong").textContent = `${text(first(gate, ["name", "gate", "label"], "Gate"))} · ${normalizeStatus(result)}`; card.querySelector("span").textContent = text(first(gate, ["detail", "message", "reason"], "No detail reported")); nodes.gate_grid.append(card); }); setText(nodes.gates_summary, `${gates.length} gate${gates.length === 1 ? "" : "s"} reported`); }
  const forward = objectAt(validation, ["forward"]); const holdout = objectAt(validation, ["holdout"]); const forwardStatus = text(first(forward, ["status", "state", "result"], "pending")); setText(nodes.forward_badge, normalizeStatus(forwardStatus)); setClass(nodes.forward_badge, "state-chip", statusKind(forwardStatus)); setText(nodes.forward_detail, text(first(forward, ["detail", "message", "reason"], null), "No forward validation has been reported.")); details(nodes.forward_list, { Status: normalizeStatus(forwardStatus), Window: first(forward, ["window", "interval"], null), Trades: first(forward, ["trades", "trade_count"], null), "Net P&L": first(forward, ["net_pnl", "pnl"], null) === null ? null : money(first(forward, ["net_pnl", "pnl"], null)) });
  const holdoutStatus = text(first(holdout, ["status", "state"], first(snapshot, ["holdout.status"], "locked"))); setText(nodes.holdout_badge, normalizeStatus(holdoutStatus)); setClass(nodes.holdout_badge, "state-chip", holdoutStatus.toLowerCase().includes("lock") ? "ready" : statusKind(holdoutStatus)); setText(nodes.holdout_detail, text(first(holdout, ["detail", "message"], null), "Untouched until the preregistered development gates pass.")); details(nodes.holdout_list, { Start: first(holdout, ["start", "start_slot_inclusive"], first(snapshot, ["holdout.start_slot_inclusive"], null)), Accessed: first(holdout, ["accessed"], false), Result: normalizeStatus(holdoutStatus) }); const validationStatus = first(validation, ["status"], null); setText(nodes.validation_state, normalizeStatus(validationStatus || (gates.length ? "reported" : "pending"))); setClass(nodes.validation_state, "section-state", statusKind(validationStatus || (gates.length ? "ready" : "pending")));
}

function logItems(snapshot) { const raw = first(snapshot, ["logs.events", "logs", "events"], []); return list(raw).map((item) => ({ level: String(first(item, ["level", "severity", "kind"], "info")).toLowerCase(), time: first(item, ["timestamp", "timestamp_epoch", "time", "created_at"], null), message: text(first(item, ["message", "detail", "event", "name"], null), "Event") })); }
function logKey(item) { return `${item.level}|${has(item.time) ? String(item.time) : "no-time"}|${item.message}`; }
function renderLogs(snapshot) { const merged = [...state.events, ...logItems(snapshot)]; const seen = new Set(); state.events = merged.filter((item) => { const key = logKey(item); if (seen.has(key)) return false; seen.add(key); return true; }).slice(-120); const level = state.logLevel; const items = state.events.filter((item) => level === "all" || item.level === level); nodes.log_stream.replaceChildren(); if (!items.length) { nodes.log_stream.innerHTML = `<div class="empty-state"><span class="empty-glyph">⌁</span><strong>Waiting for events</strong><span>Live stage and execution events will appear here.</span></div>`; return; } items.slice(-80).reverse().forEach((item) => { const row = document.createElement("div"); row.className = "log-line"; row.innerHTML = `<span class="log-time"></span><span class="log-level"></span><span class="log-message"></span>`; row.querySelector(".log-time").textContent = formatTime(item.time); row.querySelector(".log-level").textContent = item.level; row.querySelector(".log-level").className = `log-level ${item.level}`; row.querySelector(".log-message").textContent = item.message; nodes.log_stream.append(row); }); }

// Hide a group of figures when none of them has a value, rather than showing a wall of dashes.
function hideEmptyGroups() {
  const empty = (root, selector) => [...root.querySelectorAll(selector)].every((node) => node.textContent.trim() === "—");
  const ribbon = document.querySelector(".cost-ribbon");
  const note = document.querySelector(".ribbon-note");
  if (ribbon) { ribbon.hidden = empty(ribbon, "strong"); if (note) note.hidden = ribbon.hidden; }
  document.querySelectorAll(".compact-dl").forEach((list) => { list.hidden = empty(list, "dd"); });
}

function renderAll(payload) {
  const snapshot = unwrap(payload);
  if (!snapshot) return;
  const planSha256 = text(first(snapshot, ["progress.plan_sha256"], null), "");
  const sequence = finiteOr(first(snapshot, ["progress.sequence"], null));
  if (planSha256 && state.planSha256 === planSha256 && sequence !== null && state.progressSequence !== null && sequence < state.progressSequence) return;
  if (planSha256 && planSha256 !== state.planSha256) { state.planSha256 = planSha256; state.progressSequence = null; state.events = []; }
  if (sequence !== null) state.progressSequence = Math.max(state.progressSequence ?? sequence, sequence);
  state.snapshot = snapshot;
  state.rows = candidateRows(snapshot);
  state.sourceUpdatedAt = first(snapshot, ["progress.heartbeat_at_epoch", "progress.updated_at_epoch", "generated_at"], null);
  state.lastCompletedAt = first(snapshot, ["progress.liveness.last_completed_at_epoch"], null);
  renderRun(snapshot); renderResearchBrief(snapshot); renderHeadline(snapshot, state.rows); updateFilters(); renderResearch(); const selected = renderSelected(state.rows); renderTokenChart(selected); renderProfessionalReport(selected); renderWallet(snapshot); renderValidation(snapshot); renderLogs(snapshot); updateAge(); if (selected) void ensureCandidateTrades(selected);
  hideEmptyGroups();
}

function setConnection(kind, label) { state.connection = kind; nodes.connection_badge.dataset.state = kind; setText(nodes.connection_label, label || ({ live: "Live", polling: "Connected", stale: "Out of date", error: "Offline", connecting: "Connecting" }[kind] || "Connecting")); setText(nodes.sidebar_connection_label, label || ({ live: "Live", polling: "Connected", stale: "Out of date", error: "Can’t reach the server", connecting: "Connecting…" }[kind] || "Connecting…")); nodes.sidebar_led.dataset.state = kind === "live" || kind === "polling" ? "live" : kind === "error" ? "error" : "warn"; }
function timestampMs(value) { if (!has(value)) return NaN; const numeric = num(value); if (numeric !== null) return numeric < 1e12 ? numeric * 1000 : numeric; const date = new Date(value); return date.getTime(); }
function updateAge() { const transportTimestamp = timestampMs(state.receivedAt); if (!Number.isFinite(transportTimestamp)) { setText(nodes.update_age, "No snapshot"); return; } const transportAge = Math.max(0, Math.floor((Date.now() - transportTimestamp) / 1000)); setText(nodes.update_age, transportAge < 2 ? "updated now" : `updated ${transportAge}s ago`); const workTimestamp = timestampMs(state.lastCompletedAt); const workAge = Number.isFinite(workTimestamp) ? Math.max(0, Math.floor((Date.now() - workTimestamp) / 1000)) : null; setText(nodes.run_work_age, workAge === null ? "—" : workAge < 2 ? "now" : `${duration(workAge)} ago`); if (transportAge > 15 && state.connection !== "error") setConnection("stale", "Out of date"); }

function updateCurrencyControls() {
  [nodes.currency_sol, nodes.currency_usd].forEach((button) => { const active = button.id.endsWith(state.currencyMode.toLowerCase()); button.classList.toggle("active", active); button.setAttribute("aria-pressed", String(active)); });
  const rate = num(state.solUsd); const age = state.fxFetchedAt ? Math.max(0, Math.floor((Date.now() - state.fxFetchedAt.getTime()) / 1000)) : null;
  nodes.currency_usd.disabled = rate === null;
  nodes.currency_usd.title = rate === null ? "No USD price available right now" : "";
  setText(nodes.fx_status, rate === null ? "Prices in SOL" : `1 SOL = ${formatCurrency(rate, "USD", 2)} · ${state.fxStale ? "stale" : age !== null && age < 5 ? "live" : `${age}s`}`);
}

async function setCurrencyMode(mode) {
  if (!['SOL', 'USD'].includes(mode) || state.currencyMode === mode) return;
  if (mode === "USD" && num(state.solUsd) === null) await fetchFx();
  if (mode === "USD" && num(state.solUsd) === null) { updateCurrencyControls(); return; }
  state.currencyMode = mode; updateCurrencyControls();
  if (state.snapshot) renderAll(state.snapshot);
  renderAtlas();
}

function setTimeframe(seconds, custom = false) {
  const interval = Number(seconds);
  if (!Number.isInteger(interval) || interval < 1 || interval > 86_400) { setText(nodes.custom_timeframe_error, "Choose 1 second to 24 hours"); return; }
  state.timeframeSeconds = interval; state.tokenChartViewKey = null; nodes.custom_timeframe_error.textContent = "";
  document.querySelectorAll(".timeframe-button").forEach((button) => button.classList.toggle("active", custom ? button.id === "custom-timeframe-toggle" : Number(button.dataset.seconds) === interval));
  const row = state.rows.find((item) => item.key === state.selectedKey); renderTokenChart(row || null);
}

function transformChartRange(transform) {
  if (!state.tokenChart) return;
  const scale = state.tokenChart.timeScale(); const range = scale.getVisibleLogicalRange(); if (!range) return;
  const next = transform(range); if (next && Number.isFinite(next.from) && Number.isFinite(next.to) && next.to - next.from >= 2) scale.setVisibleLogicalRange(next);
}
function zoomChart(factor) { transformChartRange((range) => { const center = (range.from + range.to) / 2; const half = Math.max(1, ((range.to - range.from) * factor) / 2); return { from: center - half, to: center + half }; }); }
function scrollChart(direction) { transformChartRange((range) => { const shift = (range.to - range.from) * 0.22 * direction; return { from: range.from + shift, to: range.to + shift }; }); }

async function fetchFx() {
  if (window.PUMPQUANT_PREVIEW) { setText(nodes.fx_status, "Prices in SOL"); nodes.currency_usd.disabled = true; return; }
  try {
    const payload = await fetchJson(API.fx); const rate = num(payload.sol_usd); if (rate === null || rate <= 0) throw new Error("invalid SOL/USD quote");
    state.solUsd = rate; state.fxFetchedAt = new Date((num(payload.fetched_at_epoch) || Date.now() / 1000) * 1000); state.fxStale = Boolean(payload.stale); updateCurrencyControls();
    if (state.snapshot) renderAll(state.snapshot);
  } catch (error) {
    state.fxStale = true; updateCurrencyControls(); appendLocalLog("warn", `SOL/USD unavailable: ${error instanceof Error ? error.message : String(error)}`);
  }
}

function renderAtlas() {
  const summary = state.atlasSummary || {};
  setText(nodes.atlas_launches, summary.launches === undefined ? null : number(summary.launches, 0));
  setText(nodes.atlas_active, summary.active === undefined ? null : number(summary.active, 0));
  setText(nodes.atlas_active_rate, summary.active_rate === undefined ? "At least one swap" : `${pct(summary.active_rate)} of launches`);
  setText(nodes.atlas_completed, summary.completed === undefined ? null : number(summary.completed, 0));
  setText(nodes.atlas_completion_rate, summary.completion_rate === undefined ? "Bonding curve" : `${pct(summary.completion_rate)} of launches`);
  [2, 5, 10, 20].forEach((target) => {
    setText(nodes[`atlas_${target}x`], summary[`reached_${target}x`] === undefined ? null : number(summary[`reached_${target}x`], 0));
    setText(nodes[`atlas_${target}x_rate`], summary[`reached_${target}x_rate`] === undefined ? "Gross path" : `${pct(summary[`reached_${target}x_rate`])} of launches`);
  });
  setText(nodes.atlas_count, state.atlasLoading ? "Loading atlas…" : `${number(state.atlasTotal, 0)} matching tokens`);
  nodes.atlas_body.replaceChildren();
  if (!state.atlasTokens.length) {
    nodes.atlas_body.innerHTML = `<tr class="empty-row"><td colspan="9"><span class="empty-glyph">⌁</span><strong>${state.atlasLoading ? "Loading opportunity atlas" : "No matching tokens"}</strong><span>Adjust the minimum peak filter.</span></td></tr>`;
    return;
  }
  state.atlasTokens.forEach((token) => {
    const row = document.createElement("tr");
    const identity = document.createElement("td");
    const link = document.createElement("a");
    link.href = `https://pump.fun/coin/${encodeURIComponent(token.mint)}`;
    link.target = "_blank";
    link.rel = "noreferrer";
    link.textContent = `${text(token.symbol, "TOKEN")} · ${String(token.mint).slice(0, 6)}…${String(token.mint).slice(-4)}`;
    identity.append(link);
    const values = [
      token.maximum_multiple === null ? "—" : `${number(token.maximum_multiple)}×`,
      money(token.first_market_cap_sol, "SOL", 2),
      money(token.peak_market_cap_sol, "SOL", 2),
      token.terminal_multiple === null ? "—" : `${number(token.terminal_multiple)}×`,
      sol(token.quote_volume_sol, 2),
      number(token.swap_event_count, 0),
      token.completed_bonding_curve ? "Yes" : "No",
      duration(token.seconds_to_10_0x) || "—",
    ];
    row.append(identity);
    values.forEach((value, index) => { const cell = document.createElement("td"); cell.textContent = value; if (index !== 6) cell.className = "num"; row.append(cell); });
    nodes.atlas_body.append(row);
  });
  setText(nodes.atlas_footnote, `Showing ${number(state.atlasTokens.length, 0)} of ${number(state.atlasTotal, 0)} · gross reserve-mark paths, not executable returns`);
}

async function fetchAtlas() {
  if (state.atlasLoading) return;
  state.atlasLoading = true; renderAtlas();
  try {
    const minimum = encodeURIComponent(nodes.atlas_minimum.value || "0");
    const sort = encodeURIComponent(nodes.atlas_sort.value || "multiple");
    const [summary, tokens] = await Promise.all([
      fetchJson(API.atlasSummary),
      fetchJson(`${API.atlasTokens}?limit=100&offset=0&sort=${sort}&minimum_multiple=${minimum}`),
    ]);
    state.atlasSummary = summary;
    state.atlasTokens = Array.isArray(tokens.tokens) ? tokens.tokens : [];
    state.atlasTotal = num(tokens.total) || 0;
  } catch (error) {
    state.atlasTokens = [];
    appendLocalLog("error", `Atlas unavailable: ${error instanceof Error ? error.message : String(error)}`);
  } finally {
    state.atlasLoading = false; renderAtlas();
  }
}

function renderSignals() {
  setText(nodes.signal_count, state.signalsLoading ? "Loading patterns…" : `${number(state.signalPatterns.length, 0)} strongest cells`);
  nodes.signal_body.replaceChildren();
  if (!state.signalPatterns.length) {
    nodes.signal_body.innerHTML = `<tr class="empty-row"><td colspan="7"><span class="empty-glyph">⌁</span><strong>${state.signalsLoading ? "Loading causal patterns" : "No supported pattern cells"}</strong><span>Try another checkpoint or future target.</span></td></tr>`;
    return;
  }
  state.signalPatterns.forEach((pattern) => {
    const row = document.createElement("tr");
    const feature = String(pattern.feature || "unknown").replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
    const range = `${number(pattern.feature_min, 5)} → ${number(pattern.feature_max, 5)}`;
    const values = [feature, `D${number(pattern.decile, 0)}`, range, number(pattern.sample_size, 0), pct(pattern.baseline_rate), pct(pattern.outcome_rate), `${number(pattern.lift, 2)}×`];
    values.forEach((value, index) => { const cell = document.createElement("td"); cell.textContent = value; if (index >= 2) cell.className = "num"; row.append(cell); });
    nodes.signal_body.append(row);
  });
  const age = duration(num(nodes.signal_age.value)) || `${nodes.signal_age.value}s`;
  setText(nodes.signal_footnote, `Top supported feature buckets at age ${age}; predicting a future ${nodes.signal_target.value}× gross move.`);
}

async function fetchSignals() {
  const request = ++state.signalRequest;
  state.signalsLoading = true; renderSignals();
  try {
    const age = encodeURIComponent(nodes.signal_age.value || "60");
    const target = encodeURIComponent(nodes.signal_target.value || "5");
    const payload = await fetchJson(`${API.atlasPatterns}?checkpoint_type=age&checkpoint_value=${age}&target=${target}&minimum_sample=500&limit=50`);
    if (request === state.signalRequest) state.signalPatterns = Array.isArray(payload.patterns) ? payload.patterns : [];
  } catch (error) {
    if (request === state.signalRequest) {
      state.signalPatterns = [];
      appendLocalLog("error", `Atlas signals unavailable: ${error instanceof Error ? error.message : String(error)}`);
    }
  } finally {
    if (request === state.signalRequest) { state.signalsLoading = false; renderSignals(); }
  }
}

function readableFeature(value) {
  return String(value || "unknown").replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function renderOpportunityIdeas() {
  setText(nodes.idea_count, state.ideasLoading ? "Loading ideas…" : `${number(state.opportunityIdeaTotal, 0)} matching ideas`);
  nodes.idea_body.replaceChildren();
  if (!state.opportunityIdeas.length) {
    nodes.idea_body.innerHTML = `<tr class="empty-row"><td colspan="10"><span class="empty-glyph">⌁</span><strong>${state.ideasLoading ? "Loading pattern ideas" : "No matching ideas"}</strong><span>Change the age, target, or rule-size filter.</span></td></tr>`;
    return;
  }
  state.opportunityIdeas.forEach((idea) => {
    const row = document.createElement("tr");
    const rule = `${readableFeature(idea.feature_a)} ${String(idea.direction_a).toUpperCase()}${idea.feature_b ? ` + ${readableFeature(idea.feature_b)} ${String(idea.direction_b).toUpperCase()}` : ""}`;
    const values = [rule, duration(idea.age_seconds) || `${number(idea.age_seconds, 0)}s`, text(idea.idea_kind), number(idea.sample_size, 0), number(idea.hit_count, 0), pct(idea.baseline_rate), pct(idea.outcome_rate), `${number(idea.lift, 2)}×`, `${number(idea.conservative_lift, 2)}×`, number(idea.positive_age_count, 0)];
    values.forEach((value, index) => { const cell = document.createElement("td"); cell.textContent = value; if ([1, 3, 4, 5, 6, 7, 8, 9].includes(index)) cell.className = "num"; row.append(cell); });
    nodes.idea_body.append(row);
  });
  setText(nodes.idea_footnote, `Showing ${number(state.opportunityIdeas.length, 0)} of ${number(state.opportunityIdeaTotal, 0)} supported development ideas · conservative lift is the 95% lower-bound rate divided by baseline.`);
}

async function fetchOpportunityIdeas() {
  const request = ++state.ideaRequest;
  state.ideasLoading = true; renderOpportunityIdeas();
  try {
    const target = encodeURIComponent(nodes.idea_target.value || "5");
    const kind = encodeURIComponent(nodes.idea_kind.value || "all");
    const age = nodes.idea_age.value ? `&age_seconds=${encodeURIComponent(nodes.idea_age.value)}` : "";
    const payload = await fetchJson(`${API.atlasIdeas}?target=${target}&idea_kind=${kind}${age}&minimum_sample=300&minimum_conservative_lift=1&minimum_positive_ages=2&limit=100`);
    if (request === state.ideaRequest) {
      state.opportunityIdeas = Array.isArray(payload.ideas) ? payload.ideas : [];
      state.opportunityIdeaTotal = num(payload.total) || 0;
    }
  } catch (error) {
    if (request === state.ideaRequest) {
      state.opportunityIdeas = []; state.opportunityIdeaTotal = 0;
      appendLocalLog("error", `Opportunity ideas unavailable: ${error instanceof Error ? error.message : String(error)}`);
    }
  } finally {
    if (request === state.ideaRequest) { state.ideasLoading = false; renderOpportunityIdeas(); }
  }
}

function historicalUsd(value) {
  const amount = num(value);
  return amount === null ? "—" : `$${number(amount, 0)}`;
}

function renderVelocity() {
  const threshold = num(nodes.velocity_threshold.value) || 10_000;
  setText(nodes.velocity_count, state.velocityLoading ? "Loading velocity…" : `${state.velocityRows.length} time rules`);
  nodes.velocity_body.replaceChildren();
  if (!state.velocityRows.length) {
    nodes.velocity_body.innerHTML = `<tr class="empty-row"><td colspan="17"><span class="empty-glyph">⌁</span><strong>${state.velocityLoading ? "Loading velocity matrix" : "No velocity rows"}</strong><span>Select another market-cap threshold.</span></td></tr>`;
    return;
  }
  state.velocityRows.forEach((item) => {
    const row = document.createElement("tr");
    const values = [duration(item.max_seconds) || `${number(item.max_seconds, 0)}s`, number(item.sample_size, 0), number(item.completion_at_risk_tokens, 0), number(item.signals_per_hour, 2), number(item.at_risk_signals_per_hour, 2), pct(item.launch_reach_rate), pct(item.at_risk_launch_reach_rate), pct(item.completion_rate), `${number(item.completion_lift, 2)}×`, pct(item.completion_900s_rate), pct(item.ath_50000_rate), pct(item.ath_100000_rate), pct(item.ath_200000_rate), pct(item.ath_500000_rate), pct(item.ath_1000000_rate), historicalUsd(item.at_risk_median_observed_ath_usd), `${number(item.at_risk_median_ath_from_threshold, 2)}×`];
    values.forEach((value, index) => { const cell = document.createElement("td"); cell.textContent = value; if (index > 0) cell.className = "num"; row.append(cell); });
    nodes.velocity_body.append(row);
  });
  setText(nodes.velocity_footnote, `${historicalUsd(threshold)} market-cap proof · completion baseline ${pct(state.velocityRows[0]?.completion_base_rate)} across all launches.`);
}

async function fetchVelocity() {
  const request = ++state.velocityRequest;
  state.velocityLoading = true; renderVelocity();
  try {
    const threshold = encodeURIComponent(nodes.velocity_threshold.value || "10000");
    const payload = await fetchJson(`${API.atlasVelocity}?threshold_usd=${threshold}`);
    if (request === state.velocityRequest) state.velocityRows = Array.isArray(payload.rows) ? payload.rows : [];
  } catch (error) {
    if (request === state.velocityRequest) {
      state.velocityRows = [];
      appendLocalLog("error", `Velocity atlas unavailable: ${error instanceof Error ? error.message : String(error)}`);
    }
  } finally {
    if (request === state.velocityRequest) { state.velocityLoading = false; renderVelocity(); }
  }
}

async function fetchJson(url) { const response = await fetch(url, { cache: "no-store", headers: { Accept: "application/json" } }); if (!response.ok) throw new Error(`${response.status} ${response.statusText}`); return response.json(); }
async function fetchSnapshot() { if (state.polling) return; state.polling = true; try { const payload = await fetchJson(API.snapshot); state.receivedAt = new Date(); renderAll(payload); if (state.connection !== "live") setConnection("polling", "Connected"); updateAge(); } catch (error) { appendLocalLog("error", `Snapshot unavailable: ${error instanceof Error ? error.message : String(error)}`); setConnection("error", "Unavailable"); } finally { state.polling = false; } }
async function fetchHealth() { try { state.health = unwrap(await fetchJson(API.health)); if (state.connection === "error" && state.health?.ok) setConnection("polling", "Live polling"); } catch { /* snapshot polling reports the visible error */ } }
function connectEvents() { if (!("EventSource" in window)) return; if (state.eventSource) state.eventSource.close(); const source = new EventSource(API.events); state.eventSource = source; source.onopen = () => { setConnection("live", "Live"); updateAge(); }; source.onmessage = (event) => { try { const payload = JSON.parse(event.data); state.receivedAt = new Date(); renderAll(payload); setConnection("live", "Live"); updateAge(); } catch (error) { appendLocalLog("error", `Event ignored: ${error instanceof Error ? error.message : String(error)}`); } }; source.onerror = () => { source.close(); state.eventSource = null; if (state.connection !== "error") setConnection("polling", "Live polling"); window.setTimeout(connectEvents, 3500); }; }
function appendLocalLog(level, message) { state.events.push({ level, time: new Date(), message }); state.events = state.events.slice(-120); if (state.snapshot) renderLogs(state.snapshot); }

function init() {
  document.documentElement.dataset.chartLibrary = window.LightweightCharts ? `TradingView ${window.LightweightCharts.version()}` : "unavailable";
  document.querySelectorAll(".nav-link").forEach((link) => link.addEventListener("click", () => { document.querySelectorAll(".nav-link").forEach((item) => item.classList.toggle("active", item === link)); }));
  nodes.refresh_button.addEventListener("click", async () => { nodes.refresh_button.disabled = true; await Promise.all([fetchSnapshot(), fetchHealth(), fetchFx(), fetchAtlas(), fetchSignals(), fetchOpportunityIdeas(), fetchVelocity()]); nodes.refresh_button.disabled = false; });
  nodes.currency_sol.addEventListener("click", () => { void setCurrencyMode("SOL"); }); nodes.currency_usd.addEventListener("click", () => { void setCurrencyMode("USD"); });
  nodes.atlas_minimum.addEventListener("change", () => { void fetchAtlas(); }); nodes.atlas_sort.addEventListener("change", () => { void fetchAtlas(); }); nodes.atlas_refresh.addEventListener("click", () => { void fetchAtlas(); });
  nodes.signal_age.addEventListener("change", () => { void fetchSignals(); }); nodes.signal_target.addEventListener("change", () => { void fetchSignals(); }); nodes.signal_refresh.addEventListener("click", () => { void fetchSignals(); });
  nodes.idea_age.addEventListener("change", () => { void fetchOpportunityIdeas(); }); nodes.idea_target.addEventListener("change", () => { void fetchOpportunityIdeas(); }); nodes.idea_kind.addEventListener("change", () => { void fetchOpportunityIdeas(); }); nodes.idea_refresh.addEventListener("click", () => { void fetchOpportunityIdeas(); });
  nodes.velocity_threshold.addEventListener("change", () => { void fetchVelocity(); }); nodes.velocity_refresh.addEventListener("click", () => { void fetchVelocity(); });
  document.querySelectorAll(".timeframe-button[data-seconds]").forEach((button) => button.addEventListener("click", () => setTimeframe(Number(button.dataset.seconds))));
  nodes.custom_timeframe_toggle.addEventListener("click", () => { const open = nodes.custom_timeframe.hidden; nodes.custom_timeframe.hidden = !open; nodes.custom_timeframe_toggle.setAttribute("aria-expanded", String(open)); if (open) nodes.custom_timeframe_value.focus(); });
  nodes.custom_timeframe_apply.addEventListener("click", () => { const value = Number(nodes.custom_timeframe_value.value); const seconds = nodes.custom_timeframe_unit.value === "minutes" ? value * 60 : value; setTimeframe(seconds, true); });
  nodes.chart_zoom_out.addEventListener("click", () => zoomChart(1.35)); nodes.chart_zoom_in.addEventListener("click", () => zoomChart(0.72)); nodes.chart_left.addEventListener("click", () => scrollChart(-1)); nodes.chart_right.addEventListener("click", () => scrollChart(1)); nodes.chart_reset.addEventListener("click", () => { if (state.tokenChart) state.tokenChart.timeScale().fitContent(); });
  nodes.research_search.addEventListener("input", (event) => { state.query = event.target.value; renderResearch(); }); nodes.family_filter.addEventListener("change", (event) => { state.family = event.target.value; renderResearch(); }); nodes.fold_filter.addEventListener("change", (event) => { state.fold = event.target.value; renderResearch(); }); nodes.status_filter.addEventListener("change", (event) => { state.status = event.target.value; renderResearch(); }); nodes.clear_filters.addEventListener("click", () => { state.query = ""; state.family = "all"; state.fold = "all"; state.status = "all"; nodes.research_search.value = ""; nodes.status_filter.value = "all"; renderResearch(); }); nodes.log_level.addEventListener("change", (event) => { state.logLevel = event.target.value; if (state.snapshot) renderLogs(state.snapshot); }); nodes.copy_logs.addEventListener("click", async () => { const content = state.events.map((item) => `[${formatTime(item.time)}] ${item.level}: ${item.message}`).join("\n"); try { await navigator.clipboard.writeText(content); nodes.copy_logs.textContent = "Copied"; window.setTimeout(() => { nodes.copy_logs.textContent = "Copy log"; }, 1200); } catch { appendLocalLog("warn", "Clipboard permission unavailable"); } });
  nodes.trade_prev.addEventListener("click", () => { const row = state.rows.find((item) => item.key === state.selectedKey); if (row) void loadTradePage(row, Math.max(0, state.tradeOffset - state.tradePageSize)); }); nodes.trade_next.addEventListener("click", () => { const row = state.rows.find((item) => item.key === state.selectedKey); if (row) void loadTradePage(row, state.tradeOffset + state.tradePageSize); });
  document.querySelectorAll(".sort-button").forEach((button) => button.addEventListener("click", () => { const key = button.dataset.sort; if (state.sort === key) state.direction = state.direction === "asc" ? "desc" : "asc"; else { state.sort = key; state.direction = "desc"; } renderResearch(); }));
  document.querySelectorAll(".report-tab").forEach((button) => button.addEventListener("click", () => { state.reportView = button.dataset.report; document.querySelectorAll(".report-tab").forEach((item) => { const active = item === button; item.classList.toggle("active", active); item.setAttribute("aria-selected", String(active)); }); document.querySelectorAll(".report-view").forEach((view) => { view.hidden = view.dataset.reportView !== state.reportView; }); }));
  window.setInterval(fetchSnapshot, 2500); window.setInterval(fetchHealth, 10000); window.setInterval(fetchFx, 10000); window.setInterval(() => { updateAge(); updateCurrencyControls(); }, 1000); window.addEventListener("resize", () => { if (!state.snapshot) return; renderWallet(state.snapshot); });
  updateCurrencyControls(); fetchSnapshot(); fetchHealth(); fetchFx(); fetchAtlas(); fetchSignals(); fetchOpportunityIdeas(); fetchVelocity(); connectEvents();
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init, { once: true }); else init();
