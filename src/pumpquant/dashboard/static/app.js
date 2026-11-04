// PumpQuant overview: one question, answered from the run's own data.
//
// The page reads three read-only endpoints served by live_dashboard.py:
//   /api/snapshot           run status and one candidate per test period
//   /api/candidate-trades   the closed trades of one test period
//   /api/trade-chart        price bars around one trade, for the replay
// Nothing here writes, trades or contacts a market.

import { $, $$, chartPercent, chartTime, initShell, reduceMotion, resolveWords, revealOnScroll } from "./shell.js";

const API = { snapshot: "/api/snapshot", trades: "/api/candidate-trades", chart: "/api/trade-chart" };

/* =========================================================================
 * Format: plain, precise, with a true minus sign
 * ========================================================================= */

const MINUS = "−";
const fixed = (value, digits) => Math.abs(value).toLocaleString("en", { minimumFractionDigits: digits, maximumFractionDigits: digits });
const signed = (value, digits) => `${value < 0 ? MINUS : value > 0 ? "+" : ""}${fixed(value, digits)}`;
const sol = (value, digits = 3) => `${signed(value, digits)} SOL`;
const pct = (fraction, digits = 1) => `${signed(fraction * 100, digits)}%`;
const share = (fraction) => `${Math.round(fraction * 100)}%`;
const seconds = (ns) => Math.max(0, Math.round(ns / 1e9));
const tokenName = (mint) => `Token ${String(mint).replace(/^SYNTH-0*/, "#")}`;
const EXIT_WORDS = { stop_loss: "stop-loss", take_profit: "take-profit", trailing_stop: "trailing stop", max_holding: "time limit", momentum_reversal: "momentum reversal" };
const exitWords = (reason) => EXIT_WORDS[reason] ?? String(reason).replace(/_/g, " ");
const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;

/* =========================================================================
 * Data
 * ========================================================================= */

async function getJson(url) {
  const response = await fetch(url, { cache: "no-store", headers: { Accept: "application/json" } });
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return response.json();
}

const state = { snapshot: null, periods: [], selected: 0, trade: 0, chart: null };

async function load() {
  const snapshot = await getJson(API.snapshot);
  const candidates = snapshot.candidates ?? snapshot.strategy?.candidates ?? [];
  const lists = await Promise.all(candidates.map((c) => getJson(`${API.trades}?${new URLSearchParams({ candidate: c.id })}`).catch(() => ({ trades: [] }))));
  state.snapshot = snapshot;
  state.periods = candidates.map((c, i) => ({ ...c, closed: lists[i].trades ?? [] }));
}

/* =========================================================================
 * Render: the answer
 * ========================================================================= */

function renderAnswer() {
  const { snapshot, periods } = state;
  const profitable = periods.filter((p) => p.net_pnl > 0).length;
  const total = periods.reduce((sum, p) => sum + p.net_pnl, 0);
  const outcomes = periods.reduce((sum, p) => sum + p.trades, 0);
  const stake = periods.flatMap((p) => p.closed).find((t) => t.quote_in)?.quote_in;

  $("#question").textContent = snapshot.experiment_id ?? $("#question").textContent;
  const chip = $("#verdict-chip");
  if (profitable === periods.length) { chip.textContent = "Held up"; chip.className = "verdict-chip good"; }
  else if (profitable === 0) { chip.textContent = "Did not hold up"; chip.className = "verdict-chip bad"; }
  else { chip.textContent = "Mixed"; chip.className = "verdict-chip"; }
  $("#verdict-text").textContent = profitable === periods.length
    ? `Profitable in all ${periods.length} test periods.`
    : `Profitable in ${profitable} of ${periods.length} test periods.`;
  $("#lede").textContent = snapshot.progress?.message ?? "";

  const stats = [
    ["Combined result", sol(total), "Across all test periods, after fees", total],
    ["Periods profitable", `${profitable} of ${periods.length}`, "Each tested on later, unseen data"],
    ["Outcomes counted", String(outcomes), "Closed trades, open positions and failed orders"],
    ["Stake per trade", stake ? `${fixed(stake, 1)} SOL` : "—", "The same fixed amount every time"],
  ];
  $("#stats").innerHTML = stats.map(([label, value, note, tone]) => `
    <div class="card stat"><small>${label}</small><strong class="${tone > 0 ? "up-text" : tone < 0 ? "down-text" : ""}">${value}</strong><span>${note}</span></div>`).join("");

  const status = $("#status");
  status.classList.toggle("ready", snapshot.progress?.run_status === "complete");
  $("#status-label").textContent = snapshot.progress?.run_status === "complete" ? "Demo run complete" : "Run in progress";
}

/* =========================================================================
 * Render: test periods
 * ========================================================================= */

function renderPeriods() {
  const max = Math.max(1e-9, ...state.periods.flatMap((p) => p.closed.map((t) => Math.abs(t.net_pnl_quote))));
  $("#period-cards").innerHTML = state.periods.map((p, i) => {
    const up = p.net_pnl > 0;
    const bars = p.closed.map((t, k) => `<i class="${t.net_pnl_quote > 0 ? "up" : "down"}" style="--i:${k};height:${Math.max(4, (Math.abs(t.net_pnl_quote) / max) * 42).toFixed(1)}px"></i>`).join("");
    return `
      <button class="card period" type="button" role="radio" aria-checked="${i === state.selected}" data-period="${i}">
        <span class="period-top"><strong>${p.name}</strong><span class="badge ${up ? "up" : "down"}">${up ? "Profitable" : "Lost money"}</span></span>
        <span class="period-result ${up ? "up-text" : "down-text"}">${sol(p.net_pnl)}<small>${pct(p.return_pct)}</small></span>
        <span><span class="bars">${bars}</span><span class="bars-axis" aria-hidden="true"></span><small>Each bar is one closed trade</small></span>
        <span class="period-facts">
          <span><small>Win rate</small><b>${share(p.win_rate)}</b></span>
          <span><small>Outcomes</small><b>${p.trades}</b></span>
          <span><small>Profit factor</small><b>${fixed(p.profit_factor, 2)}</b></span>
        </span>
      </button>`;
  }).join("");
  $$(".period").forEach((card) => card.addEventListener("click", () => selectPeriod(Number(card.dataset.period), { scroll: true })));
}

/* =========================================================================
 * Render: the method diagram
 * ========================================================================= */

// Fold layout of the demo run (see src/pumpquant/demo.py):
// 36 tokens in launch order; each period learns on every earlier token
// (18, then 24, then 30) and tests on the next 6 (validation/splits.py: embargoed_walk_forward).
const DEMO_FOLDS = { tokens: 36, firstTrain: 18, test: 6 };

function renderTimeline() {
  const svg = $("#timeline");
  const { tokens, firstTrain, test } = DEMO_FOLDS;
  const n = state.periods.length || 3;
  const x0 = 112, width = 520, row = 58, unit = width / tokens;
  let out = "";
  for (let r = 0; r < n; r++) {
    const y = 14 + r * row, train = firstTrain + r * test;
    const seg = (cls, from, count, label, i) => {
      out += `<rect class="${cls} seg" x="${(x0 + from * unit + 2).toFixed(1)}" y="${y}" width="${(count * unit - 4).toFixed(1)}" height="40" rx="12" style="--i:${i}"/>`;
      if (label) out += `<text x="${(x0 + (from + count / 2) * unit).toFixed(1)}" y="${y + 26}" text-anchor="middle"${cls === "test" ? ' style="fill:#00131f;font-weight:650"' : ""}>${label}</text>`;
    };
    out += `<text class="label" x="0" y="${y + 26}">Period ${r + 1}</text>`;
    seg("learn", 0, train, `Learn on ${train}`, r * 3);
    seg("test", train, test, "Test", r * 3 + 1);
    if (train + test < tokens) seg("unused", train + test, tokens - train - test, "", r * 3 + 2);
  }
  out += `<text x="${x0}" y="${14 + n * row + 20}">Token 1</text><text x="${x0 + width}" y="${14 + n * row + 20}" text-anchor="end">Token ${tokens} →</text>`;
  svg.setAttribute("viewBox", `0 0 640 ${14 + n * row + 30}`);
  svg.setAttribute("aria-label", `Diagram drawn to scale: ${tokens} tokens in launch order. Period 1 learns on tokens 1 to ${firstTrain} and tests on the next ${test}; each later period learns on all earlier tokens and tests on the next ${test}.`);
  svg.innerHTML = out;
}

/* =========================================================================
 * Render: trades and the replay chart
 * ========================================================================= */

function selectPeriod(index, { scroll = false } = {}) {
  state.selected = index;
  state.trade = 0;
  $$(".period").forEach((card) => card.setAttribute("aria-checked", String(Number(card.dataset.period) === index)));
  renderTrades();
  if (scroll) $("#trades").scrollIntoView({ behavior: reduceMotion ? "auto" : "smooth" });
}

function renderTrades() {
  const period = state.periods[state.selected];
  if (!period) return;
  const others = period.trades - period.closed.length;
  $("#trades-eyebrow").textContent = `Trades · ${period.name}`;
  $("#trades-summary").textContent = `${plural(period.closed.length, "closed trade")}.` + (others > 0 ? ` The other ${plural(others, "outcome")} in this period ${others === 1 ? "is a position" : "are positions"} still open at its end or failed orders, which still cost fees.` : "");
  $("#trade-list").innerHTML = period.closed.map((t, i) => `
    <button class="trade" type="button" role="option" aria-selected="${i === state.trade}" data-trade="${i}">
      <span class="n">${i + 1}</span>
      <strong>${tokenName(t.mint)}</strong>
      <span class="pnl ${t.net_pnl_quote > 0 ? "up-text" : "down-text"}">${sol(t.net_pnl_quote)}</span>
      <small>${exitWords(t.exit_reason)} · held ${seconds(t.exit_time_ns - t.entry_time_ns)} s</small>
    </button>`).join("") || `<p class="story">No closed trades in this period.</p>`;
  $$(".trade").forEach((row) => row.addEventListener("click", () => selectTrade(Number(row.dataset.trade))));
  selectTrade(0);
}

function css(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }

function ensureChart() {
  if (state.chart || !window.LightweightCharts) return state.chart;
  const lib = window.LightweightCharts;
  const chart = lib.createChart($("#chart"), {
    autoSize: true,
    layout: { background: { type: "solid", color: "rgba(0,0,0,0)" }, textColor: css("--text-3"), fontFamily: "Mona Sans, system-ui, sans-serif", fontSize: 11 },
    grid: { vertLines: { visible: false }, horzLines: { color: css("--line") } },
    rightPriceScale: { borderVisible: false, mode: lib.PriceScaleMode?.Percentage ?? 2 },
    timeScale: { borderVisible: false, timeVisible: true, secondsVisible: true, tickMarkFormatter: chartTime },
    localization: { timeFormatter: chartTime, percentageFormatter: chartPercent },
    crosshair: { vertLine: { color: css("--line"), style: 2, labelBackgroundColor: css("--card-hi") }, horzLine: { color: css("--line"), style: 2, labelBackgroundColor: css("--card-hi") } },
    handleScroll: true, handleScale: true,
  });
  const candles = chart.addSeries(lib.CandlestickSeries, { upColor: css("--green"), downColor: css("--red"), borderVisible: false, wickUpColor: css("--green"), wickDownColor: css("--red"), priceLineVisible: false, lastValueVisible: false });
  state.chart = { chart, candles, markers: lib.createSeriesMarkers(candles, []) };
  return state.chart;
}

async function selectTrade(index) {
  const period = state.periods[state.selected];
  const trade = period?.closed[index];
  state.trade = index;
  $$(".trade").forEach((row) => row.setAttribute("aria-selected", String(Number(row.dataset.trade) === index)));
  if (!trade) return;
  $("#chart-title").textContent = `${tokenName(trade.mint)} · trade ${index + 1}`;
  const result = $("#chart-result");
  result.textContent = sol(trade.net_pnl_quote);
  result.className = `result ${trade.net_pnl_quote > 0 ? "up-text" : "down-text"}`;
  const move = trade.exit_price / trade.entry_price - 1;
  const wait = seconds(trade.entry_time_ns - trade.signal_time_ns);
  const held = seconds(trade.exit_time_ns - trade.entry_time_ns);
  $("#chart-story").textContent = `Bought ${wait ? `${plural(wait, "second")} after the signal` : "at the signal"}. The price ${move >= 0 ? "rose" : "fell"} ${fixed(move * 100, 0)}%, and the ${exitWords(trade.exit_reason).replace(/ /g, "-")} exit sold it ${plural(held, "second")} later. Result after fees: ${sol(trade.net_pnl_quote)}.`;

  const view = ensureChart();
  if (!view) return;
  try {
    const payload = await getJson(`${API.chart}?${new URLSearchParams({ candidate: period.id, trade_id: String(trade.trade_id) })}`);
    if (state.periods[state.selected] !== period || state.trade !== index) return; // a newer selection won
    // Demo bars carry a synthetic clock, so they are drawn from the token's launch (its first bar).
    const bars = payload.bars ?? [];
    const origin = payload.synthetic && bars.length ? bars[0].time : 0;
    view.candles.setData(bars.map((b) => ({ time: b.time - origin, open: b.open, high: b.high, low: b.low, close: b.close })));
    const at = (ns) => Math.floor(ns / 1e9) - origin;
    view.markers.setMarkers([
      { time: at(trade.signal_time_ns), position: "aboveBar", color: css("--amber"), shape: "circle", text: "Signal" },
      { time: at(trade.entry_time_ns), position: "belowBar", color: css("--green"), shape: "arrowUp", text: "Buy" },
      { time: at(trade.exit_time_ns), position: "aboveBar", color: css("--red"), shape: "arrowDown", text: "Sell" },
    ].sort((a, b) => a.time - b.time));
    view.chart.timeScale().fitContent();
    requestAnimationFrame(() => view.chart.timeScale().fitContent());
  } catch (error) {
    $("#chart-story").textContent = `The price history for this trade could not be loaded (${error.message}).`;
  }
}

/* =========================================================================
 * Motion specific to this page
 * ========================================================================= */

function motion() {
  resolveWords($("#question"));
  revealOnScroll(".verdict, .lede, .stat, .section-head, .period, .method, .trade-list, .chart-card, .limits .card, .next-card", (el) => {
    $(".bars", el)?.classList.add("in");
    if (el.matches(".method")) drawTimeline();
  });
}

function drawTimeline() {
  $$("#timeline .seg").forEach((seg) => seg.animate(
    [{ transform: "scaleX(0)", opacity: 0 }, { transform: "scaleX(1)", opacity: 1 }],
    { duration: 650, delay: Number(seg.style.getPropertyValue("--i")) * 70, easing: "cubic-bezier(.22,1.18,.36,1)", fill: "backwards", transformOrigin: "left" },
  ));
  $$("#timeline .seg").forEach((seg) => { seg.style.transformBox = "fill-box"; seg.style.transformOrigin = "left center"; });
}

/* =========================================================================
 * Start
 * ========================================================================= */

initShell({ onThemeChange: () => { if (state.chart) { state.chart.chart.remove(); state.chart = null; selectTrade(state.trade); } } });
try {
  await load();
  renderAnswer();
  renderPeriods();
  renderTimeline();
  selectPeriod(0);
} catch (error) {
  $("#verdict-text").textContent = `The run could not be loaded (${error.message}). Start it with: pumpquant preview`;
}
motion();
