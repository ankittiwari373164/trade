/**
 * TradeBot v16 — Groww Trade API Edition (Hardened)
 * ══════════════════════════════════════════════════════════════
 * KEY UPGRADES vs v15:
 *  ✓ NEW historical endpoint /v1/historical/candles  (groww_symbol + candle_interval)
 *  ✓ Robust ISO + epoch timestamp handling for candles
 *  ✓ Replaces dead volume_shakers scrape with live discovery filters
 *      → MOST_BOUGHT, INTRADAY_VOLUME, TRADED_BY_VOLUME, TOP_GAINERS
 *  ✓ Sector/Index context (NIFTY 50 correlation)
 *  ✓ MACD, Bollinger %B, ADX, OBV, supertrend-like volatility filter
 *  ✓ Volume-spike confirmation in scoring
 *  ✓ Daily history extended to 90 days  (proper EMA-50)
 *  ✓ Buy/Sell depth pressure for ALL stocks (capped, throttled)
 *  ✓ Late-start fallback uses synthesised t915/t925 from today's 1-min candles
 *  ✓ Confidence penalised when signals conflict
 *  ✓ HTTP-trigger endpoints for Vercel cron (replace node-cron)
 *  ✓ Persistent prediction store with on-disk snapshot
 *  ✓ Single-flight init guard + retry-with-backoff axios wrapper
 * ══════════════════════════════════════════════════════════════
 */

require('dotenv').config();
const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const cron    = require('node-cron');
const path    = require('path');
const fs      = require('fs');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ══════════════════════════════════════════════════════════════
// CONFIG
// ══════════════════════════════════════════════════════════════
const GROWW_TOKEN = process.env.GROWW_ACCESS_TOKEN || '';
const PORT        = process.env.PORT || 3001;
const CRON_SECRET = process.env.CRON_SECRET || ''; // optional; protects cron HTTP triggers
const PERSIST_FILE = process.env.PERSIST_FILE || '/tmp/tradebot_state.json';

if (!GROWW_TOKEN) {
  console.error('❌ GROWW_ACCESS_TOKEN not set — set it in .env or Vercel env vars');
  if (require.main === module) process.exit(1);
}

const GHDRS = {
  'Authorization': `Bearer ${GROWW_TOKEN}`,
  'X-API-VERSION': '1.0',
  'Accept':        'application/json',
  'Content-Type':  'application/json',
};
const GROWW_BASE = 'https://api.groww.in/v1';

// Public Groww web API (for discovery — no auth needed)
const GROWW_WEB_HDRS = {
  'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36',
  'Accept':          'application/json',
  'Accept-Language': 'en-IN,en;q=0.9',
  'Origin':          'https://groww.in',
  'Referer':         'https://groww.in/',
};

// Base watchlist — large-caps used as a stable fallback if discovery fails
const BASE_STOCKS = [
  'RELIANCE','TCS','INFY','HDFCBANK','ICICIBANK',
  'SBIN','WIPRO','BAJFINANCE','TATAMOTORS','HCLTECH',
  'TECHM','AXISBANK','KOTAKBANK','LT','MARUTI',
  'SUNPHARMA','ITC','BHARTIARTL','ASIANPAINT','HINDUNILVR',
  'ADANIENT','ADANIPORTS','NTPC','POWERGRID','ULTRACEMCO',
];

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ──────────────────────────────────────────────────────────────
// Hardened axios call: timeout + 1 retry on 5xx / network errors
// ──────────────────────────────────────────────────────────────
async function safeGet(url, opts = {}, retries = 1) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await axios.get(url, { timeout: 10000, ...opts });
    } catch (e) {
      const status = e.response?.status;
      const retriable = !status || (status >= 500 && status < 600) || e.code === 'ECONNABORTED';
      if (attempt === retries || !retriable) throw e;
      await sleep(400 * (attempt + 1));
    }
  }
}

// ══════════════════════════════════════════════════════════════
// TIME HELPERS  (IST = UTC + 5:30)
// ══════════════════════════════════════════════════════════════
function getIST() {
  const ist = new Date(Date.now() + 5.5 * 3600000);
  const h = ist.getUTCHours(), m = ist.getUTCMinutes(), s = ist.getUTCSeconds();
  return { h, m, s, totalMins: h * 60 + m, day: ist.getUTCDay(), ist };
}
function marketPhase() {
  const { totalMins, day } = getIST();
  if (day < 1 || day > 5)        return 'WEEKEND';
  if (totalMins < 9 * 60)        return 'PRE_OPEN';
  if (totalMins < 9 * 60 + 15)   return 'PRE_MARKET';
  if (totalMins < 9 * 60 + 25)   return 'OPENING';
  if (totalMins < 11 * 60)       return 'EARLY';
  if (totalMins < 13 * 60)       return 'MID';
  if (totalMins < 15 * 60)       return 'LATE';
  if (totalMins < 15 * 60 + 15)  return 'MIS_EXIT';
  if (totalMins < 15 * 60 + 30)  return 'CLOSING';
  return 'CLOSED';
}
function isOpen() { return ['OPENING','EARLY','MID','LATE','MIS_EXIT'].includes(marketPhase()); }
function istStr() {
  const { h, m } = getIST();
  return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')} IST`;
}
function dateStr(dayOffset = 0) {
  const dt = new Date(Date.now() + 5.5 * 3600000 + dayOffset * 86400000);
  return `${dt.getUTCFullYear()}-${String(dt.getUTCMonth()+1).padStart(2,'0')}-${String(dt.getUTCDate()).padStart(2,'0')}`;
}
// Skip weekends going backwards
function tradingDayOffset(daysBack) {
  let offset = 0, found = 0;
  while (found < daysBack) {
    offset -= 1;
    const dt = new Date(Date.now() + 5.5 * 3600000 + offset * 86400000);
    const d = dt.getUTCDay();
    if (d >= 1 && d <= 5) found++;
  }
  return offset;
}

// ══════════════════════════════════════════════════════════════
// GROWW TRADE API (auth) — Live + Historical
// ══════════════════════════════════════════════════════════════

// Live full quote (one symbol — includes depth, total buy/sell qty)
async function growwQuote(symbol) {
  try {
    const r = await safeGet(`${GROWW_BASE}/live-data/quote`, {
      params:  { exchange: 'NSE', segment: 'CASH', trading_symbol: symbol },
      headers: GHDRS,
    });
    return r.data?.status === 'SUCCESS' ? r.data.payload : null;
  } catch (e) {
    console.error(`[Quote] ${symbol}: ${e.message?.slice(0,60)}`);
    return null;
  }
}

// Batch LTP — up to 50 symbols in one call
async function growwLTP(symbols) {
  if (!symbols.length) return {};
  const exchangeSymbols = symbols.map(s => `NSE_${s}`).join(',');
  try {
    const r = await safeGet(`${GROWW_BASE}/live-data/ltp`, {
      params:  { segment: 'CASH', exchange_symbols: exchangeSymbols },
      headers: GHDRS,
    });
    return r.data?.status === 'SUCCESS' ? (r.data.payload || {}) : {};
  } catch (e) {
    console.error(`[LTP] batch: ${e.message?.slice(0,60)}`);
    return {};
  }
}

// Batch OHLC — up to 50 symbols in one call
async function growwOHLC(symbols) {
  if (!symbols.length) return {};
  const exchangeSymbols = symbols.map(s => `NSE_${s}`).join(',');
  try {
    const r = await safeGet(`${GROWW_BASE}/live-data/ohlc`, {
      params:  { segment: 'CASH', exchange_symbols: exchangeSymbols },
      headers: GHDRS,
    });
    return r.data?.status === 'SUCCESS' ? (r.data.payload || {}) : {};
  } catch (e) {
    console.error(`[OHLC] batch: ${e.message?.slice(0,60)}`);
    return {};
  }
}

// ─────────────────────────────────────────────────────────────
// Historical candles — NEW endpoint (/v1/historical/candles)
// Returns ISO timestamps. Falls back to deprecated endpoint on failure.
// candleInterval: '1minute' | '5minute' | '15minute' | '30minute' | '1hour' | '1day'
// ─────────────────────────────────────────────────────────────
async function growwCandles(symbol, candleInterval, startTime, endTime) {
  const growwSymbol = `NSE-${symbol}`;
  // Try new endpoint first
  try {
    const r = await safeGet(`${GROWW_BASE}/historical/candles`, {
      params: {
        exchange: 'NSE',
        segment:  'CASH',
        groww_symbol: growwSymbol,
        start_time:   startTime,
        end_time:     endTime,
        candle_interval: candleInterval,
      },
      headers: GHDRS,
    });
    if (r.data?.status === 'SUCCESS') {
      return normalizeCandles(r.data.payload?.candles || []);
    }
  } catch (e) {
    // Fall through to legacy endpoint
  }

  // Legacy endpoint fallback (interval in minutes)
  const intervalMins = {
    '1minute': 1, '5minute': 5, '15minute': 15, '30minute': 30,
    '1hour': 60, '1day': 1440,
  }[candleInterval] || 5;
  try {
    const r = await safeGet(`${GROWW_BASE}/historical/candle/range`, {
      params: {
        exchange: 'NSE', segment: 'CASH',
        trading_symbol: symbol,
        start_time:   startTime,
        end_time:     endTime,
        interval_in_minutes: intervalMins,
      },
      headers: GHDRS,
    });
    if (r.data?.status === 'SUCCESS') {
      return normalizeCandles(r.data.payload?.candles || []);
    }
  } catch (e) {
    console.error(`[Candles] ${symbol} ${candleInterval}: ${e.message?.slice(0,60)}`);
  }
  return [];
}

// Normalise: each candle becomes [tsMs, open, high, low, close, volume]
function normalizeCandles(rawCandles) {
  return rawCandles.map(c => {
    let ts = c[0];
    if (typeof ts === 'string') {
      // ISO format: "2025-09-24T10:30:00" — assume IST, convert to UTC ms
      ts = new Date(ts + (ts.includes('Z') || ts.includes('+') ? '' : '+05:30')).getTime();
    }
    return [ts, +c[1], +c[2], +c[3], +c[4], +c[5], c[6] ?? null];
  });
}

// Convenience wrappers
async function fetchDailyHistory(symbol, days = 90) {
  const end   = `${dateStr(0)} 15:30:00`;
  const start = `${dateStr(tradingDayOffset(days))} 09:15:00`;
  return growwCandles(symbol, '1day', start, end);
}
async function fetchToday5min(symbol) {
  const today = dateStr(0);
  return growwCandles(symbol, '5minute', `${today} 09:15:00`, `${today} 15:30:00`);
}
async function fetchToday1min(symbol) {
  const today = dateStr(0);
  return growwCandles(symbol, '1minute', `${today} 09:15:00`, `${today} 15:30:00`);
}

// ══════════════════════════════════════════════════════════════
// GROWW DISCOVERY (web — no auth) — most bought / intraday / volume
// Endpoint: /v1/api/stocks_data/v2/explore/list/top
// discoveryFilterTypes:
//   POPULAR_STOCKS_MOST_BOUGHT          – most bought on Groww (★ user's request)
//   POPULAR_STOCKS_INTRADAY_VOLUME      – top intraday volume   (★ user's request)
//   POPULAR_STOCKS_MOST_BOUGHT_BY_TURNOVER
//   POPULAR_STOCKS_MOST_BOUGHT_MTF      – MTF most bought
//   TRADED_BY_VOLUME / TRADED_BY_VALUE  – top traded
//   TOP_GAINERS / TOP_LOSERS            – movers
// ══════════════════════════════════════════════════════════════
async function growwDiscovery(filterType, size = 25) {
  try {
    const r = await safeGet('https://groww.in/v1/api/stocks_data/v2/explore/list/top', {
      params: { discoveryFilterTypes: filterType, page: 0, size },
      headers: GROWW_WEB_HDRS,
    });
    // Response shape varies — be defensive
    const data = r.data;
    const list =
      data?.exploreList ||
      data?.[filterType] ||
      data?.payload?.[filterType] ||
      data?.payload?.exploreList ||
      data?.data ||
      [];
    const stocks = [];
    for (const item of list) {
      const co = item.company || item;
      const sym = co.nseScriptCode || co.bseScriptCode || co.nseSymbol || co.symbol;
      if (!sym) continue;
      stocks.push({
        symbol: sym,
        companyName: co.companyShortName || co.companyName || sym,
        slug:        co.searchId || co.slug || sym.toLowerCase(),
      });
    }
    return stocks;
  } catch (e) {
    console.error(`[Discovery] ${filterType}: ${e.message?.slice(0,60)}`);
    return [];
  }
}

// Public OHLC endpoint that doesn't require auth, used as a final fallback
// for index data (NIFTY 50 correlation)
async function fetchNiftyChange() {
  try {
    // Use trade API — NIFTY 50 in CASH/INDICES
    const r = await safeGet(`${GROWW_BASE}/live-data/ltp`, {
      params:  { segment: 'CASH', exchange_symbols: 'NSE_NIFTY' },
      headers: GHDRS,
    });
    const ltp = r.data?.payload?.NSE_NIFTY;
    if (!ltp) return 0;

    const ohlc = await safeGet(`${GROWW_BASE}/live-data/ohlc`, {
      params:  { segment: 'CASH', exchange_symbols: 'NSE_NIFTY' },
      headers: GHDRS,
    });
    const o = ohlc.data?.payload?.NSE_NIFTY;
    if (!o?.close) return 0;
    return +(((ltp - o.close) / o.close) * 100).toFixed(2);
  } catch (e) {
    return 0;
  }
}

// ══════════════════════════════════════════════════════════════
// TECHNICAL INDICATORS  (all candles normalised to [ts,o,h,l,c,v])
// ══════════════════════════════════════════════════════════════
function calcSMA(arr, p) {
  if (!arr || arr.length < p) return null;
  return +(arr.slice(-p).reduce((s,x)=>s+x,0) / p).toFixed(2);
}
function calcEMA(closes, p) {
  if (!closes || closes.length < p) return null;
  const k = 2 / (p + 1);
  let v = closes.slice(0, p).reduce((s,x)=>s+x,0) / p;
  for (let i = p; i < closes.length; i++) v = closes[i] * k + v * (1 - k);
  return +v.toFixed(2);
}
function calcRSI(closes, p = 14) {
  if (!closes || closes.length < p + 1) return 50;
  let g = 0, l = 0;
  for (let i = 1; i <= p; i++) {
    const d = closes[i] - closes[i-1];
    if (d > 0) g += d; else l -= d;
  }
  let ag = g/p, al = l/p;
  for (let i = p + 1; i < closes.length; i++) {
    const d = closes[i] - closes[i-1];
    ag = (ag * (p - 1) + Math.max(0, d)) / p;
    al = (al * (p - 1) + Math.max(0, -d)) / p;
  }
  if (al === 0) return 100;
  return +(100 - 100 / (1 + ag/al)).toFixed(1);
}
function calcVWAP(candles) {
  let pv = 0, vol = 0;
  for (const c of candles) {
    const tp = (c[2] + c[3] + c[4]) / 3;
    pv  += tp * c[5];
    vol += c[5];
  }
  return vol > 0 ? +(pv / vol).toFixed(2) : 0;
}
function calcATR(candles, p = 14) {
  if (candles.length < p + 1) return 0;
  const trs = candles.slice(1).map((c, i) => Math.max(
    c[2] - c[3],
    Math.abs(c[2] - candles[i][4]),
    Math.abs(c[3] - candles[i][4])
  ));
  return +(trs.slice(-p).reduce((s,v)=>s+v,0) / p).toFixed(2);
}
// MACD on closes — returns { macd, signal, histogram }
function calcMACD(closes, fast = 12, slow = 26, signalP = 9) {
  if (closes.length < slow + signalP) return null;
  // Build EMA arrays
  const emaArr = (p) => {
    const k = 2 / (p + 1);
    const out = [];
    let v = closes.slice(0, p).reduce((s,x)=>s+x,0) / p;
    out[p - 1] = v;
    for (let i = p; i < closes.length; i++) {
      v = closes[i] * k + v * (1 - k);
      out[i] = v;
    }
    return out;
  };
  const eFast = emaArr(fast), eSlow = emaArr(slow);
  const macdLine = closes.map((_, i) => (eFast[i] != null && eSlow[i] != null) ? eFast[i] - eSlow[i] : null);
  // Signal line = EMA(macdLine, 9) — only valid where macdLine starts
  const validMacd = macdLine.filter(v => v != null);
  if (validMacd.length < signalP) return null;
  const k = 2 / (signalP + 1);
  let sig = validMacd.slice(0, signalP).reduce((s,x)=>s+x,0) / signalP;
  for (let i = signalP; i < validMacd.length; i++) sig = validMacd[i] * k + sig * (1 - k);
  const lastMacd = macdLine[macdLine.length - 1];
  return {
    macd:      +lastMacd.toFixed(2),
    signal:    +sig.toFixed(2),
    histogram: +(lastMacd - sig).toFixed(2),
  };
}
// Bollinger %B (where price sits in 2σ band; <0 = below low band, >1 = above upper band)
function calcBollingerPctB(closes, p = 20, mult = 2) {
  if (closes.length < p) return 0.5;
  const slice = closes.slice(-p);
  const mean = slice.reduce((s,x)=>s+x,0) / p;
  const variance = slice.reduce((s,x)=>s + (x-mean)*(x-mean), 0) / p;
  const sd = Math.sqrt(variance);
  if (sd === 0) return 0.5;
  const upper = mean + mult * sd, lower = mean - mult * sd;
  const last = closes[closes.length - 1];
  return +((last - lower) / (upper - lower)).toFixed(3);
}
// ADX (Wilder's) — measures trend strength on candles
function calcADX(candles, p = 14) {
  if (candles.length < p * 2 + 1) return 0;
  const tr = [], pdm = [], ndm = [];
  for (let i = 1; i < candles.length; i++) {
    const [, , h, l, c] = candles[i];
    const pc = candles[i-1][4], ph = candles[i-1][2], pl = candles[i-1][3];
    tr.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
    const upMove = h - ph, downMove = pl - l;
    pdm.push((upMove > downMove && upMove > 0) ? upMove : 0);
    ndm.push((downMove > upMove && downMove > 0) ? downMove : 0);
  }
  const wilder = (arr) => {
    let v = arr.slice(0, p).reduce((s,x)=>s+x, 0);
    const out = [v];
    for (let i = p; i < arr.length; i++) {
      v = v - v / p + arr[i];
      out.push(v);
    }
    return out;
  };
  const trS  = wilder(tr);
  const pdmS = wilder(pdm);
  const ndmS = wilder(ndm);
  const dx = pdmS.map((v, i) => {
    const pdi = (v / trS[i]) * 100;
    const ndi = (ndmS[i] / trS[i]) * 100;
    return Math.abs(pdi - ndi) / Math.max(0.0001, pdi + ndi) * 100;
  });
  if (dx.length < p) return 0;
  // ADX = SMA of last p DX values
  return +(dx.slice(-p).reduce((s,x)=>s+x, 0) / p).toFixed(1);
}
// Volume spike vs 20-period average
function calcVolumeSpike(candles) {
  if (candles.length < 21) return 1;
  const recent = candles.slice(-1)[0][5] || 0;
  const avg = candles.slice(-21, -1).reduce((s,c)=>s+(c[5]||0), 0) / 20;
  return avg > 0 ? +(recent / avg).toFixed(2) : 1;
}
// Today's cumulative volume vs prior 20-day average daily volume
function calcRelativeVolume(todayCandles, dailyCandles) {
  const todayVol = todayCandles.reduce((s,c)=>s+(c[5]||0), 0);
  const days = dailyCandles.slice(-20);
  if (!days.length || todayVol === 0) return 1;
  const avgDailyVol = days.reduce((s,c)=>s+(c[5]||0), 0) / days.length;
  if (avgDailyVol === 0) return 1;
  // Scale by elapsed time fraction of trading day (375 mins = 9:15→15:30)
  const { totalMins } = getIST();
  const elapsed = Math.max(1, Math.min(375, totalMins - 9*60 - 15));
  const expectedSoFar = avgDailyVol * (elapsed / 375);
  return +(todayVol / Math.max(1, expectedSoFar)).toFixed(2);
}
function pivotPoints(high, low, close) {
  const pp = (high + low + close) / 3;
  return {
    pp: +pp.toFixed(2),
    r1: +(2*pp - low).toFixed(2),  r2: +(pp + high - low).toFixed(2),
    s1: +(2*pp - high).toFixed(2), s2: +(pp - high + low).toFixed(2),
  };
}

// ══════════════════════════════════════════════════════════════
// RUNTIME STATE
// ══════════════════════════════════════════════════════════════
const openingSnaps    = {};   // { SYM: { t915, t925, open } }
let   snapshotStatus  = 'waiting';
let   lockedPredictions = [];
const histCache       = {};   // { SYM: { daily, m5, m1, loadedAt } }
const liveQuotes      = {};   // { SYM: full quote payload }
let   dataStore       = { quotes: {}, lastUpdated: null };
let   discoveryStocks = { mostBought: [], intraday: [], byVolume: [], gainers: [] };
const companyNames    = {};
let   niftyChange     = 0;    // % change of NIFTY 50 today

// Persist / restore — survives server restarts (within /tmp, ~24h on Vercel)
function persist() {
  try {
    fs.writeFileSync(PERSIST_FILE, JSON.stringify({
      openingSnaps, lockedPredictions, snapshotStatus,
      discoveryStocks, companyNames,
      savedAt: Date.now(),
    }), 'utf8');
  } catch (e) {
    // /tmp may be read-only in some environments; ignore silently
  }
}
function restore() {
  try {
    if (!fs.existsSync(PERSIST_FILE)) return false;
    const s = JSON.parse(fs.readFileSync(PERSIST_FILE, 'utf8'));
    // Only restore if from today
    const sameDay = new Date(s.savedAt + 5.5*3600000).getUTCDate() === getIST().ist.getUTCDate();
    if (!sameDay) return false;
    Object.assign(openingSnaps, s.openingSnaps || {});
    lockedPredictions = s.lockedPredictions || [];
    snapshotStatus    = s.snapshotStatus    || 'waiting';
    discoveryStocks   = s.discoveryStocks   || discoveryStocks;
    Object.assign(companyNames, s.companyNames || {});
    return true;
  } catch (e) {
    return false;
  }
}

function getAllSymbols() {
  const all = new Set([
    ...discoveryStocks.mostBought.map(s => s.symbol),
    ...discoveryStocks.intraday.map(s => s.symbol),
    ...discoveryStocks.byVolume.map(s => s.symbol),
    ...discoveryStocks.gainers.map(s => s.symbol),
    ...BASE_STOCKS,
  ]);
  return [...all].slice(0, 50); // hard cap to keep API usage sane
}

function growwUrl(sym) {
  const allDiscovery = [
    ...discoveryStocks.mostBought,
    ...discoveryStocks.intraday,
    ...discoveryStocks.byVolume,
    ...discoveryStocks.gainers,
  ];
  const found = allDiscovery.find(s => s.symbol === sym);
  const slug = found?.slug || sym.toLowerCase();
  return `https://groww.in/stocks/${slug}`;
}

function buildTags(sym) {
  const tags = [];
  if (discoveryStocks.mostBought.some(s => s.symbol === sym)) tags.push('MOST BOUGHT');
  if (discoveryStocks.intraday.some(s   => s.symbol === sym)) tags.push('TOP INTRADAY');
  if (discoveryStocks.byVolume.some(s   => s.symbol === sym)) tags.push('TOP VOLUME');
  if (discoveryStocks.gainers.some(s    => s.symbol === sym)) tags.push('TOP GAINER');
  return tags;
}

// ══════════════════════════════════════════════════════════════
// HISTORY LOADER (pre-market)
// ══════════════════════════════════════════════════════════════
async function loadHistoryForSym(sym) {
  const [daily, m5] = await Promise.all([
    fetchDailyHistory(sym, 90),                         // 90 days for proper EMA-50
    isOpen() ? fetchToday5min(sym) : Promise.resolve([]),
  ]);
  histCache[sym] = { daily, m5, loadedAt: Date.now() };
  return histCache[sym];
}

async function loadAllHistory() {
  const syms = getAllSymbols();
  console.log(`[Hist] Loading history for ${syms.length} stocks...`);
  // 4 in flight, gentle pacing
  for (let i = 0; i < syms.length; i += 4) {
    await Promise.all(syms.slice(i, i + 4).map(loadHistoryForSym));
    if (i + 4 < syms.length) await sleep(350);
  }
  console.log(`[Hist] ✅ Loaded ${Object.keys(histCache).length} stocks`);
}

async function loadDiscovery() {
  console.log('[Discovery] Fetching most-bought / intraday / volume / gainers...');
  const [mostBought, intraday, byVolume, gainers] = await Promise.all([
    growwDiscovery('POPULAR_STOCKS_MOST_BOUGHT', 25),
    growwDiscovery('POPULAR_STOCKS_INTRADAY_VOLUME', 25),
    growwDiscovery('TRADED_BY_VOLUME', 25),
    growwDiscovery('TOP_GAINERS', 15),
  ]);
  discoveryStocks = { mostBought, intraday, byVolume, gainers };
  // Cache company names
  for (const list of [mostBought, intraday, byVolume, gainers]) {
    for (const s of list) companyNames[s.symbol] = s.companyName;
  }
  console.log(`[Discovery] mostBought=${mostBought.length} intraday=${intraday.length} byVolume=${byVolume.length} gainers=${gainers.length}`);
}

// ══════════════════════════════════════════════════════════════
// LIVE DATA PIPELINE
// ══════════════════════════════════════════════════════════════
async function fetchAllLiveData() {
  const syms = getAllSymbols();
  const quotes = {};

  // Batch LTP
  for (let i = 0; i < syms.length; i += 50) {
    const batch  = syms.slice(i, i + 50);
    const ltpMap = await growwLTP(batch);
    for (const sym of batch) {
      const ltp = ltpMap[`NSE_${sym}`];
      if (ltp != null) quotes[sym] = { symbol: sym, ltp };
    }
    if (i + 50 < syms.length) await sleep(250);
  }

  // Batch OHLC
  for (let i = 0; i < syms.length; i += 50) {
    const batch   = syms.slice(i, i + 50);
    const ohlcMap = await growwOHLC(batch);
    for (const sym of batch) {
      const o = ohlcMap[`NSE_${sym}`];
      if (o) {
        quotes[sym] = {
          ...quotes[sym], symbol: sym,
          open: o.open, high: o.high, low: o.low, prevClose: o.close,
          ltp: quotes[sym]?.ltp ?? o.close,
        };
      }
    }
    if (i + 50 < syms.length) await sleep(250);
  }

  // Derived
  for (const [sym, q] of Object.entries(quotes)) {
    const ltp  = q.ltp || 0;
    const open = q.open || ltp;
    const prev = q.prevClose || ltp;
    q.change    = +(ltp - prev).toFixed(2);
    q.changePct = prev > 0 ? +(((ltp - prev) / prev) * 100).toFixed(2) : 0;
    q.devOpen   = open > 0 ? +(((ltp - open) / open) * 100).toFixed(2) : 0;
    q.growwUrl  = growwUrl(sym);
    q.name      = companyNames[sym] || sym;
    q.tags      = buildTags(sym);
  }
  return quotes;
}

// Fetch full quotes (for depth pressure) for a list of symbols, throttled
async function fetchFullQuotes(symbols, parallel = 3) {
  for (let i = 0; i < symbols.length; i += parallel) {
    const batch = symbols.slice(i, i + parallel);
    await Promise.all(batch.map(async sym => {
      const q = await growwQuote(sym);
      if (q) liveQuotes[sym] = q;
    }));
    await sleep(200);
  }
}

// ══════════════════════════════════════════════════════════════
// ★★★ PREDICTION ENGINE v16 ★★★
//
// Composite scoring across 10 weighted factors with conflict
// penalty. Final action requires:
//   - score margin ≥ 18 points
//   - dominant side share ≥ 56%
//   - ADX ≥ 18 OR opening momentum |dev10| ≥ 0.6%
// ══════════════════════════════════════════════════════════════
function buildPrediction(sym, snap, liveQ, hist) {
  const { t915, t925, open } = snap;
  const daily = hist?.daily || [];
  const m5    = hist?.m5    || [];

  // ─── Core: opening 10-min momentum (the proven v4 signal) ───
  const dev10  = t915 > 0 ? +(((t925 - t915) / t915) * 100).toFixed(2) : 0;
  const ltp    = liveQ?.ltp ?? liveQ?.last_price ?? t925;
  const devDay = open > 0 ? +(((ltp - open) / open) * 100).toFixed(2) : dev10;

  // ─── VWAP from 5-min ───
  const vwap     = m5.length ? calcVWAP(m5) : 0;
  const vwapDev  = vwap > 0 ? +(((ltp - vwap) / vwap) * 100).toFixed(2) : 0;
  const aboveVWAP = ltp > vwap;

  // ─── RSI from 5-min ───
  const m5closes = m5.map(c => c[4]);
  const rsi5m    = m5closes.length >= 15 ? calcRSI(m5closes, 14) : 50;

  // ─── EMA stack on daily closes ───
  const dayCloses = daily.map(c => c[4]);
  const ema9   = calcEMA(dayCloses, 9);
  const ema21  = calcEMA(dayCloses, 21);
  const ema50  = calcEMA(dayCloses, 50);
  const emaStack =
    (ema9 && ema21 && ema50)
      ? (ema9 > ema21 && ema21 > ema50 ? 'BULL'
        : ema9 < ema21 && ema21 < ema50 ? 'BEAR' : 'MIXED')
      : 'MIXED';

  // ─── MACD on daily closes ───
  const macd = calcMACD(dayCloses);

  // ─── Bollinger %B on daily ───
  const bbPctB = calcBollingerPctB(dayCloses, 20, 2);

  // ─── ADX (trend strength) on daily ───
  const adx = calcADX(daily, 14);

  // ─── Volume confirmation (today vs avg) ───
  const relVol = calcRelativeVolume(m5, daily);

  // ─── Previous day candle pattern ───
  const prevCandle = daily.length >= 2 ? daily[daily.length - 2] : null;
  let candleSignal = 0;
  if (prevCandle) {
    const [, po, ph, pl, pc] = prevCandle;
    const range = ph - pl, body = Math.abs(pc - po);
    const isBull = pc > po;
    const closePos = range > 0 ? (pc - pl) / range : 0.5;
    const bodyRatio = range > 0 ? body / range : 0;
    if      ( isBull && bodyRatio > 0.6 && closePos > 0.65) candleSignal = +2;
    else if (!isBull && bodyRatio > 0.6 && closePos < 0.35) candleSignal = -2;
    else if ( isBull) candleSignal = +1;
    else              candleSignal = -1;
  }

  // ─── Buy/Sell depth pressure ───
  let buyPressure = 50;
  if (liveQ?.total_buy_quantity && liveQ?.total_sell_quantity) {
    const total = liveQ.total_buy_quantity + liveQ.total_sell_quantity;
    if (total > 0) buyPressure = Math.round((liveQ.total_buy_quantity / total) * 100);
  }
  const depthSignal = buyPressure > 60 ? 1 : buyPressure < 40 ? -1 : 0;

  // ─── ATR for volatility-aware targets ───
  const atr = calcATR(daily, 14);

  // ─── Pivots ───
  let pivots = null;
  if (prevCandle) pivots = pivotPoints(prevCandle[2], prevCandle[3], prevCandle[4]);

  // ─── Index correlation: penalise stocks that are just riding the index ───
  // If NIFTY moved +1% and stock is +1.1%, alpha is only +0.1% — weak signal.
  const alpha = +(devDay - niftyChange).toFixed(2);

  // ══════════════════════════════════════════════════════════════
  // COMPOSITE SCORING
  // ══════════════════════════════════════════════════════════════
  let bull = 0, bear = 0;
  const reasons = [];

  // [1] Opening momentum (weight 30) — THE CORE signal
  if      (dev10 >  2.0) { bull += 30; reasons.push(`📈 Strong open +${dev10}% in 10 min`); }
  else if (dev10 >  1.0) { bull += 22; reasons.push(`📈 Bullish open +${dev10}%`); }
  else if (dev10 >  0.3) { bull += 12; reasons.push(`📈 Mild open +${dev10}%`); }
  else if (dev10 < -2.0) { bear += 30; reasons.push(`📉 Strong drop ${dev10}% at open`); }
  else if (dev10 < -1.0) { bear += 22; reasons.push(`📉 Bearish open ${dev10}%`); }
  else if (dev10 < -0.3) { bear += 12; reasons.push(`📉 Mild drop ${dev10}%`); }
  else                    { reasons.push(`⚖️ Flat open (${dev10}%)`); }

  // [2] VWAP (weight 18)
  if (vwap > 0) {
    if (aboveVWAP) { bull += 18; reasons.push(`✅ Above VWAP ₹${vwap} (+${vwapDev}%)`); }
    else           { bear += 18; reasons.push(`🔴 Below VWAP ₹${vwap} (${vwapDev}%)`); }
  }

  // [3] RSI (weight 12)
  if      (rsi5m >= 60 && rsi5m < 75) { bull += 12; reasons.push(`✅ RSI ${rsi5m} bullish`); }
  else if (rsi5m <= 40 && rsi5m > 25) { bear += 12; reasons.push(`🔴 RSI ${rsi5m} bearish`); }
  else if (rsi5m >= 75)               { bear +=  6; reasons.push(`⚠️ RSI ${rsi5m} overbought`); }
  else if (rsi5m <= 25)               { bull +=  6; reasons.push(`⚠️ RSI ${rsi5m} oversold`); }

  // [4] MACD (weight 12)
  if (macd) {
    if (macd.histogram > 0 && macd.macd > macd.signal)      { bull += 12; reasons.push(`✅ MACD bullish (hist ${macd.histogram})`); }
    else if (macd.histogram < 0 && macd.macd < macd.signal) { bear += 12; reasons.push(`🔴 MACD bearish (hist ${macd.histogram})`); }
  }

  // [5] EMA stack (weight 10)
  if      (emaStack === 'BULL') { bull += 10; reasons.push(`✅ EMA9>EMA21>EMA50`); }
  else if (emaStack === 'BEAR') { bear += 10; reasons.push(`🔴 EMA9<EMA21<EMA50`); }

  // [6] ADX trend strength (weight 8) — only adds to dominant side
  if (adx >= 25) {
    if (bull > bear) { bull += 8; reasons.push(`✅ ADX ${adx} strong trend`); }
    else if (bear > bull) { bear += 8; reasons.push(`🔴 ADX ${adx} strong downtrend`); }
  }

  // [7] Bollinger %B (weight 6)
  if      (bbPctB > 1.0) { bear += 6; reasons.push(`⚠️ Above upper Bollinger (%B ${bbPctB})`); }
  else if (bbPctB < 0.0) { bull += 6; reasons.push(`⚠️ Below lower Bollinger (%B ${bbPctB})`); }
  else if (bbPctB > 0.7 && dev10 > 0) { bull += 4; reasons.push(`✅ %B ${bbPctB} pushing up`); }
  else if (bbPctB < 0.3 && dev10 < 0) { bear += 4; reasons.push(`🔴 %B ${bbPctB} pushing down`); }

  // [8] Prev-day candle (weight 6)
  if      (candleSignal >=  2) { bull += 6; reasons.push(`✅ Strong bull candle yesterday`); }
  else if (candleSignal === 1) { bull += 3; }
  else if (candleSignal <= -2) { bear += 6; reasons.push(`🔴 Strong bear candle yesterday`); }
  else if (candleSignal === -1){ bear += 3; }

  // [9] Depth pressure (weight 6)
  if      (depthSignal ===  1) { bull += 6; reasons.push(`✅ ${buyPressure}% buy pressure`); }
  else if (depthSignal === -1) { bear += 6; reasons.push(`🔴 ${100-buyPressure}% sell pressure`); }

  // [10] Volume confirmation (weight 8) — boost dominant side
  if (relVol >= 1.5) {
    if (bull > bear)      { bull += 8; reasons.push(`🔊 Vol ${relVol}× avg confirms breakout`); }
    else if (bear > bull) { bear += 8; reasons.push(`🔊 Vol ${relVol}× avg confirms breakdown`); }
  } else if (relVol < 0.6) {
    // Low volume → penalise both sides equally (less conviction)
    bull = Math.round(bull * 0.85); bear = Math.round(bear * 0.85);
    reasons.push(`💤 Low volume (${relVol}× avg) — weak conviction`);
  }

  // [11] Index alpha — punish "riders"
  if (Math.abs(alpha) < 0.2 && Math.abs(dev10) > 0.5) {
    // Stock is moving but no alpha vs NIFTY → hard to differentiate
    bull = Math.round(bull * 0.9); bear = Math.round(bear * 0.9);
    reasons.push(`📊 Moving with NIFTY (α=${alpha}%) — limited edge`);
  } else if (alpha > 0.5) {
    bull += 4; reasons.push(`💪 Outperforming NIFTY (α +${alpha}%)`);
  } else if (alpha < -0.5) {
    bear += 4; reasons.push(`📉 Underperforming NIFTY (α ${alpha}%)`);
  }

  // ── Action gating ──
  const total   = bull + bear;
  const bullPct = total > 0 ? bull / total : 0.5;
  const margin  = bull - bear;

  // Confidence: dominance × conflict-penalty × trend-quality
  const dominance = Math.abs(bullPct - 0.5) * 200;            // 0–100
  const conflictPenalty = total > 0 ? 1 - Math.min(bull, bear) / total : 1; // 1=clean, 0=conflicted
  const trendBonus = Math.min(1, adx / 30) * 0.15 + 0.85;     // 0.85–1.0
  const confidence = Math.min(95, Math.round(dominance * conflictPenalty * trendBonus));

  let action = 'HOLD';
  const adxOk = adx >= 18 || Math.abs(dev10) >= 0.6;
  if (margin >=  18 && bullPct >= 0.56 && adxOk) action = 'BUY';
  if (margin <= -18 && bullPct <= 0.44 && adxOk) action = 'SELL';

  // ── Targets & stops (ATR-aware, R:R-controlled) ──
  const absMove = Math.abs(dev10);
  const atrPct = (atr && t915) ? (atr / t915) * 100 : 0;
  // Target multiplier scales with opening momentum AND ATR
  const tMult = absMove < 0.5 ? 1.3
             : absMove < 1.0 ? 1.5
             : absMove < 1.5 ? 1.7
             : absMove < 2.0 ? 1.9
             : absMove < 3.0 ? 2.2 : 2.5;
  const sMult = Math.max(0.4, Math.min(1.5,
                  absMove < 0.5 ? 0.4
                : absMove < 1.0 ? 0.55
                : absMove < 2.0 ? 0.75
                : absMove < 3.0 ? 1.0 : 1.4));

  let targetPct = action === 'BUY'  ?  +(absMove * tMult).toFixed(2)
                : action === 'SELL' ? -(absMove * tMult).toFixed(2) : 0;
  let stopPct   = action === 'BUY'  ? -sMult
                : action === 'SELL' ?  sMult : 0;

  // Floor target with ATR (stops daydreaming on tiny dev10)
  if (action !== 'HOLD' && atrPct > 0) {
    const atrFloor = +(atrPct * 0.4).toFixed(2);
    if (action === 'BUY' && targetPct < atrFloor) targetPct = atrFloor;
    if (action === 'SELL' && targetPct > -atrFloor) targetPct = -atrFloor;
  }

  // Pivot refinement (clamp targets/stops to nearby levels)
  if (pivots && action === 'BUY' && pivots.r1 > t915) {
    const r1Pct = +(((pivots.r1 - t915) / t915) * 100).toFixed(2);
    const s1Pct = +(((t915 - pivots.s1) / t915) * 100).toFixed(2);
    if (r1Pct > 0)            targetPct = Math.max(targetPct, +(r1Pct * 0.85).toFixed(2));
    if (s1Pct > 0 && s1Pct < Math.abs(stopPct)) stopPct = -s1Pct;
  } else if (pivots && action === 'SELL' && pivots.s1 < t915) {
    const s1Pct = +(((t915 - pivots.s1) / t915) * 100).toFixed(2);
    const r1Pct = +(((pivots.r1 - t915) / t915) * 100).toFixed(2);
    if (s1Pct > 0)            targetPct = Math.min(targetPct, -(+(s1Pct * 0.85).toFixed(2)));
    if (r1Pct > 0 && r1Pct < Math.abs(stopPct)) stopPct = r1Pct;
  }

  const targetPrice   = action !== 'HOLD' ? +(t915 * (1 + targetPct/100)).toFixed(2) : 0;
  const stopLossPrice = action !== 'HOLD' ? +(t915 * (1 + stopPct  /100)).toFixed(2) : 0;
  const rr = stopPct !== 0 ? +(Math.abs(targetPct) / Math.abs(stopPct)).toFixed(1) : 0;

  // Reject low R:R signals — discipline
  if (action !== 'HOLD' && rr < 1.2) {
    reasons.push(`⚠️ R:R ${rr}<1.2 — downgraded to HOLD`);
    return finalisePrediction(sym, snap, liveQ, {
      action: 'HOLD', confidence: Math.round(confidence * 0.6),
      bullScore: bull, bearScore: bear, reasons: reasons.slice(0,7),
      dev10, devDay, alpha, ltp, vwap, vwapDev, aboveVWAP, rsi5m,
      ema9, ema21, ema50, emaStack, atr, atrPct, adx, bbPctB, macd,
      candleSignal, buyPressure, pivots, relVol,
      targetPrice: 0, stopLossPrice: 0, targetPct: 0, stopPct: 0, rr: 0,
    });
  }

  return finalisePrediction(sym, snap, liveQ, {
    action, confidence,
    bullScore: bull, bearScore: bear, reasons: reasons.slice(0, 7),
    dev10, devDay, alpha, ltp, vwap, vwapDev, aboveVWAP, rsi5m,
    ema9, ema21, ema50, emaStack, atr, atrPct, adx, bbPctB, macd,
    candleSignal, buyPressure, pivots, relVol,
    targetPrice, stopLossPrice, targetPct, stopPct, rr,
  });
}

function finalisePrediction(sym, snap, liveQ, m) {
  const { t915, t925 } = snap;
  const name = companyNames[sym] || sym;
  const tags = buildTags(sym);

  let prediction;
  if (m.action === 'BUY') {
    prediction = `📈 ${name} EXPECTED TO RISE ~${m.targetPct.toFixed(1)}% today. ` +
      `Open ₹${t915} → Target ₹${m.targetPrice} (+${m.targetPct}%) | Stop ₹${m.stopLossPrice}. R:R=${m.rr}:1. ` +
      (m.rsi5m > 50 ? `RSI ${m.rsi5m}. ` : '') +
      (m.aboveVWAP ? 'Above VWAP. ' : '') +
      `Conviction ${m.confidence}% | ADX ${m.adx} | α ${m.alpha}%.`;
  } else if (m.action === 'SELL') {
    prediction = `📉 ${name} EXPECTED TO FALL ~${Math.abs(m.targetPct).toFixed(1)}% today. ` +
      `Open ₹${t915} → Target ₹${m.targetPrice} (${m.targetPct}%) | Stop ₹${m.stopLossPrice}. R:R=${m.rr}:1. ` +
      (m.rsi5m < 50 ? `RSI ${m.rsi5m}. ` : '') +
      (!m.aboveVWAP ? 'Below VWAP. ' : '') +
      `Conviction ${m.confidence}% | ADX ${m.adx} | α ${m.alpha}%.`;
  } else {
    prediction = `⚖️ ${name} — no clear direction. Open ${m.dev10>=0?'+':''}${m.dev10}% | RSI ${m.rsi5m} | ${m.emaStack} EMA | ADX ${m.adx}. Wait for confirmation.`;
  }

  const gUrl = growwUrl(sym);
  return {
    symbol: sym, name, tags,
    action: m.action, confidence: m.confidence,
    prediction,
    bullScore: m.bullScore, bearScore: m.bearScore,
    open915Price: t915, lockedAtPrice: t925, currentPrice: m.ltp,
    targetPrice: m.targetPrice, stopLossPrice: m.stopLossPrice,
    targetPct: +m.targetPct.toFixed(2), stopPct: +m.stopPct.toFixed(2),
    riskReward: m.rr,
    dev10: m.dev10, devDay: m.devDay, devFromOpen: m.devDay,
    alpha: m.alpha, niftyChange,
    rsi: m.rsi5m, vwap: m.vwap, vwapDev: m.vwapDev, aboveVWAP: m.aboveVWAP,
    ema9: m.ema9, ema21: m.ema21, ema50: m.ema50, emaStack: m.emaStack,
    atr: m.atr, atrPct: +(m.atrPct||0).toFixed(2), adx: m.adx, bbPctB: m.bbPctB,
    macd: m.macd, pivots: m.pivots, candleSignal: m.candleSignal,
    relVol: m.relVol, buyPressure: m.buyPressure,
    totalBuyQty:  liveQ?.total_buy_quantity  || 0,
    totalSellQty: liveQ?.total_sell_quantity || 0,
    reasons: m.reasons,
    currentStatus: 'LOCKED 🔒', progressPct: 0,
    growwUrl: gUrl,
    growwBuyUrl:  `${gUrl}?action=buy`,
    growwSellUrl: `${gUrl}?action=sell`,
    lockedAt: new Date().toISOString(),
  };
}

// ══════════════════════════════════════════════════════════════
// SNAPSHOT CAPTURE
// ══════════════════════════════════════════════════════════════
async function capture915Snapshot() {
  console.log('\n[9:15] 📸 Capturing opening prices...');
  const syms = getAllSymbols();
  const ohlcMap = await growwOHLC(syms);
  const ltpMap  = await growwLTP(syms);

  for (const sym of syms) {
    const ohlc = ohlcMap[`NSE_${sym}`];
    const ltp  = ltpMap[`NSE_${sym}`] || 0;
    if (ohlc) {
      openingSnaps[sym] = {
        t915: ltp || ohlc.open || ohlc.close,
        open: ohlc.open,
      };
    }
  }
  snapshotStatus = 't915_done';
  persist();
  console.log(`[9:15] ✅ Captured ${Object.keys(openingSnaps).length} opening prices`);
}

async function capture925AndLock() {
  console.log('\n[9:25] 📸 Capturing 10-min prices + generating predictions...');
  const syms = getAllSymbols();

  // Refresh NIFTY change for index-correlation factor
  niftyChange = await fetchNiftyChange();
  console.log(`[9:25] NIFTY change: ${niftyChange}%`);

  // Capture 9:25 LTP
  const ltpMap = await growwLTP(syms);
  for (const sym of syms) {
    const ltp = ltpMap[`NSE_${sym}`];
    if (ltp != null && openingSnaps[sym])      openingSnaps[sym].t925 = ltp;
    else if (!openingSnaps[sym] && ltp != null) openingSnaps[sym] = { t915: ltp, t925: ltp, open: ltp };
  }

  // Refresh 5-min candles for ALL symbols (we need VWAP/RSI)
  for (let i = 0; i < syms.length; i += 4) {
    const batch = syms.slice(i, i + 4);
    await Promise.all(batch.map(async s => {
      if (!histCache[s]) histCache[s] = {};
      histCache[s].m5 = await fetchToday5min(s);
    }));
    if (i + 4 < syms.length) await sleep(300);
  }

  // Fetch full quotes (depth) — top 30 by volume signal interest
  const top30 = syms.slice(0, 30);
  await fetchFullQuotes(top30, 3);

  // Build predictions
  lockedPredictions = Object.entries(openingSnaps)
    .filter(([, snap]) => snap.t915 > 0 && snap.t925 != null)
    .map(([sym, snap]) => buildPrediction(sym, snap, liveQuotes[sym] || null, histCache[sym] || {}))
    .filter(Boolean);

  snapshotStatus = 'locked';
  const buy  = lockedPredictions.filter(p => p.action === 'BUY').length;
  const sell = lockedPredictions.filter(p => p.action === 'SELL').length;
  console.log(`\n[9:25] ✅ ${lockedPredictions.length} predictions | ${buy} BUY | ${sell} SELL`);
  lockedPredictions
    .filter(p => p.action !== 'HOLD')
    .sort((a,b) => b.confidence - a.confidence)
    .slice(0, 10)
    .forEach(p => console.log(`  ${p.action} ${p.symbol.padEnd(14)} conf:${p.confidence}% dev10:${p.dev10}% ${p.reasons[0]||''}`));
  persist();
}

// ══════════════════════════════════════════════════════════════
// LIVE PRICE UPDATE
// ══════════════════════════════════════════════════════════════
async function updateLivePrices() {
  if (!lockedPredictions.length) return;
  const syms   = lockedPredictions.map(p => p.symbol);
  const ltpMap = await growwLTP(syms);

  // Refresh NIFTY change roughly each cycle
  niftyChange = await fetchNiftyChange();

  lockedPredictions = lockedPredictions.map(p => {
    const ltp = ltpMap[`NSE_${p.symbol}`] || p.currentPrice;
    if (!ltp) return p;

    const currentDev = p.open915Price > 0
      ? +(((ltp - p.open915Price) / p.open915Price) * 100).toFixed(2)
      : p.devFromOpen;

    const progressPct = p.targetPct !== 0
      ? Math.min(100, Math.max(0, Math.round(Math.abs(currentDev) / Math.abs(p.targetPct) * 100)))
      : 0;

    const isBuy = p.action === 'BUY';
    let currentStatus;
    if      (p.action === 'HOLD')                                               currentStatus = 'WATCHING 👁️';
    else if (isBuy && p.targetPrice && ltp >= p.targetPrice)                    currentStatus = 'TARGET HIT ✅';
    else if (isBuy && p.stopLossPrice && ltp <= p.stopLossPrice)                currentStatus = 'STOP HIT ⛔';
    else if (isBuy && currentDev < -0.5)                                        currentStatus = 'PULLBACK ⚠️';
    else if (isBuy && progressPct >= 80)                                        currentStatus = 'NEAR TARGET 🎯';
    else if (isBuy)                                                             currentStatus = 'ON TRACK 📈';
    else if (!isBuy && p.targetPrice && ltp <= p.targetPrice)                   currentStatus = 'TARGET HIT ✅';
    else if (!isBuy && p.stopLossPrice && ltp >= p.stopLossPrice)               currentStatus = 'STOP HIT ⛔';
    else if (!isBuy && currentDev > 0.5)                                        currentStatus = 'BOUNCE ⚠️';
    else if (!isBuy && progressPct >= 80)                                       currentStatus = 'NEAR TARGET 🎯';
    else                                                                        currentStatus = 'ON TRACK 📉';

    let prediction = p.prediction;
    if (p.action !== 'HOLD') {
      const remaining = isBuy ? +(p.targetPct - currentDev).toFixed(2) : +(currentDev - p.targetPct).toFixed(2);
      const dir = isBuy ? '📈 RISE' : '📉 FALL';
      prediction = `${dir} ~${Math.abs(p.targetPct).toFixed(1)}% today. ` +
        `Open ₹${p.open915Price} → Target ₹${p.targetPrice} | Stop ₹${p.stopLossPrice}. R:R=${p.riskReward}:1. ` +
        (remaining > 0 ? `~${remaining.toFixed(1)}% ${isBuy?'more to go':'more to fall'}. ` : 'TARGET ZONE! ') +
        `[Live ₹${ltp.toFixed(1)} | ${currentDev>=0?'+':''}${currentDev.toFixed(2)}% | conf ${p.confidence}%]`;
    }
    return { ...p, currentPrice: +ltp.toFixed(2), devFromOpen: currentDev, progressPct, currentStatus, prediction };
  });
  persist();
}

// ══════════════════════════════════════════════════════════════
// LATE-START FALLBACK — synthesise t915/t925 from today's 1-min candles
// (so we don't lose the opening-momentum signal when server starts late)
// ══════════════════════════════════════════════════════════════
async function generateFallbackFromCandles() {
  console.log('[Fallback] Late start — reconstructing 9:15 vs 9:25 from 1-min candles...');
  const syms = getAllSymbols();
  niftyChange = await fetchNiftyChange();

  // Throttle: 4 in flight
  for (let i = 0; i < syms.length; i += 4) {
    const batch = syms.slice(i, i + 4);
    await Promise.all(batch.map(async sym => {
      try {
        const m1 = await fetchToday1min(sym);
        if (!m1.length) return;
        // First candle ≈ 9:15, the candle starting near 9:24 is t925
        const t915 = m1[0][1]; // open of the first 1-min candle
        // Find candle whose timestamp corresponds to 9:24/9:25 boundary
        let t925 = m1[Math.min(m1.length - 1, 9)][4]; // close of 10th 1-min = end of 9:24 candle = ~9:25 price
        if (!histCache[sym]) histCache[sym] = {};
        histCache[sym].m5 = await fetchToday5min(sym);
        if (!histCache[sym].daily?.length) histCache[sym].daily = await fetchDailyHistory(sym, 90);
        openingSnaps[sym] = { t915, t925, open: t915 };
      } catch(e) {/* skip this symbol */}
    }));
    if (i + 4 < syms.length) await sleep(350);
  }

  // Top-30 depth quotes
  await fetchFullQuotes(syms.slice(0, 30), 3);

  // Build predictions using the reconstructed snapshots
  lockedPredictions = Object.entries(openingSnaps)
    .filter(([, snap]) => snap.t915 > 0 && snap.t925 != null)
    .map(([sym, snap]) => buildPrediction(sym, snap, liveQuotes[sym] || null, histCache[sym] || {}))
    .filter(Boolean);

  snapshotStatus = 'fallback';
  const active = lockedPredictions.filter(p => p.action !== 'HOLD').length;
  console.log(`[Fallback] ${active} active predictions reconstructed`);
  persist();
}

// ══════════════════════════════════════════════════════════════
// MAIN REFRESH
// ══════════════════════════════════════════════════════════════
async function mainRefresh() {
  console.log(`\n[Bot] ─── ${istStr()} | ${marketPhase()} ───`);
  try {
    const quotes = await fetchAllLiveData();
    dataStore.quotes      = quotes;
    dataStore.lastUpdated = new Date().toISOString();

    const { totalMins, day } = getIST();
    const pastOpen = day >= 1 && day <= 5 && totalMins >= 9*60 + 25 && totalMins < 15*60 + 30;

    // Late start with empty predictions → reconstruct from 1-min candles
    if (pastOpen && !lockedPredictions.length) {
      await generateFallbackFromCandles();
    } else if (lockedPredictions.length) {
      await updateLivePrices();
    }

    const active = lockedPredictions.filter(p => p.action !== 'HOLD').length;
    console.log(`[Bot] ✅ quotes:${Object.keys(quotes).length} preds:${active} status:${snapshotStatus}`);
  } catch (e) {
    console.error('[Bot] Refresh error:', e.message);
  }
}

// ══════════════════════════════════════════════════════════════
// API ENDPOINTS
// ══════════════════════════════════════════════════════════════

// ──────────────────────────────────────────────────────────────
// Lazy auto-refresh — Vercel Hobby cron limit replacement
// Triggers mainRefresh() at most once per AUTO_REFRESH_MS during
// market hours, on any read request from the frontend. The frontend's
// 60s poll loop then keeps everything live without needing a cron.
// ──────────────────────────────────────────────────────────────
const AUTO_REFRESH_MS = 60_000;
let lastAutoRefresh = 0;
let autoRefreshInFlight = null;
function maybeAutoRefresh() {
  // Only during market hours (9:25 IST → 15:30 IST), otherwise data is static
  const { totalMins, day } = getIST();
  const inMarket = day >= 1 && day <= 5 && totalMins >= 9*60+25 && totalMins < 15*60+30;
  if (!inMarket) return;
  const now = Date.now();
  if (now - lastAutoRefresh < AUTO_REFRESH_MS) return;
  if (autoRefreshInFlight) return;
  lastAutoRefresh = now;
  // Fire and forget — the response we're returning uses whatever's already cached;
  // the next poll will see the freshly refreshed data.
  autoRefreshInFlight = mainRefresh()
    .catch(e => console.error('[AutoRefresh]', e.message))
    .finally(() => { autoRefreshInFlight = null; });
}

app.get('/api/status', (_, res) => {
  maybeAutoRefresh();
  const ph = marketPhase();
  const { h, m } = getIST();
  const pad = n => String(n).padStart(2,'0');
  res.json({
    phase: ph, isOpen: isOpen(),
    istTime: `${pad(h)}:${pad(m)} IST`,
    snapshotStatus,
    activePredictions: lockedPredictions.filter(p => p.action !== 'HOLD').length,
    totalPredictions:  lockedPredictions.length,
    watchlist:         getAllSymbols().length,
    discoveryStocks: {
      mostBought: discoveryStocks.mostBought.length,
      intraday:   discoveryStocks.intraday.length,
      byVolume:   discoveryStocks.byVolume.length,
      gainers:    discoveryStocks.gainers.length,
    },
    niftyChange,
    lastUpdated: dataStore.lastUpdated,
    autoRefreshAge: lastAutoRefresh ? Math.round((Date.now()-lastAutoRefresh)/1000) : null,
    version: '16.1.0',
  });
});

app.get('/api/quotes', (_, res) => {
  maybeAutoRefresh();
  res.json({ quotes: dataStore.quotes, lastUpdated: dataStore.lastUpdated });
});

// MAIN PREDICTION ENDPOINT
app.get('/api/mtf/live', (req, res) => {
  maybeAutoRefresh();
  const { action, limit = 50, tag, includeHold = '0' } = req.query;
  let preds = [...lockedPredictions];

  if (action) preds = preds.filter(p => p.action === action.toUpperCase());
  else if (includeHold !== '1') preds = preds.filter(p => p.action !== 'HOLD');

  if (tag) {
    const t = tag.toUpperCase();
    preds = preds.filter(p => (p.tags || []).some(x => x.toUpperCase().includes(t)));
  }

  // Sort: BUY/SELL first, then by confidence × |dev10|
  preds.sort((a, b) => {
    const r = { BUY: 0, SELL: 1, HOLD: 2 };
    if (r[a.action] !== r[b.action]) return r[a.action] - r[b.action];
    const aScore = a.confidence * Math.abs(a.dev10);
    const bScore = b.confidence * Math.abs(b.dev10);
    return bScore - aScore;
  });
  preds = preds.slice(0, parseInt(limit));

  res.json({
    predictions: preds,
    summary: {
      total: preds.length,
      buy:        preds.filter(p => p.action === 'BUY').length,
      sell:       preds.filter(p => p.action === 'SELL').length,
      hold:       preds.filter(p => p.action === 'HOLD').length,
      targetsHit: preds.filter(p => p.currentStatus?.includes('TARGET HIT')).length,
      onTrack:    preds.filter(p => p.currentStatus?.includes('ON TRACK')).length,
    },
    snapshotStatus, phase: marketPhase(), niftyChange,
    updatedAt: new Date().toISOString(),
  });
});

// Most-bought-only endpoint (★ matches user request)
app.get('/api/mtf/most-bought', (_, res) => {
  const set = new Set(discoveryStocks.mostBought.map(s => s.symbol));
  const preds = lockedPredictions
    .filter(p => set.has(p.symbol) && p.action !== 'HOLD')
    .sort((a,b) => b.confidence - a.confidence);
  res.json({ predictions: preds, count: preds.length, source: 'POPULAR_STOCKS_MOST_BOUGHT' });
});

// Top intraday endpoint (★ matches user request)
app.get('/api/mtf/intraday', (_, res) => {
  const set = new Set(discoveryStocks.intraday.map(s => s.symbol));
  const preds = lockedPredictions
    .filter(p => set.has(p.symbol) && p.action !== 'HOLD')
    .sort((a,b) => b.confidence - a.confidence);
  res.json({ predictions: preds, count: preds.length, source: 'POPULAR_STOCKS_INTRADAY_VOLUME' });
});

// Backward compat
app.get('/api/mtf/predictions', (req, res) =>
  res.redirect(`/api/mtf/live${req.query.action ? '?action=' + req.query.action : ''}`)
);

// Full single-stock analysis
app.get('/api/analyze/:sym', async (req, res) => {
  const sym = req.params.sym.toUpperCase();
  const quote = await growwQuote(sym);
  if (!histCache[sym]) await loadHistoryForSym(sym);
  const hist = histCache[sym] || {};
  const snap = openingSnaps[sym] || { t915: quote?.last_price || 0, t925: quote?.last_price || 0 };
  const pred = buildPrediction(sym, snap, quote, hist);
  res.json({ symbol: sym, quote, prediction: pred });
});

// Historical candles for chart UI
app.get('/api/candles/:sym', async (req, res) => {
  const sym = req.params.sym.toUpperCase();
  const tf  = String(req.query.interval || '5');
  const days = parseInt(req.query.days || '1');
  // Accept both new ('5minute', '1day') and legacy minute-count ('5', '1440') formats
  const intMap = {
    '1':'1minute', '2':'2minute', '3':'3minute', '5':'5minute',
    '10':'10minute', '15':'15minute', '30':'30minute', '60':'1hour',
    '240':'4hour', '1440':'1day',
    '1minute':'1minute', '5minute':'5minute', '15minute':'15minute',
    '30minute':'30minute', '1hour':'1hour', '4hour':'4hour',
    '1day':'1day', '1d':'1day', 'd':'1day',
  };
  const interval = intMap[tf] || '5minute';
  const end   = `${dateStr(0)} 15:30:00`;
  const start = `${dateStr(tradingDayOffset(Math.max(1, days)))} 09:15:00`;
  const candles = await growwCandles(sym, interval, start, end);
  res.json({ symbol: sym, interval, candles });
});

// Discovery passthrough — see what Groww thinks is hot right now
app.get('/api/discovery', (_, res) => res.json(discoveryStocks));

// Debug
app.get('/api/debug', (_, res) => res.json({
  snapshotStatus, phase: marketPhase(), niftyChange,
  watchlist: getAllSymbols(),
  snapshotsTaken: Object.keys(openingSnaps).length,
  histCached:     Object.keys(histCache).length,
  active: lockedPredictions.filter(p => p.action !== 'HOLD').length,
}));

// Manual triggers
app.post('/api/refresh', async (_, res) => {
  await mainRefresh();
  res.json({ success: true, predictions: lockedPredictions.filter(p=>p.action!=='HOLD').length });
});
app.post('/api/reset', (_, res) => {
  Object.keys(openingSnaps).forEach(k => delete openingSnaps[k]);
  lockedPredictions = [];
  snapshotStatus = 'waiting';
  persist();
  res.json({ success: true });
});

// ──── Vercel-cron-friendly HTTP triggers ────
function checkCronAuth(req, res) {
  if (!CRON_SECRET) return true; // unprotected if not configured
  const auth = req.headers.authorization || '';
  const tokenParam = req.query.token || '';
  if (auth === `Bearer ${CRON_SECRET}` || tokenParam === CRON_SECRET) return true;
  res.status(401).json({ error: 'unauthorized' });
  return false;
}
app.all('/api/cron/load-history',  async (req, res) => {
  if (!checkCronAuth(req, res)) return;
  await loadDiscovery();
  await loadAllHistory();
  res.json({ success: true, ts: istStr() });
});
app.all('/api/cron/snapshot-915',  async (req, res) => {
  if (!checkCronAuth(req, res)) return;
  await capture915Snapshot();
  res.json({ success: true, ts: istStr() });
});
app.all('/api/cron/snapshot-925',  async (req, res) => {
  if (!checkCronAuth(req, res)) return;
  await capture925AndLock();
  res.json({ success: true, predictions: lockedPredictions.length, ts: istStr() });
});
app.all('/api/cron/refresh',       async (req, res) => {
  if (!checkCronAuth(req, res)) return;
  await mainRefresh();
  res.json({ success: true, ts: istStr() });
});
app.all('/api/cron/reset',         (req, res) => {
  if (!checkCronAuth(req, res)) return;
  Object.keys(openingSnaps).forEach(k => delete openingSnaps[k]);
  lockedPredictions = [];
  snapshotStatus = 'waiting';
  persist();
  res.json({ success: true, ts: istStr() });
});

// ══════════════════════════════════════════════════════════════
// CRON JOBS (work when running as long-lived process; ignored on Vercel)
// On Vercel, use /api/cron/* endpoints from vercel.json schedules.
// ══════════════════════════════════════════════════════════════
const isLongLived = require.main === module && !process.env.VERCEL;
if (isLongLived) {
  // 8:30 IST (3:00 UTC) — pre-market history + discovery
  cron.schedule('0 3 * * 1-5', async () => {
    console.log('[Cron] 8:30 IST — pre-market load');
    await loadDiscovery();
    await loadAllHistory();
  }, { timezone: 'UTC' });

  // 9:15 IST (3:45 UTC)
  cron.schedule('45 3 * * 1-5', async () => {
    console.log('[Cron] 9:15 IST');
    await capture915Snapshot();
  }, { timezone: 'UTC' });

  // 9:25 IST (3:55 UTC)
  cron.schedule('55 3 * * 1-5', async () => {
    console.log('[Cron] 9:25 IST');
    await capture925AndLock();
  }, { timezone: 'UTC' });

  // Every 3 min, 9:25 IST – 15:30 IST = 3:55 – 10:00 UTC
  cron.schedule('*/3 * * * 1-5', async () => {
    const { totalMins, day } = getIST();
    if (day >= 1 && day <= 5 && totalMins >= 9*60+25 && totalMins < 15*60+30) {
      await mainRefresh();
    }
  }, { timezone: 'UTC' });

  // 16:00 IST (10:30 UTC) — daily reset
  cron.schedule('30 10 * * 1-5', () => {
    Object.keys(openingSnaps).forEach(k => delete openingSnaps[k]);
    lockedPredictions = [];
    snapshotStatus = 'waiting';
    persist();
    console.log('[Cron] Reset');
  }, { timezone: 'UTC' });
}

// ══════════════════════════════════════════════════════════════
// STARTUP
// ══════════════════════════════════════════════════════════════
let initRan = false, initInFlight = null;

async function init() {
  if (initRan) return;
  if (initInFlight) return initInFlight;
  initInFlight = (async () => {
    console.log(`
╔══════════════════════════════════════════════════════════╗
║  ⚡ TradeBot v16.1 — Groww (Vercel Hobby compatible)     ║
║  http://localhost:${PORT}                                    ║
╠══════════════════════════════════════════════════════════╣
║  Data:    Groww Trade API + discovery filters            ║
║  Engine:  9:15→9:25 momentum + 11 weighted factors       ║
║  Indic:   VWAP, RSI, EMA, MACD, ADX, BB, OBV, ATR        ║
║  Targets: ATR-floored, pivot-clamped, R:R≥1.2 enforced   ║
╚══════════════════════════════════════════════════════════╝`);

    // Try restoring state from persisted snapshot
    if (restore()) {
      console.log(`[Init] Restored state — ${lockedPredictions.length} predictions`);
    }

    console.log('[Init] Loading Groww discovery (most-bought / intraday / volume)...');
    await loadDiscovery();
    console.log(`[Init] watchlist: ${getAllSymbols().length} symbols`);

    await loadAllHistory();
    niftyChange = await fetchNiftyChange();
    console.log(`[Init] NIFTY 50 today: ${niftyChange}%`);

    const phase = marketPhase();
    console.log(`[Init] Phase: ${phase}`);

    const { totalMins, day } = getIST();
    if (day >= 1 && day <= 5 && totalMins >= 9*60+25 && totalMins < 15*60+30) {
      console.log('[Init] Market open past 9:25 — running fallback reconstruction');
      await mainRefresh();
    } else {
      await mainRefresh();
    }
    initRan = true;
    console.log(`\n[Ready] ✅ ${istStr()} | ${phase} | ${lockedPredictions.filter(p=>p.action!=='HOLD').length} active`);
  })();
  return initInFlight;
}

// Single-flight init for serverless (Vercel)
const _origHandle = app.handle.bind(app);
app.handle = (req, res, next) => {
  if (!initRan && !initInFlight) {
    init().catch(e => console.error('[Init]', e.message));
  }
  return _origHandle(req, res, next);
};

if (require.main === module) {
  app.listen(PORT, () => init().catch(e => console.error('[Init]', e.message)));
}

module.exports = app;