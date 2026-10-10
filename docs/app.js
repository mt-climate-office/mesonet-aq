/* ============================================================================
   Mesonet Air Quality · app.js
   PurpleAir sensors at Montana Mesonet stations, on mco-web-style 0.13.1.
   Structure follows the kit exemplar (HOUSE-STYLE section refs as §).

   Data: the mesonet-aq archive on the MCO data CDN. manifest.json lists every
   file (HTTPS has no listing); latest/latest.json drives the map; hourly
   station-year and raw station-month Parquet drive the chart, read with the
   vendored hyparquet bundle (vendor/, scripts/vendor.sh) via HTTP range
   requests. ECharts is lazy-loaded the first time a chart is drawn.

   Classic script (CSP script-src 'self'); the Parquet reader comes in by
   dynamic import(), which classic scripts allow.
   ========================================================================== */
(function () {
  'use strict';

  /* ── Constants ─────────────────────────────────────────────────────────── */

  // Local development only: ?dev-data reads ./dev-data/air-quality (a local
  // `mesonet-aq` dry run copied next to the page; same origin, so the CSP's
  // 'self' covers it). Ignored on any other host.
  const DEV = ['localhost', '127.0.0.1'].includes(location.hostname) &&
    new URLSearchParams(location.search).has('dev-data');
  const BASE = DEV ? 'dev-data/air-quality' : 'https://data2.climate.umt.edu/mesonet/air-quality';
  const ECHARTS = {
    src: 'https://cdn.jsdelivr.net/npm/echarts@6.1.0/dist/echarts.min.js',
    integrity: 'sha384-C2iskrW/uPW46KzOjrvJIQo4YkV8lkD+QS0CrDN18IIPIpT/g2USu8bTP3nvmIAD',
  };
  const LS = (k) => `mco-air-${k}`;          // app-prefixed localStorage keys — §4

  /* kit-override: AQI category colors are the EPA/AirNow standard, a public
     health convention readers recognize, so they are an app-local data ROLE
     rather than a kit ramp (§6, decided with Kyle 2026-10-10). They are
     hue-coded, so color is never the only channel: the AQI number is printed
     on every map dot, and the category NAME is in the legend, tooltip,
     detail sheet and sr-table. `ink` is the text color on each fill (contrast
     >= 4.5:1 checked for both). */
  const AQI = [
    { key: 'Good',                           short: 'Good',          max: 50,  fill: '#00e400', ink: '#000000' },
    { key: 'Moderate',                       short: 'Moderate',      max: 100, fill: '#ffff00', ink: '#000000' },
    { key: 'Unhealthy for Sensitive Groups', short: 'USG',           max: 150, fill: '#ff7e00', ink: '#000000' },
    { key: 'Unhealthy',                      short: 'Unhealthy',     max: 200, fill: '#ff0000', ink: '#000000' },
    { key: 'Very Unhealthy',                 short: 'Very Unhealthy', max: 300, fill: '#8f3f97', ink: '#ffffff' },
    { key: 'Hazardous',                      short: 'Hazardous',     max: 500, fill: '#7e0023', ink: '#ffffff' },
  ];
  const aqiByKey = new Map(AQI.map((c) => [c.key, c]));
  const PM25_BREAKS = [9.0, 35.4, 55.4, 125.4, 225.4];   // the AQI category tops, ug/m3

  // Chart variables: hourly columns always exist; raw columns only where the
  // quantity has a 2-minute form.
  const VARS = [
    { key: 'pm25',   label: 'PM2.5, EPA-corrected', unit: 'µg/m³', hourly: ['pm2.5_epa'], raw: null, breaks: PM25_BREAKS },
    { key: 'aqi',    label: 'AQI (NowCast)', unit: 'AQI', hourly: ['aqi'], raw: null, breaks: [50, 100, 150, 200, 300] },
    { key: 'pm25ab', label: 'PM2.5 channels A and B (uncorrected)', unit: 'µg/m³', hourly: ['pm2.5_atm_a', 'pm2.5_atm_b'], raw: ['pm2.5_atm_a', 'pm2.5_atm_b'] },
    { key: 'pm10',   label: 'PM10 (uncorrected)', unit: 'µg/m³', hourly: ['pm10.0_atm'], raw: ['pm10.0_atm_a', 'pm10.0_atm_b'] },
    { key: 'pm1',    label: 'PM1.0 (uncorrected)', unit: 'µg/m³', hourly: ['pm1.0_atm'], raw: ['pm1.0_atm_a', 'pm1.0_atm_b'] },
    { key: 'temp',   label: 'Temperature (sensor housing)', unit: '°F', hourly: ['temperature'], raw: ['temperature_a'] },
    { key: 'rh',     label: 'Relative humidity (sensor)', unit: '%', hourly: ['humidity'], raw: ['humidity_a'] },
    { key: 'pres',   label: 'Pressure', unit: 'hPa', hourly: ['pressure'], raw: ['pressure_a'] },
  ];
  const varByKey = new Map(VARS.map((v) => [v.key, v]));
  const COLUMN_LABEL = {
    'pm2.5_epa': 'PM2.5 (EPA)', aqi: 'AQI', 'pm2.5_atm_a': 'Channel A', 'pm2.5_atm_b': 'Channel B',
    'pm10.0_atm': 'PM10', 'pm10.0_atm_a': 'Channel A', 'pm10.0_atm_b': 'Channel B',
    'pm1.0_atm': 'PM1.0', 'pm1.0_atm_a': 'Channel A', 'pm1.0_atm_b': 'Channel B',
    temperature: 'Temperature', temperature_a: 'Temperature', humidity: 'Humidity', humidity_a: 'Humidity',
    pressure: 'Pressure', pressure_a: 'Pressure',
  };

  const DAY = 86400000;
  const RANGES = [
    { key: '1d', label: '24 h', ms: DAY },
    { key: '7d', label: '7 d', ms: 7 * DAY },
    { key: '30d', label: '30 d', ms: 30 * DAY },
    { key: '1y', label: '1 yr', ms: 365 * DAY },
    { key: 'all', label: 'All', ms: Infinity },
  ];
  const rangeByKey = new Map(RANGES.map((r) => [r.key, r]));
  const RAW_MAX_MS = 7 * DAY;               // 2-minute data only for short ranges
  const DEFAULTS = { v: 'pm25', range: '7d', res: 'hourly' };

  /* ── State (URL > localStorage > default, validated — §4) ──────────────── */

  const params = MCO.urlParams();
  function pick(key, valid, fallback) {
    const u = MCO.getParamLower(key, params);
    if (u && valid.has(u)) return u;
    const s = MCO.lsGet(LS(key));
    if (s && valid.has(s)) return s;
    return fallback;
  }
  const state = {
    v: pick('v', varByKey, DEFAULTS.v),
    range: pick('range', rangeByKey, DEFAULTS.range),
    res: pick('res', new Map([['hourly', 1], ['raw', 1]]), DEFAULTS.res),
    station: MCO.getParamLower('station', params) || null,   // validated after data loads
  };

  let manifest = null;
  let latest = [];
  const stationById = new Map();
  const latestById = new Map();
  let chartRows = [];          // what the chart and its twin currently show
  let chartCols = [];
  let chart = null;

  /* ── DOM ───────────────────────────────────────────────────────────────── */

  const $ = (id) => document.getElementById(id);
  const selectEl = $('station-select');
  const sheetEl = $('sheet');
  const chartEl = $('chart');
  const chartStatus = $('chart-status');

  const mapTwin = MCO.srTable({
    container: $('sr-twin'),
    caption: 'Montana Mesonet air quality sensors',
    rowKey: (r) => r.station,
    columns: [
      { label: 'Station', rowHeader: true, value: (r) => `${r.name} (${r.station})` },
      { label: 'NowCast AQI', value: (r) => (r.aqi == null ? 'no recent data' : `${r.aqi}, ${r.aqi_category}`) },
      { label: 'PM2.5 (EPA), µg/m³', value: (r) => fmt(r['pm2.5_epa'], 1) },
      { label: 'As of', value: (r) => (r.time_stamp ? MCO.formatStampMT(Date.parse(r.time_stamp)) : '') },
    ],
    selectable: true,
    onSelect: (r) => openStation(r.station, { fly: true }),
  });

  function fmt(v, d) {
    return v == null || !Number.isFinite(Number(v)) ? '' : Number(v).toFixed(d);
  }

  /* ── Mountain Time on the chart axis ───────────────────────────────────── */
  // ECharts formats a time axis in browser-local time or UTC only. Shift each
  // timestamp by the America/Denver offset and render with useUTC, so axis
  // labels and tooltips read Mountain wall time everywhere (§ all times MT).
  const offsetCache = new Map();
  const mtParts = new Intl.DateTimeFormat('en-US', {
    timeZone: MCO.TZ, hourCycle: 'h23', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  });
  function mtOffset(ms) {
    const h = Math.floor(ms / 3600000);
    let off = offsetCache.get(h);
    if (off === undefined) {
      const p = Object.fromEntries(mtParts.formatToParts(new Date(h * 3600000)).map((x) => [x.type, x.value]));
      off = Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour, +p.minute, +p.second) - h * 3600000;
      offsetCache.set(h, off);
    }
    return off;
  }

  /* ── Parquet + ECharts loaders ─────────────────────────────────────────── */

  const cache = MCO.promiseCache();
  const parquetLib = () => cache.cached('lib', () => import('./vendor/parquet.min.js'));

  function readParquet(file, columns) {
    const url = `${BASE}/${file.path}`;
    return cache.cached(`${url}|${columns.join(',')}`, () => parquetLib().then((pq) =>
      pq.asyncBufferFromUrl({ url, byteLength: file.bytes || undefined }).then((buf) =>
        pq.parquetReadObjects({
          file: buf,
          columns: ['time_stamp', ...columns],
          compressors: { ZSTD: (input) => pq.zstdDecompress(input) },
        }))));
  }

  function loadECharts() {
    return cache.cached('echarts', () => new Promise((resolve, reject) => {
      const s = document.createElement('script');
      s.src = ECHARTS.src;
      s.integrity = ECHARTS.integrity;
      s.crossOrigin = 'anonymous';
      s.onload = () => resolve(window.echarts);
      s.onerror = () => reject(new Error('chart library failed to load'));
      document.head.appendChild(s);
    }));
  }

  /* ── Map ───────────────────────────────────────────────────────────────── */

  let map = null;
  const overlayData = {};

  function stationsFC() {
    return {
      type: 'FeatureCollection',
      features: latest.filter((r) => r.lat != null && r.lon != null).map((r) => ({
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [r.lon, r.lat] },
        properties: {
          id: r.station, name: r.name,
          cat: r.aqi_category || '', aqi: r.aqi == null ? '' : String(r.aqi),
          stale: !!r.stale || r.aqi == null,
        },
      })),
    };
  }

  function fillExpr() {
    const m = ['match', ['get', 'cat']];
    AQI.forEach((c) => m.push(c.key, c.fill));
    m.push('rgba(0,0,0,0)');
    return m;
  }

  function addCustomLayers() {
    if (map.getLayer('boundary_county')) map.setLayoutProperty('boundary_county', 'visibility', 'none');
    MCO.map.addHillshade(map);
    const paints = MCO.map.overlayPaints();
    if (overlayData.counties && !map.getSource('counties')) {
      map.addSource('counties', { type: 'geojson', data: overlayData.counties });
      map.addLayer({ id: 'counties-line', type: 'line', source: 'counties', paint: paints.countiesLine });
    }
    if (overlayData.tribal && !map.getSource('tribal')) {
      map.addSource('tribal', { type: 'geojson', data: overlayData.tribal });
      map.addLayer({ id: 'tribal-fill', type: 'fill', source: 'tribal', paint: paints.tribalFill });
      map.addLayer({ id: 'tribal-line', type: 'line', source: 'tribal', paint: paints.tribalLine });
      map.addLayer({ id: 'tribal-label', type: 'symbol', source: 'tribal', minzoom: 6,
        layout: MCO.map.TRIBAL_LABEL_LAYOUT, paint: paints.tribalLabelPaint });
    }
    if (overlayData.state && !map.getSource('state')) {
      map.addSource('state', { type: 'geojson', data: overlayData.state });
      map.addLayer({ id: 'state-line', type: 'line', source: 'state', paint: paints.stateLine });
    }
    if (!map.getSource('stations')) {
      map.addSource('stations', { type: 'geojson', data: stationsFC() });
      const stroke = MCO.cssVar('--dot-stroke') || MCO.cssVar('--text-primary');
      // Filled by category; no recent AQI = hollow ring (shape, not color, §6).
      map.addLayer({
        id: 'stations-dot', type: 'circle', source: 'stations',
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 4, 9, 10, 13],
          'circle-color': ['case', ['get', 'stale'], 'rgba(0,0,0,0)', fillExpr()],
          'circle-stroke-color': stroke,
          'circle-stroke-width': ['case', ['get', 'stale'], 2.5, 1.5],
        },
      });
      // The AQI number ON the dot: the non-color channel (§6).
      const inkMatch = ['match', ['get', 'cat']];
      AQI.forEach((c) => inkMatch.push(c.key, c.ink));
      inkMatch.push(MCO.cssVar('--text-primary'));
      map.addLayer({
        id: 'stations-aqi', type: 'symbol', source: 'stations',
        filter: ['!', ['get', 'stale']],
        layout: {
          'text-field': ['get', 'aqi'], 'text-font': ['Open Sans Semibold', 'Arial Unicode MS Bold'],
          'text-size': ['interpolate', ['linear'], ['zoom'], 4, 9, 10, 12],
          'text-allow-overlap': true, 'text-ignore-placement': true,
        },
        paint: { 'text-color': inkMatch },
      });
      map.addLayer({ id: 'stations-hit', type: 'circle', source: 'stations', paint: MCO.map.hitPaint({ radius: 14 }) });
      // Selection halo in --selection-ring (read live by MCO.map.selectionPaint).
      map.addLayer({
        id: 'stations-selected', type: 'circle', source: 'stations',
        filter: ['==', ['get', 'id'], state.station || ''],
        paint: MCO.map.selectionPaint({ radius: ['interpolate', ['linear'], ['zoom'], 4, 12, 10, 16] }),
      });
    }
  }

  function renderMap() {
    mapTwin.render(latest, { selected: state.station });
    if (!map) return;
    map.getSource('stations')?.setData(stationsFC());
    map.getLayer('stations-selected') &&
      map.setFilter('stations-selected', ['==', ['get', 'id'], state.station || '']);
  }

  /* ── Legend ────────────────────────────────────────────────────────────── */

  function buildLegend() {
    const rows = $('legend-rows');
    let lo = 0;
    const frag = AQI.map((c) => {
      const row = document.createElement('div');
      row.className = 'legend-row';
      const sw = document.createElement('span');
      sw.className = 'legend-swatch';
      sw.setAttribute('aria-hidden', 'true');
      sw.style.setProperty('--swatch', c.fill);
      const name = document.createElement('span');
      name.textContent = c.key;
      const range = document.createElement('span');
      range.className = 'legend-range';
      range.textContent = `${lo}–${c.max}`;
      lo = c.max + 1;
      row.append(sw, name, range);
      return row;
    });
    const none = document.createElement('div');
    none.className = 'legend-row';
    const ring = document.createElement('span');
    ring.className = 'legend-swatch';
    ring.setAttribute('aria-hidden', 'true');
    const label = document.createElement('span');
    label.textContent = 'No recent data (hollow)';
    none.append(ring, label);
    rows.replaceChildren(...frag, none);
  }

  /* ── Detail sheet ──────────────────────────────────────────────────────── */

  const sheet = MCO.initSheet({
    sheet: sheetEl,
    peekHeight: 'auto',
    fallbackFocus: chartEl,
    onState: (s) => {
      if (s === 'closed') onSheetClosed();
      else if (chart) setTimeout(() => chart && chart.resize(), 260);
    },
  });

  function setFacts(r, s) {
    const facts = [
      ['PM2.5 (EPA)', r && r['pm2.5_epa'] != null ? `${fmt(r['pm2.5_epa'], 1)} µg/m³` : '—'],
      ['NowCast PM2.5', r && r['pm2.5_nowcast'] != null ? `${fmt(r['pm2.5_nowcast'], 1)} µg/m³` : '—'],
      ['Temperature', r && r.temperature != null ? `${fmt(r.temperature, 0)} °F` : '—'],
      ['Humidity', r && r.humidity != null ? `${fmt(r.humidity, 0)} %` : '—'],
      ['Sensor', s && s.sensor_index ? `PurpleAir ${s.sensor_index}` : '—'],
      ['Record from', s && s.data_start ? s.data_start : '—'],
    ];
    $('facts').replaceChildren(...facts.map(([k, v]) => {
      const d = document.createElement('div');
      const dt = document.createElement('dt');
      dt.textContent = k;
      const dd = document.createElement('dd');
      dd.textContent = v;
      d.append(dt, dd);
      return d;
    }));
  }

  function setBadge(r) {
    const badge = $('aqi-badge');
    const c = r && aqiByKey.get(r.aqi_category);
    badge.textContent = r && r.aqi != null ? String(r.aqi) : '–';
    badge.style.setProperty('--aqi-bg', c ? c.fill : '');
    badge.style.setProperty('--aqi-fg', c ? c.ink : '');
    $('aqi-cat').textContent = c ? c.key : 'No recent data';
    const when = r && (r.aqi_time_stamp || r.time_stamp);
    $('aqi-when').textContent = when ? `NowCast for the hour of ${MCO.formatStampMT(Date.parse(when))}` : '';
  }

  let _opener = null;
  function openStation(id, { fly = false, url = 'auto' } = {}) {
    const s = stationById.get(id);
    if (!s) return;
    const wasOpen = sheet.state() !== 'closed';
    state.station = id;
    const r = latestById.get(id);
    $('sheet-title').textContent = s.name;
    setBadge(r);
    setFacts(r, s);
    selectEl.value = id;
    if (!wasOpen) _opener = document.activeElement;
    sheet.open('peek', { opener: _opener });
    MCO.setPageTitle({ short: 'Air Quality', detail: s.name });
    MCO.announce(`${s.name}: ${r && r.aqi != null ? `AQI ${r.aqi}, ${r.aqi_category}` : 'no recent AQI'}.`);
    renderMap();
    if (url === 'auto' && !(history.state && history.state.mcoDetail)) writeUrl({ push: true });
    else if (url !== 'none') writeUrl();
    if (fly && map && s.lat != null) {
      map.flyTo({ center: [s.lon, s.lat], zoom: Math.max(map.getZoom(), 7), animate: !MCO.reducedMotion() });
    }
    drawChart();
  }

  function onSheetClosed() {
    if (!state.station) return;
    state.station = null;
    selectEl.value = '';
    MCO.setPageTitle({ short: 'Air Quality' });
    renderMap();
    if (history.state && history.state.mcoDetail) history.back();
    else writeUrl();
  }

  /* ── Chart ─────────────────────────────────────────────────────────────── */

  function stationEndMs(id) {
    const s = stationById.get(id);
    const r = latestById.get(id);
    if (r && r.time_stamp) return Date.parse(r.time_stamp) + 3600000;
    if (s && s.fetched_through) return Date.parse(`${s.fetched_through}T00:00:00Z`) + DAY;
    return Date.now();
  }

  function effectiveRes(v, range) {
    return state.res === 'raw' && v.raw && range.ms <= RAW_MAX_MS ? 'raw' : 'hourly';
  }

  function filesFor(id, res, startMs, endMs) {
    const list = manifest.files[res] || [];
    return list.filter((f) => {
      if (f.station !== id) return false;
      const a = res === 'raw' ? Date.UTC(f.year, f.month - 1, 1) : Date.UTC(f.year, 0, 1);
      const b = res === 'raw' ? Date.UTC(f.year, f.month, 1) : Date.UTC(f.year + 1, 0, 1);
      return a < endMs && b > startMs;
    });
  }

  function syncControls() {
    const v = varByKey.get(state.v);
    const range = rangeByKey.get(state.range);
    $('var-select').value = state.v;
    document.querySelectorAll('#range-seg [data-range]').forEach((b) =>
      b.setAttribute('aria-pressed', String(b.dataset.range === state.range)));
    const rawOk = !!v.raw && range.ms <= RAW_MAX_MS;
    const resSel = $('res-select');
    resSel.value = effectiveRes(v, range);
    resSel.disabled = !rawOk;
    resSel.title = rawOk ? '' : '2-minute data is available for ranges up to 7 days and uncorrected variables';
  }

  let drawSeq = 0;
  async function drawChart() {
    const id = state.station;
    if (!id) return;
    syncControls();
    const seq = ++drawSeq;
    const v = varByKey.get(state.v);
    const range = rangeByKey.get(state.range);
    const res = effectiveRes(v, range);
    const cols = res === 'raw' ? v.raw : v.hourly;
    const endMs = stationEndMs(id);
    const s = stationById.get(id);
    const firstMs = s && s.data_start ? Date.parse(`${s.data_start}-01T00:00:00Z`) : endMs - 365 * DAY;
    const startMs = Number.isFinite(range.ms) ? Math.max(endMs - range.ms, firstMs) : firstMs;
    const files = filesFor(id, res, startMs, endMs);
    renderFileLinks(files, res);

    chartStatus.textContent = 'Loading…';
    chartEl.setAttribute('aria-busy', 'true');
    let rows;
    try {
      const extra = res === 'hourly' && v.key === 'pm25' ? ['pm2.5_epa_basis'] : [];
      const parts = await Promise.all(files.map((f) => readParquet(f, cols.concat(extra))));
      rows = parts.flat().filter((r) => {
        const t = +r.time_stamp;
        return t >= startMs && t < endMs;
      }).sort((a, b) => a.time_stamp - b.time_stamp);
    } catch (e) {
      if (seq !== drawSeq) return;
      chartEl.removeAttribute('aria-busy');
      chartStatus.textContent = 'The data failed to load.';
      MCO.notice({ tone: 'danger', toneLabel: 'Data error', text: 'The chart data failed to load. Check your connection and try again.',
        container: chartEl.parentElement, action: { label: 'Retry', onClick: drawChart } });
      return;
    }
    if (seq !== drawSeq) return;          // a newer request superseded this one

    chartCols = cols;
    chartRows = rows.map((r) => {
      const o = { t: +r.time_stamp };
      cols.forEach((c) => { o[c] = r[c] == null ? null : Number(r[c]); });
      if (r['pm2.5_epa_basis'] !== undefined) o.basis = r['pm2.5_epa_basis'];
      return o;
    });
    $('basis-note').hidden = !chartRows.some((r) => r.basis === 'atm');

    const span = `${MCO.formatDateMT(startMs)} – ${MCO.formatDateMT(endMs - 1)}`;
    const resLabel = res === 'raw' ? '2-minute' : 'hourly';
    chartStatus.textContent = chartRows.length
      ? `${chartRows.length.toLocaleString()} ${resLabel} values, ${span} (Mountain Time)`
      : `No data for ${span}.`;
    chartEl.setAttribute('aria-label',
      `${v.label}, ${resLabel}, ${span}, ${chartRows.length} values. The data is in the table that follows.`);
    renderTwin(v);
    MCO.announce(`Chart: ${v.label}, ${RANGES.find((x) => x.key === state.range).label}, ${chartRows.length} values.`);

    try {
      const echarts = await loadECharts();
      if (seq !== drawSeq) return;
      paintChart(echarts, v, res);
    } catch (e) {
      chartStatus.textContent = 'The chart library failed to load; the data is still in the table and the CSV.';
    }
    chartEl.removeAttribute('aria-busy');
  }

  function renderTwin(v) {
    const cols = [{ label: 'Time (Mountain)', rowHeader: true, value: (r) => MCO.formatStampMT(r.t) }]
      .concat(chartCols.map((c) => ({ label: `${COLUMN_LABEL[c] || c} (${v.unit})`, value: (r) => fmt(r[c], 1) })));
    // srTable's columns are fixed at creation; rebuild the twin when they change.
    const key = chartCols.join('|') + v.unit;
    if (renderTwin.key !== key) {
      renderTwin.key = key;
      $('chart-twin').replaceChildren();
      renderTwin.table = MCO.srTable({
        container: $('chart-twin'), caption: `Chart data: ${v.label}`, rowKey: (r) => r.t, maxRows: 400,
        overflowText: (n) => `…and ${n} more rows. Use “CSV of this chart” for all of them.`, columns: cols,
      });
    }
    renderTwin.table.render(chartRows);
  }

  function seriesData(col, cadenceMs) {
    const out = [];
    let prev = null;
    for (const r of chartRows) {
      if (prev != null && r.t - prev > cadenceMs * 1.6) out.push([prev + cadenceMs + mtOffset(prev), null]);  // break the line at gaps
      out.push([r.t + mtOffset(r.t), r[col]]);
      prev = r.t;
    }
    return out;
  }

  function paintChart(echarts, v, res) {
    const tok = MCO.chartTokens();
    const theme = MCO.getTheme();
    const colors = MCO.palette.categorical(Math.max(2, chartCols.length), theme);
    const cadence = res === 'raw' ? 120000 : 3600000;
    const zoom = chart && chart.getOption && chart.getOption().dataZoom;
    if (chart) chart.dispose();
    chart = echarts.init(chartEl, null, { renderer: 'canvas' });
    const series = chartCols.map((c, i) => ({
      type: 'line', name: COLUMN_LABEL[c] || c, data: seriesData(c, cadence),
      showSymbol: false, sampling: 'lttb', connectNulls: false,
      lineStyle: { width: 1.5 }, color: colors[i],
      markLine: i === 0 && v.breaks ? {
        silent: true, symbol: 'none',
        lineStyle: { color: tok.grid, type: 'dashed', width: 1 },
        label: { color: tok.textSecondary, fontFamily: tok.fontUi, fontSize: 10, formatter: (p) => p.name, position: 'insideEndTop' },
        data: v.breaks.map((y, k) => ({ yAxis: y, name: AQI[k + 1] ? `${AQI[k + 1].short} above` : '' })),
      } : undefined,
    }));
    chart.setOption({
      animation: !MCO.reducedMotion(),
      useUTC: true,                              // timestamps are pre-shifted to Mountain
      textStyle: { fontFamily: tok.fontUi, color: tok.text },
      grid: { left: 52, right: 16, top: chartCols.length > 1 ? 34 : 20, bottom: 56 },
      legend: chartCols.length > 1 ? { top: 0, textStyle: { color: tok.textSecondary }, inactiveColor: tok.textMuted } : undefined,
      tooltip: {
        trigger: 'axis', backgroundColor: tok.tooltipBg, borderColor: tok.tooltipBorder,
        textStyle: { color: tok.text, fontFamily: tok.fontUi },
        valueFormatter: (y) => (y == null ? '—' : `${Number(y).toFixed(1)} ${v.unit}`),
      },
      xAxis: {
        type: 'time', axisLine: { lineStyle: { color: tok.grid } },
        axisLabel: { color: tok.textSecondary, hideOverlap: true }, splitLine: { show: false },
      },
      yAxis: {
        type: 'value', name: v.unit, nameTextStyle: { color: tok.textSecondary },
        axisLabel: { color: tok.textSecondary }, splitLine: { lineStyle: { color: tok.grid } },
        min: v.key === 'temp' || v.key === 'pres' ? 'dataMin' : 0,
      },
      dataZoom: [
        { type: 'inside', ...(zoom && zoom[0] ? { start: zoom[0].start, end: zoom[0].end } : {}) },
        { type: 'slider', height: 18, bottom: 8, borderColor: tok.grid, textStyle: { color: tok.textSecondary },
          ...(zoom && zoom[1] ? { start: zoom[1].start, end: zoom[1].end } : {}) },
      ],
      series,
    });
  }

  // Theme change: dispose and re-init, carrying zoom (kit guidance for ECharts).
  document.addEventListener('mco:themechange', () => {
    if (chart && window.echarts && state.station) {
      const v = varByKey.get(state.v);
      paintChart(window.echarts, v, effectiveRes(v, rangeByKey.get(state.range)));
    }
  });
  window.addEventListener('resize', () => chart && chart.resize());

  function renderFileLinks(files, res) {
    const ul = $('file-links');
    const items = files.map((f) => {
      const li = document.createElement('li');
      const a = document.createElement('a');
      a.href = MCO.map.safeUrl(`${BASE}/${f.path}`);
      a.textContent = `${f.path.split('/').pop()} (${res === 'raw' ? '2-minute' : 'hourly'} Parquet)`;
      a.rel = 'noopener';
      li.appendChild(a);
      return li;
    });
    const all = document.createElement('li');
    const a = document.createElement('a');
    a.href = `${BASE}/README.md`;
    a.target = '_blank';
    a.rel = 'noopener noreferrer';
    a.textContent = 'All files, schema and how to read them';
    all.appendChild(a);
    ul.replaceChildren(...items, all);
  }

  $('btn-csv').addEventListener('click', () => {
    if (!chartRows.length) { MCO.showToast('Nothing to download for this range.'); return; }
    const head = ['time_utc', 'time_mountain', ...chartCols];
    const lines = [head.join(',')].concat(chartRows.map((r) => [
      new Date(r.t).toISOString(), `"${MCO.formatStampMT(r.t)}"`, ...chartCols.map((c) => (r[c] == null ? '' : r[c])),
    ].join(',')));
    const blob = new Blob([lines.join('\n') + '\n'], { type: 'text/csv' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `${state.station}_${state.v}_${state.range}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    MCO.announce('CSV downloaded.');
  });

  /* ── Controls ──────────────────────────────────────────────────────────── */

  const varSel = $('var-select');
  varSel.append(...VARS.map((v) => {
    const o = document.createElement('option');
    o.value = v.key;
    o.textContent = v.label;
    return o;
  }));
  varSel.addEventListener('change', () => { state.v = varSel.value; MCO.lsSet(LS('v'), state.v); writeUrl(); drawChart(); });

  $('range-seg').append(...RANGES.map((r) => {
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'nav-btn seg-btn';
    b.dataset.range = r.key;
    b.textContent = r.label;
    b.setAttribute('aria-pressed', 'false');
    b.addEventListener('click', () => {
      state.range = r.key;
      MCO.lsSet(LS('range'), state.range);
      writeUrl();
      drawChart();
    });
    return b;
  }));

  $('res-select').addEventListener('change', (e) => {
    state.res = e.target.value;
    MCO.lsSet(LS('res'), state.res);
    writeUrl();
    drawChart();
  });

  selectEl.addEventListener('change', () => { if (selectEl.value) openStation(selectEl.value, { fly: true }); });

  /* ── URL state (§4): clean at defaults; push only for the drill-down ───── */

  function writeUrl({ push = false } = {}) {
    const p = {};
    if (state.station) p.station = state.station;
    if (state.v !== DEFAULTS.v) p.v = state.v;
    if (state.range !== DEFAULTS.range) p.range = state.range;
    if (state.res !== DEFAULTS.res) p.res = state.res;
    const theme = MCO.getTheme();
    if (theme !== MCO.osTheme()) p.theme = theme;
    if (params.get('kbd') === 'off') p.kbd = 'off';
    if (DEV) p['dev-data'] = '';
    if (map) Object.assign(p, MCO.map.cameraParamsIfDefault(map));
    if (push) MCO.pushUrlState(p, { state: { mcoDetail: state.station } });
    else MCO.replaceUrlState(p);
  }

  MCO.onUrlState((p) => {
    const id = p.get('station');
    if (id && stationById.has(id)) {
      if (id !== state.station) openStation(id, { url: 'none' });
    } else if (state.station) {
      state.station = null;              // onSheetClosed must not touch history
      sheet.close();
      selectEl.value = '';
      renderMap();
    }
  });

  /* ── Chrome: theme, share, legend, info ────────────────────────────────── */

  MCO.initThemeToggle({
    button: $('btn-theme'), iconSun: $('icon-sun'), iconMoon: $('icon-moon'), cycle: true,
    onChange: () => {
      if (!map) return;
      map.setStyle(MCO.map.cartoStyleUrl());   // style.load re-adds the layers
      writeUrl();
    },
  });

  $('btn-share').addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(location.href);
      MCO.showToast('Link copied. It opens exactly this view.');
    } catch (e) {
      MCO.showToast('Copy failed. The address bar URL is the share link.', 4000);
    }
  });

  MCO.initCollapsible({
    toggle: $('legend-toggle'), body: $('legend-body'),
    storageKey: LS('legend'), autoCollapseOnCompact: true,
  });
  buildLegend();
  MCO.initInfoModal({ dialog: $('info-modal'), trigger: $('btn-info') });
  $('credit').textContent = MCO.credit({ source: 'PurpleAir sensors, Montana Mesonet' });

  /* ── Boot ──────────────────────────────────────────────────────────────── */

  const loader = MCO.loading($('main'), { label: 'Loading air quality data', place: 'over' });

  function loadData() {
    loader.start();
    return Promise.all([
      MCO.fetchJSON(`${BASE}/manifest.json`, { timeoutMs: 30000 }),
      MCO.fetchJSON(`${BASE}/latest/latest.json`, { timeoutMs: 30000 }),
      MCO.fetchJSON('data/mt_state_simple.geojson'),
      MCO.fetchJSON('data/mt_counties_simple.geojson'),
      MCO.fetchJSON('data/mt_reservations_simple.geojson'),
    ]).then(([man, lat, st, co, tr]) => {
      manifest = man;
      latest = (lat.stations || []).slice().sort((a, b) => String(a.name).localeCompare(String(b.name)));
      overlayData.state = st;
      overlayData.counties = co;
      overlayData.tribal = tr;
      stationById.clear();
      man.stations.forEach((s) => stationById.set(s.id, s));
      latestById.clear();
      latest.forEach((r) => latestById.set(r.station, r));

      selectEl.replaceChildren(selectEl.options[0] || new Option('Open a station…', ''));
      selectEl.append(...latest.map((r) => new Option(
        `${r.name} (${r.aqi == null ? 'no recent data' : `AQI ${r.aqi}`})`, r.station)));

      const stamp = lat.generated_at ? MCO.formatStampMT(Date.parse(lat.generated_at)) : '';
      $('data-stamp').textContent = stamp ? `updated ${stamp}` : 'updated nightly';
      loader.done();
      return true;
    }).catch(() => {
      loader.fail('The air quality data failed to load.', { retry: () => loadData().then(onData) });
      return false;
    });
  }

  function onData(ok) {
    if (!ok || !map) return;
    addCustomLayers();
    renderMap();
    MCO.ready();
    const deep = state.station && stationById.has(state.station);
    if (deep) openStation(state.station, { fly: !params.has('lng'), url: 'replace' });
    else state.station = null;
    // First-visit intro, never over a deep link or an active user (§4).
    const hasDeepLink = ['station', 'v', 'range', 'lng'].some((k) => params.has(k));
    if (!MCO.lsGet(LS('seen-intro')) && !hasDeepLink) {
      setTimeout(() => {
        const dlg = $('info-modal');
        const busy = document.activeElement && document.activeElement !== document.body;
        if (!dlg.open && !busy) dlg.showModal();
        MCO.lsSet(LS('seen-intro'), '1');
      }, 350);
    }
  }

  const dataReady = loadData();

  MCO.map.loadMapLibre().then((maplibregl) => {
    map = new maplibregl.Map({ container: 'map', style: MCO.map.cartoStyleUrl(), ...MCO.map.initialCamera(params) });
    MCO.map.addNavigation(map);
    MCO.map.watchBasemap(map);
    MCO.map.addFitControl(map);
    const zoomFloor = MCO.map.installZoomFloor(map);
    MCO.map.initCursorTooltip(map, {
      element: $('tooltip'),
      layers: ['stations-hit'],
      render: (f) => {
        const r = latestById.get(f.properties.id);
        return {
          name: f.properties.name,
          sub: r && r.aqi != null ? `AQI ${r.aqi} · ${r.aqi_category}` : 'No recent data',
        };
      },
    });
    map.on('click', 'stations-hit', (e) => {
      const f = e.features && e.features[0];
      if (f) openStation(f.properties.id);
    });
    map.on('moveend', () => writeUrl());
    map.on('style.load', () => { if (manifest) { addCustomLayers(); renderMap(); } });
    map.on('load', () => {
      zoomFloor.refresh();
      dataReady.then(onData);
    });
  }, () => {
    MCO.notice({ tone: 'danger', text: 'The map library failed to load. Check your connection and reload.' });
    // The data still loads: the station picker, sheet and chart work without the map.
    dataReady.then((ok) => {
      if (!ok) return;
      renderMap();
      MCO.ready();
      if (state.station && stationById.has(state.station)) openStation(state.station, { url: 'replace' });
    });
  });
})();
