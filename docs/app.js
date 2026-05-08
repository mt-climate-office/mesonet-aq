import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

const MANIFEST_URL = './manifest.json';

const VARIABLES = [
  ['pm2.5_atm_a',             'PM2.5 µg/m³ (sensor A)'],
  ['pm2.5_atm_b',             'PM2.5 µg/m³ (sensor B)'],
  ['pm10.0_atm_a',            'PM10 µg/m³ (A)'],
  ['pm10.0_atm_b',            'PM10 µg/m³ (B)'],
  ['pm1.0_atm_a',             'PM1.0 µg/m³ (A)'],
  ['pm1.0_atm_b',             'PM1.0 µg/m³ (B)'],
  ['temperature_a',           'Temperature °F (A)'],
  ['temperature_b',           'Temperature °F (B)'],
  ['humidity_a',              'Humidity % (A)'],
  ['humidity_b',              'Humidity % (B)'],
  ['pressure_a',              'Pressure mbar (A)'],
  ['pressure_b',              'Pressure mbar (B)'],
  ['voc_a',                   'VOC (A)'],
  ['voc_b',                   'VOC (B)'],
  ['scattering_coefficient_a','Scattering coef (A)'],
  ['deciviews_a',             'Deciviews (A)'],
  ['visual_range_a',          'Visual range (A)'],
];

const state = {
  manifest: null,
  stations: {},
  db: null,
  conn: null,
  map: null,
  markers: {},
  selectedStation: null,
  rows: [],
  dataTable: null,
  chart: null,
};

const $ = (id) => document.getElementById(id);

function setStatus(text, isError = false) {
  const el = $('status');
  el.textContent = text;
  el.classList.toggle('error', isError);
}

function setInfo(text) {
  $('info').textContent = text;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => (
    { '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]
  ));
}

async function initDuckDB() {
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  );
  const worker = new Worker(workerUrl);
  state.db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING), worker);
  await state.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);
  state.conn = await state.db.connect();
}

function initMap() {
  state.map = new maplibregl.Map({
    container: 'map',
    style: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
    center: [-110, 47],
    zoom: 5,
  });
  state.map.addControl(new maplibregl.NavigationControl(), 'top-right');
  state.map.addControl(new maplibregl.ScaleControl({ unit: 'metric' }), 'bottom-left');
}

function plotStations() {
  const bounds = new maplibregl.LngLatBounds();
  let any = false;
  for (const s of state.manifest.stations) {
    if (s.lat == null || s.lon == null) continue;
    const el = document.createElement('div');
    el.className = 'station-marker';
    if (!s.dates || s.dates.length === 0) el.classList.add('no-data');
    el.title = `${s.name} (${s.id})`;

    const popupHtml = `
      <strong>${escapeHtml(s.name)}</strong>
      <div style="opacity:0.7">${escapeHtml(s.id)}</div>
      <div style="opacity:0.7;margin-top:0.25rem">${(s.dates || []).length} day(s) of data</div>
    `;
    const marker = new maplibregl.Marker({ element: el })
      .setLngLat([s.lon, s.lat])
      .setPopup(new maplibregl.Popup({ offset: 14, closeButton: false }).setHTML(popupHtml))
      .addTo(state.map);
    el.addEventListener('click', (e) => {
      e.stopPropagation();
      selectStation(s.id);
    });
    state.markers[s.id] = marker;
    bounds.extend([s.lon, s.lat]);
    any = true;
  }
  if (any) state.map.fitBounds(bounds, { padding: 50, maxZoom: 9, duration: 0 });
}

function populateStationSelect() {
  const sel = $('station-select');
  sel.innerHTML = '';
  for (const s of state.manifest.stations) {
    const opt = document.createElement('option');
    opt.value = s.id;
    const tag = (s.dates && s.dates.length) ? '' : ' — no data';
    opt.textContent = `${s.name} (${s.id})${tag}`;
    sel.appendChild(opt);
  }
  sel.addEventListener('change', () => selectStation(sel.value));
}

function populateVariableSelect() {
  const sel = $('variable-select');
  for (const [v, label] of VARIABLES) {
    const opt = document.createElement('option');
    opt.value = v;
    opt.textContent = label;
    sel.appendChild(opt);
  }
  sel.addEventListener('change', () => updateChart());
}

function selectStation(id) {
  const s = state.stations[id];
  if (!s) return;
  state.selectedStation = id;
  $('station-select').value = id;

  for (const [sid, marker] of Object.entries(state.markers)) {
    marker.getElement().classList.toggle('selected', sid === id);
  }
  if (s.lat != null && s.lon != null) {
    state.map.flyTo({ center: [s.lon, s.lat], zoom: 9 });
  }

  if (!s.dates || s.dates.length === 0) {
    $('start-date').value = '';
    $('end-date').value = '';
    state.rows = [];
    updateChart();
    updateTable();
    setStatus(`${s.name}: no data yet`);
    setInfo('');
    return;
  }

  const last = s.dates[s.dates.length - 1];
  const startIdx = Math.max(0, s.dates.length - 7);
  const start = s.dates[startIdx];
  $('start-date').value = start;
  $('start-date').min = s.dates[0];
  $('start-date').max = last;
  $('end-date').value = last;
  $('end-date').min = s.dates[0];
  $('end-date').max = last;

  loadData();
}

async function loadData() {
  const s = state.stations[state.selectedStation];
  if (!s) return;
  const start = $('start-date').value;
  const end = $('end-date').value;
  const dates = (s.dates || []).filter(d => d >= start && d <= end);
  if (dates.length === 0) {
    state.rows = [];
    updateChart();
    updateTable();
    setStatus('No data in selected range');
    setInfo('');
    return;
  }

  setStatus(`Loading ${dates.length} day(s) for ${s.id}…`);
  const base = state.manifest.bucket_base.replace(/\/+$/, '');
  const urls = dates.map(d => `${base}/station=${s.id}/date=${d}/${s.id}_${d}.parquet`);
  const sqlList = urls.map(u => `'${u.replace(/'/g, "''")}'`).join(', ');

  const sql = `
    SELECT
      * EXCLUDE (time_stamp),
      epoch_ms(time_stamp)::BIGINT  AS _ts_ms,
      strftime(time_stamp, '%Y-%m-%d %H:%M:%S') AS time_stamp
    FROM read_parquet([${sqlList}], union_by_name = true)
    ORDER BY _ts_ms
  `;

  const t0 = performance.now();
  try {
    const result = await state.conn.query(sql);
    state.rows = result.toArray().map(row => normalizeRow(row.toJSON()));
    const dt = ((performance.now() - t0) / 1000).toFixed(2);
    setStatus(`Loaded ${state.rows.length.toLocaleString()} rows from ${s.id} in ${dt}s`);
    setInfo(`${dates.length} file(s) · ${state.rows.length.toLocaleString()} rows\n${start} → ${end}`);
  } catch (e) {
    console.error(e);
    state.rows = [];
    const msg = String(e?.message || e);
    if (/403|forbidden/i.test(msg)) {
      setStatus('S3 returned 403 — bucket read access not enabled. See README.', true);
    } else {
      setStatus(`Query failed: ${msg.slice(0, 200)}`, true);
    }
    setInfo('');
  }
  updateChart();
  updateTable();
}

function normalizeRow(o) {
  for (const k of Object.keys(o)) {
    if (typeof o[k] === 'bigint') o[k] = Number(o[k]);
  }
  return o;
}

function updateChart() {
  const variable = $('variable-select').value;
  const ctx = $('chart');
  if (state.chart) state.chart.destroy();
  if (!state.rows.length) return;

  const data = state.rows
    .map(r => ({ x: r._ts_ms, y: r[variable] == null ? null : Number(r[variable]) }))
    .filter(p => p.x != null);

  state.chart = new Chart(ctx, {
    type: 'line',
    data: {
      datasets: [{
        label: variable,
        data,
        borderColor: '#2271b1',
        backgroundColor: 'rgba(34, 113, 177, 0.08)',
        pointRadius: 0,
        borderWidth: 1.4,
        spanGaps: false,
      }],
    },
    options: {
      animation: false,
      maintainAspectRatio: false,
      responsive: true,
      parsing: false,
      scales: {
        x: {
          type: 'linear',
          ticks: {
            maxTicksLimit: 8,
            callback: v => new Date(v).toLocaleString(undefined, {
              month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
            }),
          },
        },
        y: { beginAtZero: false },
      },
      plugins: {
        legend: { display: true, position: 'top', align: 'end' },
        tooltip: {
          callbacks: {
            title: items => new Date(items[0].parsed.x).toLocaleString(),
            label: item => `${variable}: ${item.parsed.y}`,
          },
        },
      },
    },
  });
}

function updateTable() {
  if (state.dataTable) {
    state.dataTable.destroy();
    $('data-table').innerHTML = '';
  }
  if (!state.rows.length) return;

  const cols = Object.keys(state.rows[0])
    .filter(k => k !== '_ts_ms')
    .map(k => ({
      title: k,
      data: k,
      render: (v) => formatCell(v),
    }));

  state.dataTable = new DataTable('#data-table', {
    data: state.rows,
    columns: cols,
    pageLength: 50,
    deferRender: true,
    scrollX: true,
    order: [],
  });
}

function formatCell(v) {
  if (v == null) return '';
  if (typeof v === 'number') {
    return Number.isInteger(v) ? String(v) : v.toFixed(2);
  }
  return String(v);
}

async function main() {
  initMap();
  populateVariableSelect();

  setStatus('Loading manifest…');
  let manifest;
  try {
    const r = await fetch(MANIFEST_URL, { cache: 'no-store' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    manifest = await r.json();
  } catch (e) {
    setStatus(`Could not load manifest.json (${e.message}). The nightly action generates this file.`, true);
    return;
  }
  if (!manifest?.stations?.length) {
    setStatus('Manifest is empty — no stations registered yet.');
    return;
  }
  state.manifest = manifest;
  state.stations = Object.fromEntries(manifest.stations.map(s => [s.id, s]));

  populateStationSelect();
  plotStations();

  setStatus('Initializing DuckDB-WASM…');
  try {
    await initDuckDB();
  } catch (e) {
    setStatus(`DuckDB init failed: ${e.message}`, true);
    return;
  }

  $('load-btn').addEventListener('click', () => loadData());

  const first = manifest.stations.find(s => s.dates && s.dates.length) || manifest.stations[0];
  if (first) {
    selectStation(first.id);
  } else {
    setStatus('No stations to display.');
  }
}

main().catch(e => {
  console.error(e);
  setStatus(`Fatal: ${e.message}`, true);
});
