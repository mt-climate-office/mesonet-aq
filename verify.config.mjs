// verify.config.mjs — config for mco-web-style's tools/verify/ harness.
// Run from a kit checkout beside this repo:
//   node tools/verify/head.mjs       --root ../mesonet-aq --page docs/index.html
//   node tools/verify/axe-matrix.mjs --root ../mesonet-aq --config ../mesonet-aq/verify.config.mjs
//   node tools/verify/keyboard.mjs   --root ../mesonet-aq --config ../mesonet-aq/verify.config.mjs
//
// Data comes from the live CDN. Before the archive exists there (or to test
// a pipeline change), point the page at a local dry run instead: copy
// `AQ_STORE=… mesonet-aq migrate|run` output to docs/dev-data/air-quality
// (gitignored) and run with AQ_DEV=1 — the page reads ./dev-data on
// localhost only.
//
// Render evidence is the map's screen-reader twin (one row per station) and,
// for the station scenario, the chart twin — never networkidle.

const DEV = process.env.AQ_DEV ? 'dev-data&' : '';
const STATION = process.env.AQ_STATION || 'mcolubre';
const stations = () => document.querySelectorAll('#sr-twin tbody tr').length > 0;

export default {
  root: '.',
  page: 'docs/index.html',
  storage: { 'mco-air-seen-intro': '1' },
  scenarios: [
    { name: 'default', query: `?${DEV}`, ready: stations },
    {
      name: 'station', query: `?${DEV}station=${STATION}`,
      ready: () => document.querySelectorAll('#sr-twin tbody tr').length > 0
        && document.querySelectorAll('#chart-twin tbody tr').length > 0
        && !!document.querySelector('#chart canvas'),
    },
    {
      name: 'raw', query: `?${DEV}station=${STATION}&v=pm25ab&range=1d&res=raw`,
      ready: () => document.querySelectorAll('#chart-twin tbody tr').length > 0,
    },
  ],
  exemptTargets: '',
  allowProblems: [],
  dialogOpener: '.mco-btn-info',
  shortcuts: [],
};
