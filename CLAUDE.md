# mesonet-aq

Nightly PurpleAir archive (`src/mesonet_aq/`, Fargate via `terraform/`) and the Air Quality explorer (`docs/`).

## Data contract: read before changing `layout.py` or `schema.py`

- Published keys, schemas and cache policy are a public contract. Readers include the explorer, R/Python
  users and `read-archive.R`.
  - A breaking change bumps `meta.SCHEMA_VERSION` and is documented in `PUBLIC_README.md`.
  - Never change a column type in place.
- Writes are put-overwrite only. Never delete-then-write: CDN readers would see a gap.
- `manifest.json` is written LAST. It is the commit point.
- CloudFront invalidation paths are cache keys: `/air-quality/...`, never `/mesonet/air-quality/...`.
  The wrong form reports Completed and flushes nothing (mesonet-db-rds #167).
- The bucket `mco-mesonet` is private and owned by mco-aws `stacks/mco-mesonet-bucket`, which also holds the ONE
  lifecycle document. Do not declare bucket resources here.
- All pipeline dates are UTC days. The explorer renders Mountain Time.

## Deploying

- **Code:** merging to `main` pushes the image; the next 01:30 MT run uses it.
- **Terraform:** `export AWS_PROFILE=mco`, then `plan -out`, read the plan (0 destroy, 0 replace), then apply that plan.
  - Never put a `profile` in .tf files, and never add `common_tags`.
- **Done:** a step is done when its output has been observed. That means the CloudWatch log group
  `/ecs/mesonet-aq`, a `curl -I` of a data2 URL, and the `MesonetAQ/RunSucceeded` alarm in OK.
- **Repairs:** a missed night needs no action, because the next run resumes from `fetched_through`.
  For repairs, use run-task.yml `run --since`.

## Verification

`uv run ruff check src tests && uv run pytest`; `terraform -chdir=terraform validate`.

## Explorer (`docs/`)

This app consumes mco-web-style (pinned + SRI in index.html). Follow its AGENTS.md:
- tokens only (no raw hexes);
- `--accent` is fill-only;
- `aria-pressed` drives toggle styling;
- canvas data needs a live region and an sr-only table twin.

To change shared styling, change the kit and bump the pinned version here. Never patch a local copy.

App-local rules:
- **AQI colors** are the EPA/AirNow standard. The `AQI` table in `app.js` is a documented `kit-override` role (Kyle, 2026-10-10). Color is never the only channel: the AQI number is printed on each dot, and category names appear in the legend, tooltip, sheet and sr-table.
- **Parquet** is read with `docs/vendor/parquet.min.js` (hyparquet and fzstd), built by `scripts/vendor.sh`.
  - Never hand-edit it.
  - To upgrade, bump the versions in the script and re-run it.
- **ECharts** is lazy-loaded from jsDelivr with SRI. Bumping it means recomputing the `integrity` in `app.js`.
- **CSP:** after any edit to the inline anti-flash script, recompute its sha256 (mco-web-style MIGRATING.md § Gotchas).
- **Times:** the chart pre-shifts timestamps to America/Denver and renders with `useUTC`, so axes read Mountain time.
- **`?dev-data`** (localhost only) reads `docs/dev-data/air-quality`, a local pipeline dry run (gitignored).

**Deploying:** pushing `main` IS a production deploy (GitHub Pages `/docs`, served at mesonet.climate.umt.edu/air/).

Before you push, run these from a kit checkout beside this repo:

```sh
node --check docs/app.js
npx html-validate@9 docs/index.html
node tools/conformance.mjs ../mesonet-aq docs/index.html
node tools/verify/head.mjs --root ../mesonet-aq --page docs/index.html
node tools/verify/axe-matrix.mjs --root ../mesonet-aq --config ../mesonet-aq/verify.config.mjs
node tools/verify/keyboard.mjs --root ../mesonet-aq --config ../mesonet-aq/verify.config.mjs
```

Set `AQ_DEV=1` on the last two to test against `docs/dev-data`.
