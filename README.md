# mesonet-aq

This repository archives data nightly from the PurpleAir air-quality sensors at Montana Mesonet stations.
It also publishes derived products:

- hourly summaries,
- EPA-corrected PM2.5, NowCast and AQI,
- a latest snapshot.

Everything is served through the Montana Climate Office data CDN.

- **Data:** <https://data2.climate.umt.edu/mesonet/air-quality/>. Start at
  [`manifest.json`](https://data2.climate.umt.edu/mesonet/air-quality/manifest.json).
  The public data README is at
  [`README.md`](https://data2.climate.umt.edu/mesonet/air-quality/README.md); its source is
  [`src/mesonet_aq/PUBLIC_README.md`](src/mesonet_aq/PUBLIC_README.md).
- **Explorer:** <https://mesonet.climate.umt.edu/air/>. The source is [`docs/`](docs/).

## Layout

```
air-quality/
  README.md  manifest.json  stations.parquet  stations.geojson
  latest/latest.json  latest/latest.parquet
  raw/station=<id>/year=YYYY/<id>_YYYY-MM.parquet     ~2-min A/B readings, one file per station-month
  hourly/station=<id>/year=YYYY/<id>_YYYY.parquet     hourly + EPA PM2.5 + NowCast/AQI, one per station-year
```

The bucket (`mco-mesonet`) is private and cannot be listed. It is readable only through CloudFront (OAC).
Clients enumerate files from `manifest.json`.

Every file has one fixed Arrow schema ([`schema.py`](src/mesonet_aq/schema.py)), so multi-file reads need no type fixes.

Cache policy:

| Files | `Cache-Control` |
|---|---|
| Closed months and years | `max-age=86400`; invalidated if they are ever rewritten |
| Current month and year, indexes | `max-age=300` |

The R example is [`read-archive.R`](read-archive.R).

## How it runs

```
EventBridge Scheduler (01:30 America/Denver)
  → ECS Fargate task `mesonet-aq` (image: ECR mesonet-aq:latest, `mesonet-aq run`)
      Airtable Deployments  → which sensor is at which station since when
      PurpleAir history API → each UTC day after the station's `fetched_through`
      → merge into raw station-months → rebuild hourly station-years
      → stations.*, latest/*, manifest.json (written last) → CloudFront invalidation
      → CloudWatch MesonetAQ/RunSucceeded (dead-man alarm → mco-ops-alerts)
```

| Concern | Where |
|---|---|
| Pipeline code | [`src/mesonet_aq/`](src/mesonet_aq/), with tests in [`tests/`](tests/) |
| Infrastructure | [`terraform/`](terraform/): ECR, ECS, schedule, task + CI roles, secrets, alarm |
| Bucket and lifecycle | `mco-aws` `stacks/mco-mesonet-bucket` (not here) |
| CDN | `mco-data-cdn` (not here) |
| Explorer hosting | GitHub Pages `/docs`, proxied at `/air` by `mesonet-gateway` |

### Commands

```sh
mesonet-aq run [--since YYYY-MM-DD] [--station ID]   # nightly; --since re-fetches (repair/backfill)
mesonet-aq migrate [--station ID]                    # one-shot: legacy station=/date= tree → this layout
mesonet-aq rebuild-derived                           # recompute hourly/latest/manifest from raw (no API calls)
```

The `run` command handles failures this way:

- **Resuming:** each station resumes from its `fetched_through` in the manifest.
- **Fetch failure:** if a fetch fails, that station stops at the gap and the run exits non-zero. The next run fills the gap.
- **No manifest yet:** `run` refuses to start while legacy files exist and no manifest has been written.

Run a command on Fargate with the **run-task** workflow (Actions → run-task).

### Local development

```sh
uv sync
uv run pytest
cp .env.example .env    # add PURPLEAIR_API_KEY / AIRTABLE_TOKEN
set -a; . ./.env; set +a
AQ_STORE=/tmp/aq uv run mesonet-aq run --station acebirne --since 2026-10-01   # writes to a local dir
```

## Deploying

- **Code:** merge to `main`. `build-push.yml` pushes `:latest` and `:<sha>`, and the next scheduled run uses the new image. No Terraform is needed.
- **Infrastructure:** applies are manual and local (mco-aws ADR 0006):

  ```sh
  cd terraform
  export AWS_PROFILE=mco
  terraform init
  terraform plan -out=tfplan     # read it: expect 0 to destroy, 0 to replace
  terraform apply tfplan
  ```

  After the first apply, finish setup:
  1. Set the secret values with `aws secretsmanager put-secret-value`; see the header of [`terraform/secrets.tf`](terraform/secrets.tf).
  2. Set these repo Actions **variables** from the Terraform outputs: `AWS_ROLE_ARN`, `ECS_CLUSTER`, `ECS_TASK_FAMILY` and `ECS_NETWORK`.

## Requirements

Python ≥ 3.12 and [uv](https://docs.astral.sh/uv/). Terraform ≥ 1.10.

## License

MIT
