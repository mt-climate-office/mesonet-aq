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
