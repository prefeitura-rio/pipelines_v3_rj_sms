---
name: migrate-v1-to-v3
description: Migrates Prefect flows from v1 to v3. Use when user asks to migrate a Prefect flow, which are folders containing Python scripts such as flows.py and schedules.py.
---

# Migrate v1 to v3

## Instructions

### 1. Navigate to v1 flow
The flow's folder should be inside `pipelines_rj_sms/` (v1), under `pipelines/`.
Try to find the flow (eg. by looking for 'flows.py') and navigate to it; if you can't, ask user for path.

### 2. Create folder for v3
The migration is to a sibling directory of (v1), `pipelines_v3_rj_sms` (v3).
Check that you can find the (v3) directory. It is outside (v1). If you can't find it, inform user and halt.

Create a folder in (v3) with the same path as in (v1).
Eg.: for `pipelines_rj_sms/pipelines/datalake/transform/dbt/`, you'd create
`pipelines_v3_rj_sms/pipelines/datalake/transform/dbt/`.

Check that it was created. If it wasn't, inform user and halt.

### 3. Migrate files
Migrate all files inside the flow's folder, starting with `flows.py`.
They will require syntax changes. Check other flows inside (v3) to understand changes needed.
Also consider:

- 2 spaces to indent, not tabs/4 spaces.
- `_flows` is required in `flows.py` files. It contains `flow_config()`s.
  - In `flow_config()`s, never use `memory` or `dockerfile` parameters.
  - Leave a comment in (v3) with the value of `num_workers=`, `memory_limit=` and `memory_request=` in (v1).
- `flows.py` files cannot have a `constants`; those must be renamed when imported
  (eg. `from pipelines.constants import constants as global_consts`).
- In each flow parameter in (v3), copy their default values and leave a comment indicating if they had required=True in (v1).
- Keep all comments, but ignore `# pylint` comments.
- Instead of `datetime.now(pytz.timezone("America/Sao_Paulo"))`, use `now()` via
  `from pipelines.utils.datetime import now`.
- In `flows.py`, on parallel uploads to datalake, add `rate_limit("um-por-segundo")` inside `for`s
  before `upload_xxx.submit()`.
- In `schedules.py`, with weekly schedules (`interval=timedelta(days=7)`), use English names
  for weekdays (eg. `config={"weekday": "sunday", ...}`)

### 4. Catalog it
The file `localrun.cases.yaml` at the root of (v3) lists all flows. Add a new entry for the migrated flow
under the correct section ("Reports", "Extract/Load", etc) according to flow path.
- `case_slug` is made up;
- `flow_path` is the path with `.` separators;
- `flow_name` is the function name;
- `params` is a list of its parameters. For params without defaults, leave them commented, following the pattern.
