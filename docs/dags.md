# DAGs

DAGs are config-driven: each DAG is a factory function in the comet package, instantiated from a YAML entry in the DAGs bucket.

## Bucket layout

```
s3://<stackname>-airflow-dags/
  dags.py     # dag-processor entry point
  dags.yaml   # one entry per DAG instance
```

Both files come down in the same bundle snapshot, so `dags.py` reads its sibling `dags.yaml` from local disk. The bundle refreshes every 60 seconds. The bucket is versioned, so a bad push is recoverable via S3 object versions.

## Add a DAG instance

Append an entry to `dags.yaml` and re-upload:

```yaml
dags:
  - dag_id: my_dag
    factory: comet.dags.my_dag.create_my_dag
    params:
      start_date: 2026-01-01
      deadline_minutes: 90
      bucket_name: comet-dev-s3-data
```

```bash
aws s3 cp dags.yaml s3://<stackname>-airflow-dags/dags.yaml
```

Set `enabled: false` on an entry to skip it without removing it.

## Writing a factory

A factory is a plain function in the `comet.dags` package with the signature `(dag_id: str, params: <BaseDagParams subclass>) -> DAG`. The loader discovers the params class from the type annotation; no decorator or registration. `BaseDagParams` provides `start_date`, `end_date`, `catchup`, `tags`, `max_active_runs`, and `deadline_minutes`, with `extra='forbid'` so a typo in the YAML fails at parse time.

```python
# src/comet/dags/my_dag.py
from __future__ import annotations
from airflow import DAG
from comet.airflow import BaseDagParams
from comet.airflow.notifications import alert_kwargs


class MyDagParams(BaseDagParams):
    bucket_name: str


def create_my_dag(dag_id: str, params: MyDagParams) -> DAG:
    with DAG(dag_id=dag_id, **params.dag_kwargs(), **alert_kwargs(params.deadline_minutes)) as dag:
        ...
    return dag
```

The reference implementation is `src/comet/dags/ror_ingest.py`.

`deadline_minutes` defaults to 90 and starts when the DAG run is queued. `alert_kwargs(...)` adds task-failure and deadline notifications. For status messages, add a `slack_notifier(...)` success callback to a task; skipped tasks do not notify. See `src/comet/dags/ror_ingest.py` for an example.

## Failure behaviour

- The loader attempts every entry. If any fail (bad factory path, params validation error, exception in the factory, duplicate `dag_id`), it raises one `DagLoadError` at the end wrapping every failure, each annotated with its `dag_id` and factory. All the errors show up together as a single import error in the Airflow UI rather than just the first one.
- Because the module fails to parse, one bad entry takes all the DAGs down. Fix the entry or set `enabled: false` on it.
- Malformed YAML or unknown top-level keys fail immediately.
- A missing `dags.yaml` logs a warning and parses green; the DAGs appear once the next refresh delivers it.
- The loader uses Airflow's parsing context, so running a single task only builds that DAG's entry — a broken entry elsewhere in the YAML doesn't stop in-flight tasks of other DAGs.

## Worker sizing

Each Airflow task runs as a Fargate worker (1 vCPU / 2 GiB memory / 20 GiB disk by default). CPU and memory can be overridden per task:

```python
from airflow.sdk import task


@task(executor_config={"overrides": {"cpu": "2048", "memory": "4096"}})
def heavy_task(): ...
```

Ephemeral storage can't be overridden at runtime. Jobs that need more than 20 GiB of scratch belong on AWS Batch.
