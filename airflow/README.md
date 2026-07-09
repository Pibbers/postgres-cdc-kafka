Airflow has been added to the stack for orchestration.

To start the services:

```bash
./scripts/start.sh
```

Then open the Airflow UI at:

```text
http://localhost:8082
```

The sample DAG is defined in [airflow/dags/cdc_demo_dag.py](airflow/dags/cdc_demo_dag.py).
