"""
Plume Care Navigator — Daily ETL DAG
=====================================
Orchestrates the full structured data pipeline on a daily schedule:
  1. Load new Parquet files from GCS into BigQuery Bronze layer (bq load)
  2. Run dbt models: Bronze → Silver → Gold → Mart
  3. Run dbt tests to validate data quality
  4. Send a Slack notification on success or failure

Schedule: Daily at 06:00 UTC (before care team starts their day)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# ── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ID   = "{{ var.value.gcp_project_id }}"
DATASET      = "plume_bronze"
GCS_BUCKET   = "{{ var.value.gcs_data_bucket }}"
DBT_DIR      = "/opt/airflow/dbt"
SLACK_CONN   = "slack_webhook_plume"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── Slack notification helpers ─────────────────────────────────────────────────
def _slack_success_message(context):
    return (
        f":white_check_mark: *Plume Daily ETL succeeded*\n"
        f"Run date: `{context['ds']}`\n"
        f"Duration: `{context['task_instance'].duration:.0f}s`"
    )

def _slack_failure_message(context):
    return (
        f":x: *Plume Daily ETL FAILED*\n"
        f"Task: `{context['task_instance'].task_id}`\n"
        f"Run date: `{context['ds']}`\n"
        f"Log: {context['task_instance'].log_url}"
    )


# ── DAG Definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="plume_daily_etl",
    description="Daily ETL: GCS → BigQuery Bronze → dbt Silver/Gold/Mart",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",   # 06:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["plume", "etl", "bigquery", "dbt"],
) as dag:

    # ── Task 1: Load Parquet files from GCS to BigQuery Bronze ─────────────────
    # In production, new files land in GCS daily from the Healthie/Stripe webhooks.
    # For demo purposes, this loads the static synthetic Parquet files.
    load_patients = BigQueryInsertJobOperator(
        task_id="load_bronze_patients",
        configuration={
            "load": {
                "sourceUris":            [f"gs://{GCS_BUCKET}/raw/patients/*.parquet"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId":   "patients",
                },
                "sourceFormat":          "PARQUET",
                "writeDisposition":      "WRITE_TRUNCATE",
                "autodetect":            True,
            }
        },
        project_id=PROJECT_ID,
    )

    load_subscriptions = BigQueryInsertJobOperator(
        task_id="load_bronze_subscriptions",
        configuration={
            "load": {
                "sourceUris":            [f"gs://{GCS_BUCKET}/raw/subscriptions/*.parquet"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId":   "subscriptions",
                },
                "sourceFormat":          "PARQUET",
                "writeDisposition":      "WRITE_TRUNCATE",
                "autodetect":            True,
            }
        },
        project_id=PROJECT_ID,
    )

    load_lab_results = BigQueryInsertJobOperator(
        task_id="load_bronze_lab_results",
        configuration={
            "load": {
                "sourceUris":            [f"gs://{GCS_BUCKET}/raw/lab_results/*.parquet"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId":   "lab_results",
                },
                "sourceFormat":          "PARQUET",
                "writeDisposition":      "WRITE_TRUNCATE",
                "autodetect":            True,
            }
        },
        project_id=PROJECT_ID,
    )

    load_clinical_visits = BigQueryInsertJobOperator(
        task_id="load_bronze_clinical_visits",
        configuration={
            "load": {
                "sourceUris":            [f"gs://{GCS_BUCKET}/raw/clinical_visits/*.parquet"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId":   "clinical_visits",
                },
                "sourceFormat":          "PARQUET",
                "writeDisposition":      "WRITE_TRUNCATE",
                "autodetect":            True,
            }
        },
        project_id=PROJECT_ID,
    )

    # ── Task 2: Run dbt models ─────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run "
            "--profiles-dir . "
            "--target prod "
            "--select bronze silver gold mart "
            "--vars '{\"run_date\": \"{{ ds }}\"}'"
        ),
    )

    # ── Task 3: Run dbt tests ──────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt test "
            "--profiles-dir . "
            "--target prod "
            "--select bronze silver gold mart"
        ),
    )

    # ── Task 4: Generate dbt docs ──────────────────────────────────────────────
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt docs generate "
            "--profiles-dir . "
            "--target prod"
        ),
    )

    # ── Task 5: Slack success notification ────────────────────────────────────
    slack_success = SlackWebhookOperator(
        task_id="slack_notify_success",
        slack_webhook_conn_id=SLACK_CONN,
        message=_slack_success_message,
        trigger_rule="all_success",
    )

    # ── Task 6: Slack failure notification ────────────────────────────────────
    slack_failure = SlackWebhookOperator(
        task_id="slack_notify_failure",
        slack_webhook_conn_id=SLACK_CONN,
        message=_slack_failure_message,
        trigger_rule="one_failed",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    # All four Bronze loads run in parallel, then dbt runs sequentially
    [load_patients, load_subscriptions, load_lab_results, load_clinical_visits] >> dbt_run
    dbt_run >> dbt_test >> dbt_docs >> slack_success
    dbt_run >> slack_failure
    dbt_test >> slack_failure
