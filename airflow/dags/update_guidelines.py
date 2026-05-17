"""
Plume Care Navigator — Update Guidelines DAG
=============================================
Refreshes the ChromaDB vector store with the latest WPATH Standards of Care
document. Runs weekly (Sunday at 02:00 UTC) or can be triggered manually
when a new version of the guidelines is published.

Pipeline:
  1. Download the WPATH SoC v8 PDF from a configured URL (or GCS bucket)
  2. Chunk the document using LangChain's RecursiveCharacterTextSplitter
  3. Embed chunks using Voyage AI voyage-3 embeddings
  4. Persist the updated ChromaDB index to GCS for shared access
  5. Notify the care team via Slack
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# ── Configuration ──────────────────────────────────────────────────────────────
GCS_BUCKET      = "{{ var.value.gcs_data_bucket }}"
WPATH_PDF_URL   = "{{ var.value.wpath_pdf_url }}"   # Set in Airflow Variables
CHROMA_DIR      = "/opt/airflow/data/vector_db"
VOYAGE_API_KEY  = os.environ.get("VOYAGE_API_KEY")
SLACK_CONN      = "slack_webhook_plume"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
}


# ── Task Functions ─────────────────────────────────────────────────────────────

def download_wpath_pdf(**context):
    """Download the WPATH SoC PDF to a local temp file."""
    import requests, tempfile, os

    url       = context["params"]["wpath_pdf_url"]
    dest_path = os.path.join(CHROMA_DIR, "wpath_soc_v8.pdf")
    os.makedirs(CHROMA_DIR, exist_ok=True)

    print(f"Downloading WPATH PDF from: {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()

    with open(dest_path, "wb") as f:
        f.write(response.content)

    file_size_mb = os.path.getsize(dest_path) / 1e6
    print(f"✓ Downloaded {file_size_mb:.1f} MB to {dest_path}")
    context["ti"].xcom_push(key="pdf_path", value=dest_path)


def chunk_and_embed(**context):
    """Chunk the WPATH PDF and embed into ChromaDB."""
    from langchain_community.document_loaders import PyPDFLoader
    from langchain.text_splitter import RecursiveCharacterTextSplitter
    from langchain_voyageai import VoyageAIEmbeddings
    from langchain_community.vectorstores import Chroma

    pdf_path = context["ti"].xcom_pull(key="pdf_path", task_ids="download_wpath_pdf")
    print(f"Loading PDF: {pdf_path}")

    # Load and split
    loader   = PyPDFLoader(pdf_path)
    pages    = loader.load()
    print(f"  Loaded {len(pages)} pages from WPATH SoC v8")

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=150,
        separators=["\n\n", "\n", ". ", " "],
    )
    chunks = splitter.split_documents(pages)
    print(f"  Split into {len(chunks)} chunks")

    # Embed and persist using Voyage AI voyage-3
    # Free tier: 200M tokens/month — more than sufficient for WPATH SoC v8
    embeddings = VoyageAIEmbeddings(
        model="voyage-3",
        voyage_api_key=VOYAGE_API_KEY,
    )

    # Rebuild the index from scratch (ensures no stale chunks)
    import shutil
    if os.path.exists(CHROMA_DIR + "/chroma.sqlite3"):
        shutil.rmtree(CHROMA_DIR)
        os.makedirs(CHROMA_DIR, exist_ok=True)

    vectorstore = Chroma.from_documents(
        documents=chunks,
        embedding=embeddings,
        persist_directory=CHROMA_DIR,
        collection_name="wpath_guidelines",
    )
    print(f"  ✓ Persisted {len(chunks)} vectors to ChromaDB at {CHROMA_DIR}")

    context["ti"].xcom_push(key="chunk_count", value=len(chunks))


def upload_to_gcs(**context):
    """Upload the updated ChromaDB index to GCS for shared access."""
    from google.cloud import storage
    import shutil, os

    bucket_name = GCS_BUCKET
    gcs_prefix  = "vector_db/wpath_guidelines"

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Zip the ChromaDB directory and upload
    archive_path = "/tmp/chroma_index.tar.gz"
    shutil.make_archive("/tmp/chroma_index", "gztar", CHROMA_DIR)

    blob = bucket.blob(f"{gcs_prefix}/chroma_index.tar.gz")
    blob.upload_from_filename(archive_path)
    print(f"✓ Uploaded ChromaDB index to gs://{bucket_name}/{gcs_prefix}/chroma_index.tar.gz")


def _slack_success_message(context):
    chunk_count = context["ti"].xcom_pull(key="chunk_count", task_ids="chunk_and_embed") or "unknown"
    return (
        f":books: *WPATH Guidelines index updated successfully*\n"
        f"Chunks embedded: `{chunk_count}`\n"
        f"Run date: `{context['ds']}`"
    )


def _slack_failure_message(context):
    return (
        f":x: *WPATH Guidelines update FAILED*\n"
        f"Task: `{context['task_instance'].task_id}`\n"
        f"Log: {context['task_instance'].log_url}"
    )


# ── DAG Definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="plume_update_guidelines",
    description="Weekly refresh of the WPATH SoC ChromaDB vector index",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * 0",   # 02:00 UTC every Sunday
    catchup=False,
    max_active_runs=1,
    params={"wpath_pdf_url": WPATH_PDF_URL},
    tags=["plume", "rag", "chromadb", "wpath"],
) as dag:

    download = PythonOperator(
        task_id="download_wpath_pdf",
        python_callable=download_wpath_pdf,
    )

    embed = PythonOperator(
        task_id="chunk_and_embed",
        python_callable=chunk_and_embed,
    )

    upload = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    slack_success = SlackWebhookOperator(
        task_id="slack_notify_success",
        slack_webhook_conn_id=SLACK_CONN,
        message=_slack_success_message,
        trigger_rule="all_success",
    )

    slack_failure = SlackWebhookOperator(
        task_id="slack_notify_failure",
        slack_webhook_conn_id=SLACK_CONN,
        message=_slack_failure_message,
        trigger_rule="one_failed",
    )

    download >> embed >> upload >> slack_success
    embed >> slack_failure
