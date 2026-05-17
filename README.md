# Plume Care Navigator: Technical Design & Architecture

![Architecture Diagram](docs/plume_architecture_diagram_v2.png)

## Overview

The **Plume Care Navigator** is an end-to-end clinical data platform and applied AI application. I designed and built this project specifically for the Senior Data Engineer application at Plume Clinic. 

This repo contains production-grade architecture that ingests 39.4 million rows of synthetic clinical data (EHR, billing, and lab results), transforms it via a dbt medallion architecture, and serves it alongside clinical guidelines (WPATH Standards of Care v8) through a LangChain Retrieval-Augmented Generation (RAG) assistant.

### The Business Problem

Care coordinators and providers at virtual clinics spend significant time manually synthesizing structured data (EHR records, lab results, subscription status) and cross-referencing it against unstructured clinical guidelines during patient encounters. 

This platform solves that problem by providing a unified, HIPAA-compliant interface. Providers can ask natural language questions about a specific patient and receive answers grounded in cited clinical guidelines, drastically reducing cognitive load and improving care efficiency at scale.

---

## Architecture & Tool Selection Rationale

Every tool in this stack was chosen to balance performance, cost, and compliance in a healthcare context.

### 1. Storage & Transformation Layer: BigQuery + dbt
* **Rationale:** BigQuery is the industry standard for serverless data warehousing, and dbt provides software engineering rigor (version control, testing, macros) to SQL.
* **Implementation:** The pipeline implements a full medallion architecture:
  * **Bronze:** Raw pass-through views with schema contracts.
  * **Silver:** Cleaned, typed, and PHI-tokenized using a custom Jinja macro (`hash_phi`).
  * **Gold:** Dimensional models (`dim_patient`, `fact_lab_results`, `fact_visits`), including a true SCD Type 2 `dim_state_policy` table tracking legislative changes over time.
* **Production Consideration:** In production, BigQuery row-level security (RLS) would be applied at the Gold layer to restrict provider access to only their assigned patients.

### 2. The Bridge: `mart_patient_rag_context`
* **The Trade-off:** Asking an LLM to generate SQL against a warehouse (Text-to-SQL) is brittle, slow, and prone to hallucination—unacceptable in a clinical setting.
* **The Solution:** I built a dbt mart model that uses SQL string aggregation to compile a patient's entire clinical state into a single, anonymized text column.
* **Example Output:** *"Patient TKN-4821: Trans Woman, age 28, state: CO (Protected). HRT: Feminising HRT. Subscription: active. Last labs: Estradiol 148 pg/mL (IN RANGE). Visits last 90 days: 2 completed."*
* **Why it matters:** This allows the LangChain chain to read structured warehouse data as context instantly, making the AI safe, fast, and auditable.

### 3. Orchestration: Apache Airflow
* **Rationale:** Airflow is the standard for complex dependency management and retry logic.
* **Implementation:** Two production-grade DAGs handle the pipelines. The `daily_etl` DAG runs the BigQuery ingestion and dbt build in parallel, while `update_guidelines` handles the WPATH vector ingestion. Both utilize `trigger_rule="one_failed"` for Slack alerting.
* **Production Consideration:** The DAGs use `catchup=False` and `max_active_runs=1` to prevent runaway backfills, a common issue in production Airflow environments.

### 4. HIPAA Guardrail: Presidio PII Scrubber
* **Rationale:** Sending PHI to external LLM APIs is a severe HIPAA violation.
* **Implementation:** Microsoft Presidio runs locally in the Streamlit app to scrub PII *before* any text touches the LLM API. 
* **The Trade-off:** Out-of-the-box, Presidio aggressively flags clinical lab units (e.g., `pg/mL`) as locations. I wrote a custom `scrubber.py` module with a clinical deny-list to fix this false positive, ensuring lab values aren't corrupted while still protecting patient names and SSNs.

### 5. LLM & Embeddings: Anthropic Claude + Voyage AI
* **Rationale:** Claude 3.5 Haiku was selected over OpenAI for its speed and Anthropic's stronger reputation for safety alignment in healthcare. Voyage AI (`voyage-3`) was chosen for embeddings due to its superior retrieval performance on domain-specific text.
* **Implementation:** A dual-retriever LangChain chain queries both ChromaDB (WPATH guidelines) and BigQuery (patient mart) simultaneously.

### 6. MLOps Monitoring: LangSmith
* **Rationale:** Deploying AI in healthcare requires observability.
* **Implementation:** LangSmith tracing is integrated into the LangChain pipeline to log prompt inputs/outputs, track latency, and monitor token usage. This provides a full audit trail for every clinical query.

### 7. BI Consumption: Looker & Tableau
* **Rationale:** The data platform must serve business users, not just AI applications.
* **Implementation:** The `bi/` directory contains LookML view stubs (`plume_patients.view.lkml`, `plume_lab_results.view.lkml`) and a Tableau-ready SQL view file (`tableau_clinical_views.sql`) designed to connect directly to the Gold layer.

---

## Repository Structure

```text
plume_care_navigator/
├── notebooks/                      # Google Colab notebooks for execution
│   ├── 00_data_generation.ipynb    # Generates 1M–2.68M synthetic patient records
│   ├── 01_bigquery_ingestion.ipynb # Loads Parquet files to BigQuery Bronze layer
│   └── 02_rag_pipeline.ipynb       # WPATH → ChromaDB ingestion + LangChain + LangSmith
├── dbt/                            # dbt project for data transformation
│   ├── models/
│   │   ├── bronze/                 # Raw pass-through views with schema contracts
│   │   ├── silver/                 # Cleaned, typed, PHI-tokenised
│   │   ├── gold/                   # Dimensional models (facts & dims, SCD Type 2)
│   │   └── mart/                   # mart_patient_rag_context — SQL → LLM bridge
│   ├── macros/                     # hash_phi.sql — PHI tokenisation macro
│   └── dbt_project.yml
├── airflow/                        # Orchestration DAGs
│   └── dags/
│       ├── daily_etl.py            # Parallel BQ load → dbt run → dbt test → Slack
│       └── update_guidelines.py    # WPATH PDF download → ChromaDB embed → Slack
├── bi/                             # BI layer — Looker and Tableau assets
│   ├── lookml/
│   └── tableau/
├── app/                            # Streamlit Provider UI
│   ├── app.py                      # Care Navigator interface
│   ├── scrubber.py                 # Presidio PII scrubber (clinical deny-list)
│   └── requirements.txt
├── .github/workflows/dbt_ci.yml    # CI/CD: Runs `dbt build` on every PR
---

## Getting Started

To run this project locally:

1. **Generate Data:** Run `notebooks/00_data_generation.ipynb` in Google Colab to generate the synthetic Parquet files.
2. **Setup GCP:** Create a GCP project, enable the BigQuery API, and create a dataset named `plume_bronze`.
3. **Ingest Data:** Run `notebooks/01_bigquery_ingestion.ipynb` to load the Parquet files into BigQuery.
4. **Run dbt:** Navigate to `dbt/`, configure `profiles.yml` with your GCP project, and run `dbt build`.
5. **Build Vector DB:** Run `notebooks/02_rag_pipeline.ipynb` to download the WPATH SoC v8 guidelines, embed them into ChromaDB, and test the LangChain chain.
6. **Launch UI:** Navigate to `app/` and run `streamlit run app.py`.

*(Note: The Airflow DAGs are provided as production-grade code samples demonstrating orchestration design and can be executed locally via Astro CLI or a Docker-based Airflow setup.)*
