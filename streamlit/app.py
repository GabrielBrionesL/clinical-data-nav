"""
Plume Care Navigator — Streamlit Provider UI
=============================================
A two-panel clinical decision-support interface for Plume care coordinators.

Panel A (left): Live patient lab summary and subscription status from BigQuery Gold layer
Panel B (right): RAG-powered chat interface backed by WPATH SoC v8 guidelines

Run with:
    streamlit run app.py

Environment variables required (set in .env or Streamlit secrets):
    ANTHROPIC_API_KEY
    VOYAGE_API_KEY
    GCP_PROJECT_ID
    MART_DATASET      (default: plume_mart)
    GOLD_DATASET      (default: plume_gold)
    CHROMA_DIR        (default: ../data/vector_db)
"""

import os
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from langchain_anthropic import ChatAnthropic
from langchain_voyageai import VoyageAIEmbeddings
from langchain_chroma import Chroma
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough, RunnableLambda
from langchain_core.output_parsers import StrOutputParser
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ID   = os.getenv("GCP_PROJECT_ID", "YOUR_PROJECT_ID")
MART_DATASET = os.getenv("MART_DATASET", "plume_mart")
GOLD_DATASET = os.getenv("GOLD_DATASET", "plume_gold")
CHROMA_DIR   = os.getenv("CHROMA_DIR", "../data/vector_db")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
VOYAGE_KEY    = os.getenv("VOYAGE_API_KEY")

# ── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Plume Care Navigator",
    page_icon="🌸",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #6B46C1 0%, #D53F8C 100%);
        padding: 1.2rem 1.5rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        color: white;
    }
    .metric-card {
        background: #F7FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 0.8rem 1rem;
        margin-bottom: 0.5rem;
    }
    .flag-normal  { color: #38A169; font-weight: bold; }
    .flag-high    { color: #E53E3E; font-weight: bold; }
    .flag-low     { color: #DD6B20; font-weight: bold; }
    .policy-protected  { color: #38A169; }
    .policy-restricted { color: #E53E3E; }
    .policy-neutral    { color: #718096; }
    .chat-message-user      { background: #EBF8FF; border-radius: 8px; padding: 0.8rem; margin: 0.4rem 0; }
    .chat-message-assistant { background: #F0FFF4; border-radius: 8px; padding: 0.8rem; margin: 0.4rem 0; }
    .hipaa-badge {
        background: #FED7D7; color: #742A2A;
        padding: 0.2rem 0.6rem; border-radius: 4px;
        font-size: 0.75rem; font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)


# ── Cached Resource Initialisation ────────────────────────────────────────────

@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

@st.cache_resource
def get_presidio():
    return AnalyzerEngine(), AnonymizerEngine()

@st.cache_resource
def get_vectorstore():
    # Voyage AI voyage-3: Anthropic's recommended embedding model.
    # Free tier: 200M tokens/month at https://dash.voyageai.com
    embeddings = VoyageAIEmbeddings(
        model="voyage-3",
        voyage_api_key=VOYAGE_KEY,
    )
    return Chroma(
        persist_directory=CHROMA_DIR,
        embedding_function=embeddings,
        collection_name="wpath_guidelines",
    )

@st.cache_resource
def get_llm():
    # Claude 3.5 Haiku: fast, cost-efficient, strong instruction-following.
    # Ideal for clinical decision-support where accuracy and safety matter.
    return ChatAnthropic(
        model="claude-3-5-haiku-20241022",
        temperature=0.1,
        anthropic_api_key=ANTHROPIC_KEY,
        max_tokens=1024,
    )


# ── Helper Functions ───────────────────────────────────────────────────────────

def scrub_pii(text: str) -> str:
    analyzer, anonymizer = get_presidio()
    results = analyzer.analyze(
        text=text, language="en",
        entities=["PERSON", "DATE_TIME", "LOCATION", "EMAIL_ADDRESS",
                  "PHONE_NUMBER", "US_SSN", "MEDICAL_LICENSE"],
    )
    if not results:
        return text
    return anonymizer.anonymize(text=text, analyzer_results=results).text


@st.cache_data(ttl=300)
def get_patient_list():
    """Fetch a list of active patients for the sidebar selector."""
    bq = get_bq_client()
    query = f"""
    SELECT
        patient_token,
        SUBSTR(patient_token, 1, 8)     AS display_id,
        gender_identity,
        hrt_category,
        state,
        subscription_status,
        age_band
    FROM `{PROJECT_ID}.{MART_DATASET}.mart_patient_rag_context`
    WHERE subscription_status = 'Active'
    ORDER BY patient_token
    LIMIT 500
    """
    return bq.query(query).to_dataframe()


@st.cache_data(ttl=60)
def get_patient_context(patient_token: str) -> str:
    bq = get_bq_client()
    query = f"""
    SELECT rag_context_text
    FROM `{PROJECT_ID}.{MART_DATASET}.mart_patient_rag_context`
    WHERE patient_token = @token
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("token", "STRING", patient_token)]
    )
    rows = list(bq.query(query, job_config=job_config).result())
    if not rows:
        return "No patient context found."
    return scrub_pii(rows[0].rag_context_text)


@st.cache_data(ttl=60)
def get_patient_labs(patient_token: str) -> pd.DataFrame:
    bq = get_bq_client()
    query = f"""
    SELECT
        test_name,
        result_value,
        unit,
        flag,
        is_in_therapeutic_target,
        therapeutic_target_low,
        therapeutic_target_high,
        lab_date
    FROM `{PROJECT_ID}.{GOLD_DATASET}.fact_lab_results`
    WHERE patient_token = @token
    ORDER BY lab_date DESC, test_name
    LIMIT 20
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("token", "STRING", patient_token)]
    )
    return bq.query(query, job_config=job_config).to_dataframe()


@st.cache_data(ttl=60)
def get_patient_summary(patient_token: str) -> dict:
    bq = get_bq_client()
    query = f"""
    SELECT
        gender_identity, hrt_category, hrt_regimen, age_years, age_band,
        state, policy_status, subscription_status, plan_type, mrr,
        tenure_months, completed_90d, no_shows_90d, latest_lab_date
    FROM `{PROJECT_ID}.{MART_DATASET}.mart_patient_rag_context`
    WHERE patient_token = @token
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("token", "STRING", patient_token)]
    )
    rows = list(bq.query(query, job_config=job_config).result())
    return dict(rows[0]) if rows else {}


def build_rag_chain(patient_token: str):
    vectorstore = get_vectorstore()
    llm         = get_llm()

    chroma_retriever = vectorstore.as_retriever(
        search_type="similarity",
        search_kwargs={"k": 4},
    )

    patient_ctx = get_patient_context(patient_token)

    SYSTEM_PROMPT = """
You are a clinical decision-support assistant for Plume, a trans-focused telehealth provider.
Your role is to help care coordinators and providers by:
1. Summarising a patient's current clinical state from their patient context
2. Answering clinical questions by citing relevant sections of the WPATH Standards of Care v8

IMPORTANT RULES:
- Always cite the specific WPATH SoC v8 chapter or section you are drawing from
- Never make diagnostic decisions — only surface relevant guideline information
- If the patient context does not contain enough information, say so clearly
- Use respectful, affirming language consistent with trans-inclusive care
- Do not speculate beyond what is in the provided context and guidelines

Patient Context:
{patient_context}

Relevant WPATH SoC v8 Guideline Excerpts:
{guideline_context}
"""

    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        ("human", "{question}"),
    ])

    def format_docs(docs):
        return "\n\n---\n\n".join(
            f"[Page {d.metadata.get('page', '?')}] {d.page_content}"
            for d in docs
        )

    chain = (
        {
            "guideline_context": chroma_retriever | RunnableLambda(format_docs),
            "patient_context":   RunnableLambda(lambda _: patient_ctx),
            "question":          RunnablePassthrough(),
        }
        | prompt
        | llm
        | StrOutputParser()
    )
    return chain


# ── UI Layout ──────────────────────────────────────────────────────────────────

# Header
st.markdown("""
<div class="main-header">
    <h2 style="margin:0">🌸 Plume Care Navigator</h2>
    <p style="margin:0.3rem 0 0 0; opacity:0.85">Clinical decision support powered by WPATH SoC v8 &nbsp;|&nbsp;
    <span class="hipaa-badge">HIPAA SAFE — PII Scrubbed</span></p>
</div>
""", unsafe_allow_html=True)

# ── Sidebar: Patient Selector ──────────────────────────────────────────────────
with st.sidebar:
    st.header("Patient Selector")

    try:
        patients_df = get_patient_list()

        # Filters
        gender_filter = st.multiselect(
            "Gender Identity",
            options=patients_df["gender_identity"].unique().tolist(),
            default=[],
        )
        hrt_filter = st.multiselect(
            "HRT Category",
            options=patients_df["hrt_category"].unique().tolist(),
            default=[],
        )
        state_filter = st.multiselect(
            "State",
            options=sorted(patients_df["state"].unique().tolist()),
            default=[],
        )

        filtered = patients_df.copy()
        if gender_filter:
            filtered = filtered[filtered["gender_identity"].isin(gender_filter)]
        if hrt_filter:
            filtered = filtered[filtered["hrt_category"].isin(hrt_filter)]
        if state_filter:
            filtered = filtered[filtered["state"].isin(state_filter)]

        if filtered.empty:
            st.warning("No patients match the selected filters.")
            selected_token = None
        else:
            # Build display labels
            filtered["label"] = (
                "ID: " + filtered["display_id"] + " | "
                + filtered["gender_identity"] + " | "
                + filtered["state"]
            )
            selected_label = st.selectbox(
                f"Select Patient ({len(filtered):,} shown)",
                options=filtered["label"].tolist(),
            )
            selected_row   = filtered[filtered["label"] == selected_label].iloc[0]
            selected_token = selected_row["patient_token"]

    except Exception as e:
        st.error(f"Could not load patient list: {e}")
        st.info("Ensure BigQuery credentials are configured and dbt models have been run.")
        selected_token = None

    st.divider()
    st.caption("Data refreshes every 5 minutes. Lab panel refreshes every 60 seconds.")


# ── Main Content: Two Columns ──────────────────────────────────────────────────
if selected_token is None:
    st.info("Select a patient from the sidebar to begin.")
    st.stop()

col_labs, col_chat = st.columns([1, 1.4], gap="large")

# ── Column A: Patient Summary + Lab Panel ─────────────────────────────────────
with col_labs:
    st.subheader("Patient Summary")

    try:
        summary = get_patient_summary(selected_token)

        if summary:
            # Subscription & demographics
            c1, c2, c3 = st.columns(3)
            c1.metric("Age Band",    summary.get("age_band", "—"))
            c2.metric("State",       summary.get("state", "—"))
            c3.metric("Tenure",      f"{summary.get('tenure_months', 0)} mo")

            c4, c5, c6 = st.columns(3)
            c4.metric("Plan",        summary.get("plan_type", "—"))
            c5.metric("MRR",         f"${summary.get('mrr', 0):.0f}")
            c6.metric("Status",      summary.get("subscription_status", "—"))

            # State policy badge
            policy = summary.get("policy_status", "Unknown")
            policy_color = {
                "Protected": "🟢", "Neutral": "🟡", "Restricted": "🔴"
            }.get(policy, "⚪")
            st.markdown(f"**State Policy:** {policy_color} {policy}")

            # HRT info
            st.markdown(f"**HRT:** {summary.get('hrt_regimen', '—')}")
            st.markdown(f"**Category:** {summary.get('hrt_category', '—')}")

            # Visit engagement
            completed = summary.get("completed_90d", 0) or 0
            no_shows  = summary.get("no_shows_90d", 0) or 0
            st.markdown(f"**Visits (90d):** {completed} completed · {no_shows} no-shows")
        else:
            st.warning("No summary data found for this patient.")

    except Exception as e:
        st.error(f"Error loading patient summary: {e}")

    st.divider()
    st.subheader("Latest Lab Results")

    try:
        labs_df = get_patient_labs(selected_token)

        if labs_df.empty:
            st.info("No lab results on file for this patient.")
        else:
            # Show only the most recent result per test
            latest_labs = labs_df.sort_values("lab_date", ascending=False).groupby("test_name").first().reset_index()

            for _, row in latest_labs.iterrows():
                flag_icon = {"Normal": "✅", "High": "🔴", "Low": "🟠"}.get(row["flag"], "—")
                target_text = ""
                if pd.notna(row.get("therapeutic_target_low")):
                    in_target = "IN TARGET" if row["is_in_therapeutic_target"] else "OUT OF TARGET"
                    target_text = f" [{in_target}: {row['therapeutic_target_low']:.0f}–{row['therapeutic_target_high']:.0f}]"

                st.markdown(
                    f"**{row['test_name']}**: {row['result_value']:.1f} {row['unit']} "
                    f"{flag_icon} {row['flag']}{target_text}  \n"
                    f"<small>Last collected: {row['lab_date']}</small>",
                    unsafe_allow_html=True,
                )

    except Exception as e:
        st.error(f"Error loading lab results: {e}")


# ── Column B: RAG Chat Interface ───────────────────────────────────────────────
with col_chat:
    st.subheader("Care Navigator Chat")
    st.caption("Ask clinical questions — answers are grounded in WPATH SoC v8 guidelines")

    # Initialise chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "current_patient" not in st.session_state:
        st.session_state.current_patient = None

    # Reset chat when patient changes
    if st.session_state.current_patient != selected_token:
        st.session_state.messages = []
        st.session_state.current_patient = selected_token

    # Suggested questions
    with st.expander("💡 Suggested Questions", expanded=False):
        suggestions = [
            "What does WPATH SoC v8 recommend for monitoring estradiol levels?",
            "What is the recommended follow-up frequency for patients on feminising HRT?",
            "What are the WPATH recommendations for testosterone monitoring in masculinising HRT?",
            "What does WPATH say about mental health support for trans patients on HRT?",
            "How should a provider respond to a patient with out-of-range potassium levels?",
        ]
        for s in suggestions:
            if st.button(s, key=f"suggest_{s[:20]}"):
                st.session_state.messages.append({"role": "user", "content": s})

    # Display chat history
    chat_container = st.container()
    with chat_container:
        for msg in st.session_state.messages:
            if msg["role"] == "user":
                st.markdown(
                    f'<div class="chat-message-user">👤 <strong>You:</strong> {msg["content"]}</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div class="chat-message-assistant">🌸 <strong>Navigator:</strong><br>{msg["content"]}</div>',
                    unsafe_allow_html=True,
                )

    # Chat input
    user_input = st.chat_input("Ask a clinical question about this patient...")

    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})

        try:
            chain = build_rag_chain(selected_token)

            with st.spinner("Retrieving guidelines and generating response..."):
                response = chain.invoke(user_input)

            st.session_state.messages.append({"role": "assistant", "content": response})
            st.rerun()

        except Exception as e:
            st.error(f"Error generating response: {e}")
            st.info("Ensure ANTHROPIC_API_KEY and VOYAGE_API_KEY are set and the ChromaDB index has been built (run notebook 02).")

    # Clear chat button
    if st.session_state.messages:
        if st.button("🗑 Clear Chat", type="secondary"):
            st.session_state.messages = []
            st.rerun()

# ── Footer ─────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "⚠️ This tool is for clinical decision **support** only. "
    "All recommendations must be reviewed by a licensed provider. "
    "Patient data is anonymised — no PHI is transmitted to external APIs. "
    "Guidelines sourced from WPATH Standards of Care v8 (2022)."
)
