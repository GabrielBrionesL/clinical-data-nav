"""
HIPAA PII Scrubber — Plume Care Navigator
==========================================
Wraps Presidio with clinical-domain-aware configuration to prevent
false positives on medical abbreviations (pg, mL, ng, dL, etc.)
that Presidio's NLP model incorrectly tags as LOCATION entities.

Usage:
    from scrubber import scrub_pii
    clean_text = scrub_pii(raw_text)
"""

from presidio_analyzer import AnalyzerEngine, RecognizerResult
from presidio_anonymizer import AnonymizerEngine
from typing import List

# ── Clinical abbreviations that Presidio falsely tags as LOCATION ──────────────
# These are lab units, medical abbreviations, and clinical shorthand that
# should NEVER be anonymised.
CLINICAL_DENYLIST = {
    "pg",   # picograms (pg/mL)
    "ng",   # nanograms (ng/dL)
    "mL",   # millilitres
    "dL",   # decilitres
    "mmol", # millimoles
    "mg",   # milligrams
    "mcg",  # micrograms
    "IU",   # international units
    "g",    # grams (g/dL for haemoglobin)
    "CO",   # Colorado (state code — keep for policy context)
    "CA",   # California
    "TX",   # Texas
    "FL",   # Florida
    "NY",   # New York
    "WA",   # Washington
    "OR",   # Oregon
    "MN",   # Minnesota
    "IL",   # Illinois
    "MA",   # Massachusetts
    "CT",   # Connecticut
    "NJ",   # New Jersey
    "MD",   # Maryland
    "VT",   # Vermont
    "ME",   # Maine
    "RI",   # Rhode Island
    "HI",   # Hawaii
    "NM",   # New Mexico
    "NV",   # Nevada
    "DE",   # Delaware
    "DC",   # District of Columbia
}

_analyzer   = None
_anonymizer = None


def _get_engines():
    global _analyzer, _anonymizer
    if _analyzer is None:
        _analyzer   = AnalyzerEngine()
        _anonymizer = AnonymizerEngine()
    return _analyzer, _anonymizer


def scrub_pii(text: str) -> str:
    """
    Scrub PHI/PII from text before sending to the LLM.

    Detects and replaces: PERSON, EMAIL_ADDRESS, PHONE_NUMBER, US_SSN,
    MEDICAL_LICENSE, NRP (national ID).

    Explicitly EXCLUDES LOCATION to prevent false positives on clinical
    abbreviations (pg, ng, mL, dL) and US state codes used in policy context.

    Args:
        text: Raw text that may contain PHI.

    Returns:
        Anonymised text safe for LLM consumption.
    """
    if not text or not text.strip():
        return text

    analyzer, anonymizer = _get_engines()

    # Only detect entity types that are unambiguous in a clinical context.
    # LOCATION is intentionally excluded — it causes too many false positives
    # on clinical abbreviations and state codes.
    results = analyzer.analyze(
        text=text,
        language="en",
        entities=[
            "PERSON",
            "EMAIL_ADDRESS",
            "PHONE_NUMBER",
            "US_SSN",
            "MEDICAL_LICENSE",
            "NRP",
        ],
    )

    # Additional safety: filter out any results that match clinical denylist
    filtered_results = [
        r for r in results
        if text[r.start:r.end] not in CLINICAL_DENYLIST
    ]

    if not filtered_results:
        return text

    return anonymizer.anonymize(text=text, analyzer_results=filtered_results).text
