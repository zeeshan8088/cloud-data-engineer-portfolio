"""
Week 6 - Day 3: Hospital Pipeline on GCP Dataflow
Same 5 transforms as Day 2 — only the runner changes.

Day 2: DirectRunner  (your laptop/Docker)
Day 3: DataflowRunner (Google Cloud — auto-scaling workers)
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
import csv
import json
import logging
import hashlib
import re
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


# ============================================================
# THE SAME 5 TRANSFORMS FROM DAY 2 — NOTHING CHANGED
# ============================================================

class ParseHospitalRecord(beam.DoFn):
    """Step 1: Convert raw CSV text rows into Python dictionaries."""
    def process(self, element):
        try:
            reader = csv.DictReader(
                [element],
                fieldnames=[
                    "facility_id", "facility_name", "city", "state",
                    "zip_code", "county", "hospital_type", "ownership",
                    "emergency_services", "overall_rating",
                    "readmission_comparison", "patient_experience_comparison",
                    "mortality_comparison"
                ]
            )
            for row in reader:
                if row["facility_id"] in ("Facility ID", "facility_id"):
                    return
                yield {
                    "facility_id": row.get("facility_id", "").strip(),
                    "facility_name": row.get("facility_name", "").strip(),
                    "city": row.get("city", "").strip(),
                    "state": row.get("state", "").strip(),
                    "zip_code": row.get("zip_code", "").strip(),
                    "county": row.get("county", "").strip(),
                    "hospital_type": row.get("hospital_type", "").strip(),
                    "ownership": row.get("ownership", "").strip(),
                    "emergency_services": row.get("emergency_services", "").strip(),
                    "overall_rating": row.get("overall_rating", "Not Available").strip(),
                    "readmission_comparison": row.get("readmission_comparison", "Not Available").strip(),
                    "patient_experience_comparison": row.get("patient_experience_comparison", "Not Available").strip(),
                    "mortality_comparison": row.get("mortality_comparison", "Not Available").strip(),
                    "ingested_at": datetime.utcnow().isoformat(),
                    "pipeline_version": "v1.0",
                }
        except Exception as e:
            logger.warning(f"Parse error: {e}")


class ValidateHospitalRecord(beam.DoFn):
    """Step 2: Separate valid records from invalid ones."""
    VALID_TAG = "valid"
    INVALID_TAG = "invalid"

    def process(self, element):
        errors = []
        if not element.get("facility_id"):
            errors.append("missing_facility_id")
        state = element.get("state", "")
        if not state or len(state) != 2 or not state.isalpha():
            errors.append(f"invalid_state:{state}")
        if not element.get("hospital_type"):
            errors.append("missing_hospital_type")
        if errors:
            yield beam.pvalue.TaggedOutput(
                self.INVALID_TAG,
                {**element, "validation_errors": ", ".join(errors)}
            )
        else:
            yield beam.pvalue.TaggedOutput(self.VALID_TAG, element)


class MaskPIIFields(beam.DoFn):
    """Step 3: Hash facility IDs so real IDs are never exposed."""
    def process(self, element):
        raw_id = element.get("facility_id", "")
        masked_id = hashlib.sha256(
            f"salt_zeeshan_{raw_id}".encode()
        ).hexdigest()[:12].upper()
        yield {
            **element,
            "facility_id_masked": masked_id,
            "pii_masked": True,
        }


class EnrichHospitalRecord(beam.DoFn):
    """Step 4: Add quality tier and risk score to each hospital."""
    RATING_TO_TIER = {"5": "A", "4": "B", "3": "C", "2": "D", "1": "F"}
    COMPARISON_SCORES = {
        "Above the national average": 2,
        "Same as the national average": 1,
        "Below the national average": 0,
        "Not Available": 1,
    }

    def process(self, element):
        rating = element.get("overall_rating", "Not Available")
        quality_tier = self.RATING_TO_TIER.get(rating, "UNKNOWN")
        readmission_score = self.COMPARISON_SCORES.get(element.get("readmission_comparison"), 1)
        mortality_score = self.COMPARISON_SCORES.get(element.get("mortality_comparison"), 1)
        experience_score = self.COMPARISON_SCORES.get(element.get("patient_experience_comparison"), 1)
        composite = readmission_score + mortality_score + experience_score
        risk_score = round((6 - composite) / 6 * 100, 2)
        is_high_risk = (
            element.get("readmission_comparison") == "Above the national average"
            and element.get("mortality_comparison") == "Above the national average"
        )
        yield {
            **element,
            "quality_tier": quality_tier,
            "risk_score": risk_score,
            "is_high_risk": is_high_risk,
            "has_emergency": element.get("emergency_services") == "Yes",
            "enriched_at": datetime.utcnow().isoformat(),
        }


class DetectHighRiskHospitals(beam.DoFn):
    """Step 5: Flag hospitals with both high readmission and high mortality."""
    NORMAL_TAG = "normal"
    ALERT_TAG = "alert"

    def process(self, element):
        if element.get("is_high_risk"):
            yield beam.pvalue.TaggedOutput(
                self.ALERT_TAG,
                {
                    **element,
                    "alert_reason": "High readmission AND high mortality vs national average",
                    "alert_severity": "HIGH",
                    "alert_created_at": datetime.utcnow().isoformat(),
                }
            )
        else:
            yield beam.pvalue.TaggedOutput(self.NORMAL_TAG, element)


# ============================================================
# BIGQUERY TABLE SCHEMAS
# Tells Dataflow exactly what columns to create in BigQuery
# ============================================================

CURATED_SCHEMA = {
    "fields": [
        {"name": "facility_id",                     "type": "STRING"},
        {"name": "facility_id_masked",              "type": "STRING"},
        {"name": "facility_name",                   "type": "STRING"},
        {"name": "city",                            "type": "STRING"},
        {"name": "state",                           "type": "STRING"},
        {"name": "zip_code",                        "type": "STRING"},
        {"name": "county",                          "type": "STRING"},
        {"name": "hospital_type",                   "type": "STRING"},
        {"name": "ownership",                       "type": "STRING"},
        {"name": "emergency_services",              "type": "STRING"},
        {"name": "overall_rating",                  "type": "STRING"},
        {"name": "readmission_comparison",          "type": "STRING"},
        {"name": "patient_experience_comparison",   "type": "STRING"},
        {"name": "mortality_comparison",            "type": "STRING"},
        {"name": "quality_tier",                    "type": "STRING"},
        {"name": "risk_score",                      "type": "FLOAT"},
        {"name": "is_high_risk",                    "type": "BOOLEAN"},
        {"name": "has_emergency",                   "type": "BOOLEAN"},
        {"name": "pii_masked",                      "type": "BOOLEAN"},
        {"name": "ingested_at",                     "type": "STRING"},
        {"name": "enriched_at",                     "type": "STRING"},
        {"name": "pipeline_version",                "type": "STRING"},
    ]
}

ALERTS_SCHEMA = {
    "fields": [
        {"name": "facility_id_masked",  "type": "STRING"},
        {"name": "facility_name",       "type": "STRING"},
        {"name": "state",               "type": "STRING"},
        {"name": "quality_tier",        "type": "STRING"},
        {"name": "risk_score",          "type": "FLOAT"},
        {"name": "readmission_comparison", "type": "STRING"},
        {"name": "mortality_comparison",   "type": "STRING"},
        {"name": "alert_reason",        "type": "STRING"},
        {"name": "alert_severity",      "type": "STRING"},
        {"name": "alert_created_at",    "type": "STRING"},
        {"name": "ingested_at",         "type": "STRING"},
    ]
}


# ============================================================
# PIPELINE RUNNER — THIS IS WHAT CHANGED FROM DAY 2
# ============================================================

def run_pipeline():
    PROJECT   = "intricate-ward-459513-e1"
    REGION    = "us-central1"
    BUCKET    = "zeeshan-hospital-pipeline"
    INPUT     = f"gs://{BUCKET}/data/hospitals_raw.csv"
    STAGING   = f"gs://{BUCKET}/staging"
    TEMP      = f"gs://{BUCKET}/temp"

    # BigQuery destination tables
    CURATED_TABLE = f"{PROJECT}:hospital_analytics.curated_hospitals"
    ALERTS_TABLE  = f"{PROJECT}:hospital_analytics.hospital_alerts"

    logger.info("=" * 60)
    logger.info("HOSPITAL PIPELINE - GCP DATAFLOW")
    logger.info(f"Project: {PROJECT}")
    logger.info(f"Input:   {INPUT}")
    logger.info(f"Output:  BigQuery → hospital_analytics")
    logger.info("=" * 60)

    # Pipeline options — this is the KEY difference from Day 2
    options = PipelineOptions()

    # Google Cloud specific options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project          = PROJECT
    google_cloud_options.region      = "us-west1"
    google_cloud_options.temp_location    = TEMP
    google_cloud_options.job_name         = "hospital-pipeline-zeeshan"

    # Tell Beam to use Dataflow (cloud) instead of DirectRunner (laptop)
    options.view_as(StandardOptions).runner = "DataflowRunner"

    # This tells Dataflow to install your requirements on cloud workers
    options.view_as(SetupOptions).save_main_session = True

    # ── THE SAME 5 TRANSFORMS — IDENTICAL TO DAY 2 ──────────
    with beam.Pipeline(options=options) as pipeline:

        raw_rows = (
            pipeline
            | "Read from GCS" >> beam.io.ReadFromText(INPUT, skip_header_lines=1)
        )

        parsed = (
            raw_rows
            | "Parse Records" >> beam.ParDo(ParseHospitalRecord())
        )

        validation_result = (
            parsed
            | "Validate" >> beam.ParDo(ValidateHospitalRecord()).with_outputs(
                ValidateHospitalRecord.VALID_TAG,
                ValidateHospitalRecord.INVALID_TAG
            )
        )

        valid_records   = validation_result[ValidateHospitalRecord.VALID_TAG]
        invalid_records = validation_result[ValidateHospitalRecord.INVALID_TAG]

        masked_records = (
            valid_records
            | "Mask PII" >> beam.ParDo(MaskPIIFields())
        )

        enriched_records = (
            masked_records
            | "Enrich" >> beam.ParDo(EnrichHospitalRecord())
        )

        anomaly_result = (
            enriched_records
            | "Detect Risk" >> beam.ParDo(DetectHighRiskHospitals()).with_outputs(
                DetectHighRiskHospitals.NORMAL_TAG,
                DetectHighRiskHospitals.ALERT_TAG
            )
        )

        normal_records = anomaly_result[DetectHighRiskHospitals.NORMAL_TAG]
        alert_records  = anomaly_result[DetectHighRiskHospitals.ALERT_TAG]

        # Write to BigQuery — this is new vs Day 2
        # Day 2 wrote to local .jsonl files
        # Day 3 writes directly to BigQuery tables in the cloud
        (
            normal_records
            | "Write Curated to BQ" >> beam.io.WriteToBigQuery(
                CURATED_TABLE,
                schema=CURATED_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        (
            alert_records
            | "Write Alerts to BQ" >> beam.io.WriteToBigQuery(
                ALERTS_TABLE,
                schema=ALERTS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

    logger.info("✅ Job submitted to Dataflow!")
    logger.info(f"Watch it here:")
    logger.info(f"https://console.cloud.google.com/dataflow/jobs?project={PROJECT}")


if __name__ == "__main__":
    run_pipeline()