import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import json
import logging
import hashlib
import re
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class ParseHospitalRecord(beam.DoFn):
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


def run_pipeline(input_file, output_dir="output"):
    os.makedirs(output_dir, exist_ok=True)

    logger.info("=" * 60)
    logger.info("HOSPITAL DATA PIPELINE - Week 6 Day 2")
    logger.info(f"Input:  {input_file}")
    logger.info(f"Runner: DirectRunner (local Docker)")
    logger.info("=" * 60)

    options = PipelineOptions(["--runner=DirectRunner"])

    with beam.Pipeline(options=options) as pipeline:

        raw_rows = (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
        )

        parsed = (
            raw_rows
            | "Parse Records" >> beam.ParDo(ParseHospitalRecord())
        )

        validation_result = (
            parsed
            | "Validate" >> beam.ParDo(ValidateHospitalRecord()).with_outputs(
                ValidateHospitalRecord.VALID_TAG,
                ValidateHospitalRecord.INVALID_TAG,
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
                DetectHighRiskHospitals.ALERT_TAG,
            )
        )

        normal_records = anomaly_result[DetectHighRiskHospitals.NORMAL_TAG]
        alert_records  = anomaly_result[DetectHighRiskHospitals.ALERT_TAG]

        (
            normal_records
            | "Format Normal" >> beam.Map(json.dumps)
            | "Write Curated" >> beam.io.WriteToText(
                f"{output_dir}/curated_hospitals", file_name_suffix=".jsonl"
            )
        )

        (
            alert_records
            | "Format Alerts" >> beam.Map(json.dumps)
            | "Write Alerts" >> beam.io.WriteToText(
                f"{output_dir}/hospital_alerts", file_name_suffix=".jsonl"
            )
        )

        (
            invalid_records
            | "Format Invalid" >> beam.Map(json.dumps)
            | "Write Dead Letter" >> beam.io.WriteToText(
                f"{output_dir}/dead_letter", file_name_suffix=".jsonl"
            )
        )

    logger.info("PIPELINE COMPLETE - counting results...")

    import glob
    for pattern, label in [
        (f"{output_dir}/curated_hospitals*.jsonl", "Curated records "),
        (f"{output_dir}/hospital_alerts*.jsonl",   "Alert records   "),
        (f"{output_dir}/dead_letter*.jsonl",        "Dead letter     "),
    ]:
        total = sum(
            sum(1 for _ in open(fp)) for fp in glob.glob(pattern)
        )
        logger.info(f"  {label}: {total}")

    alert_files = glob.glob(f"{output_dir}/hospital_alerts*.jsonl")
    if alert_files:
        with open(alert_files[0]) as f:
            line = f.readline()
            if line:
                sample = json.loads(line)
                logger.info(f"\nSample HIGH RISK hospital:")
                logger.info(f"  Name:      {sample.get('facility_name')}")
                logger.info(f"  State:     {sample.get('state')}")
                logger.info(f"  Risk Score:{sample.get('risk_score')}")
                logger.info(f"  Tier:      {sample.get('quality_tier')}")

    logger.info("=" * 60)
    logger.info("Day 2 complete - local pipeline working!")
    logger.info("Next: Day 3 - run this on GCP Dataflow")


if __name__ == "__main__":
    run_pipeline(
        input_file="data/hospitals_raw.csv",
        output_dir="output"
    )