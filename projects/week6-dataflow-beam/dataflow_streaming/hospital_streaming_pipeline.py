"""
Week 6 - Day 6: Hospital Streaming Pipeline
Upgrades Day 5 with:
- BigQuery sink for fixed window stats → windowed_admissions
- BigQuery sink for sliding window stats → sliding_admissions
- Proper TIMESTAMP formatting for BigQuery
"""

import os
import warnings
os.environ["GOOGLE_CLOUD_PROJECT"] = "intricate-ward-459513-e1"
warnings.filterwarnings("ignore")

import logging
logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')
logging.getLogger("apache_beam").setLevel(logging.ERROR)
logging.getLogger("google").setLevel(logging.ERROR)
logging.getLogger("grpc").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("absl").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions,
    StandardOptions, SetupOptions
)
from apache_beam.transforms.window import FixedWindows, SlidingWindows
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterCount, AccumulationMode
)
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import json
import hashlib
from datetime import datetime


# ============================================================
# TRANSFORM 1: Parse Pub/Sub message
# ============================================================

class ParseAdmissionEvent(beam.DoFn):
    VALID_TAG   = "valid"
    INVALID_TAG = "invalid"

    def process(self, element):
        try:
            message = json.loads(element.decode("utf-8"))
            if not message.get("hospital_id"):
                yield beam.pvalue.TaggedOutput(self.INVALID_TAG, {"raw": str(element), "error": "missing_hospital_id"})
                return
            if not message.get("severity"):
                yield beam.pvalue.TaggedOutput(self.INVALID_TAG, {"raw": str(element), "error": "missing_severity"})
                return
            yield beam.pvalue.TaggedOutput(self.VALID_TAG, message)
        except Exception as e:
            yield beam.pvalue.TaggedOutput(self.INVALID_TAG, {"raw": str(element)[:200], "error": str(e)})


# ============================================================
# TRANSFORM 2: Enrich admission event
# ============================================================

class EnrichAdmissionEvent(beam.DoFn):
    SEVERITY_SCORES = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}

    def process(self, element):
        severity_score    = self.SEVERITY_SCORES.get(element.get("severity", "LOW"), 1)
        raw_patient_id    = element.get("patient_id", "")
        masked_patient_id = hashlib.sha256(
            f"salt_zeeshan_{raw_patient_id}".encode()
        ).hexdigest()[:10].upper()

        yield {
            **element,
            "severity_score":    severity_score,
            "patient_id_masked": masked_patient_id,
            "is_critical":       element.get("severity") == "CRITICAL",
            "processed_at":      datetime.utcnow().isoformat(),
            "pipeline_version":  "streaming_v3.0",
        }


# ============================================================
# TRANSFORM 3: Compute window aggregations
# ============================================================

class ComputeWindowStats(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        hospital_id = element[0]
        events      = list(element[1])
        if not events:
            return

        # BigQuery expects TIMESTAMP as "YYYY-MM-DD HH:MM:SS UTC"
        window_start = datetime.utcfromtimestamp(float(window.start)).strftime("%Y-%m-%d %H:%M:%S UTC")
        window_end   = datetime.utcfromtimestamp(float(window.end)).strftime("%Y-%m-%d %H:%M:%S UTC")

        total_admissions = len(events)
        critical_count   = sum(1 for e in events if e.get("is_critical"))
        high_count       = sum(1 for e in events if e.get("severity") == "HIGH")
        avg_severity     = sum(e.get("severity_score", 1) for e in events) / total_admissions
        is_surge         = total_admissions > 5
        sample_event     = events[0]

        yield {
            "hospital_id":        hospital_id,
            "hospital_name":      sample_event.get("hospital_name", ""),
            "state":              sample_event.get("state", ""),
            "window_start":       window_start,
            "window_end":         window_end,
            "total_admissions":   total_admissions,
            "critical_count":     critical_count,
            "high_count":         high_count,
            "avg_severity_score": round(avg_severity, 2),
            "is_surge":           is_surge,
            "surge_reason":       f"{total_admissions} admissions in window" if is_surge else "",
            "computed_at":        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        }


# ============================================================
# PRINT HELPERS
# ============================================================

def print_window_result(record):
    surge = "*** SURGE DETECTED ***" if record.get("is_surge") else "Normal"
    print(
        f"\n{'='*52}\n"
        f"  [FIXED WINDOW] {record['hospital_name']}\n"
        f"  Period  : {record['window_start']} -> {record['window_end']}\n"
        f"  Total   : {record['total_admissions']} admissions\n"
        f"  Critical: {record['critical_count']} | High: {record['high_count']}\n"
        f"  Avg Sev : {record['avg_severity_score']} | {surge}\n"
        f"  → Written to BQ: hospital_streaming.windowed_admissions\n"
        f"{'='*52}"
    )
    return record


def print_sliding_result(record):
    surge = " <-- SURGE" if record.get("is_surge") else ""
    print(
        f"  [SLIDING] {record['hospital_name']:<25}"
        f" | {record['window_start'][11:16]}->{record['window_end'][11:16]}"
        f" | {record['total_admissions']:3d} admissions"
        f" | avg sev: {record['avg_severity_score']}{surge}"
        f" → BQ"
    )
    return record


# ============================================================
# PIPELINE RUNNER
# ============================================================

def run_streaming_pipeline():
    PROJECT  = "intricate-ward-459513-e1"
    REGION   = "us-west1"
    BUCKET   = "zeeshan-hospital-pipeline"
    DATASET  = "hospital_streaming"
    STAGING  = f"gs://{BUCKET}/staging"
    TEMP     = f"gs://{BUCKET}/temp"

    BQ_FIXED   = f"{PROJECT}:{DATASET}.windowed_admissions"
    BQ_SLIDING = f"{PROJECT}:{DATASET}.sliding_admissions"

    print("=" * 52)
    print("  HOSPITAL STREAMING PIPELINE - Day 6")
    print("  BigQuery Sink: windowed_admissions + sliding_admissions")
    print("=" * 52)

    options = PipelineOptions()

    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project          = PROJECT
    gcp_options.region           = REGION
    gcp_options.staging_location = STAGING
    gcp_options.temp_location    = TEMP
    gcp_options.job_name         = "hospital-streaming-zeeshan-d6"

    options.view_as(StandardOptions).runner    = "DirectRunner"
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as pipeline:

        # ── READ FROM PUB/SUB ──────────────────────────────
        raw_messages = (
            pipeline
            | "Read from PubSub" >> beam.io.ReadFromPubSub(
                subscription=f"projects/{PROJECT}/subscriptions/hospital-admissions-sub",
            )
        )

        # ── PARSE & VALIDATE ───────────────────────────────
        parsed = (
            raw_messages
            | "Parse Events" >> beam.ParDo(
                ParseAdmissionEvent()
            ).with_outputs(
                ParseAdmissionEvent.VALID_TAG,
                ParseAdmissionEvent.INVALID_TAG
            )
        )

        valid_events = parsed[ParseAdmissionEvent.VALID_TAG]

        # ── ENRICH ─────────────────────────────────────────
        enriched_events = (
            valid_events
            | "Enrich Events" >> beam.ParDo(EnrichAdmissionEvent())
        )

        # ── LOG EACH EVENT ─────────────────────────────────
        (
            enriched_events
            | "Log Events" >> beam.Map(
                lambda e: print(f"  EVENT: {e['hospital_id']} | {e['severity']:<8} | {e['condition']}")
            )
        )

        # ── FIXED WINDOWS ──────────────────────────────────
        windowed_fixed = (
            enriched_events
            | "Fixed Windows" >> beam.WindowInto(
                FixedWindows(5 * 60),
                trigger=AfterWatermark(late=AfterCount(1)),
                allowed_lateness=2 * 60,
                accumulation_mode=AccumulationMode.ACCUMULATING,
            )
        )

        # ── SLIDING WINDOWS ────────────────────────────────
        windowed_sliding = (
            enriched_events
            | "Sliding Windows" >> beam.WindowInto(
                SlidingWindows(3 * 60, 1 * 60),
            )
        )

        # ── FIXED WINDOW STATS → CONSOLE + BIGQUERY ────────
        fixed_stats = (
            windowed_fixed
            | "Key Fixed by Hospital"   >> beam.Map(lambda e: (e["hospital_id"], e))
            | "Group Fixed by Hospital" >> beam.GroupByKey()
            | "Compute Fixed Stats"     >> beam.ParDo(ComputeWindowStats())
            | "Print Fixed Stats"       >> beam.Map(print_window_result)
        )

        (
            fixed_stats
            | "Write Fixed to BQ" >> WriteToBigQuery(
                table=BQ_FIXED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
            )
        )

        # ── SLIDING WINDOW STATS → CONSOLE + BIGQUERY ──────
        sliding_stats = (
            windowed_sliding
            | "Key Sliding by Hospital"   >> beam.Map(lambda e: (e["hospital_id"], e))
            | "Group Sliding by Hospital" >> beam.GroupByKey()
            | "Compute Sliding Stats"     >> beam.ParDo(ComputeWindowStats())
            | "Print Sliding Stats"       >> beam.Map(print_sliding_result)
        )

        (
            sliding_stats
            | "Write Sliding to BQ" >> WriteToBigQuery(
                table=BQ_SLIDING,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
            )
        )

    print("Pipeline stopped.")


if __name__ == "__main__":
    run_streaming_pipeline()