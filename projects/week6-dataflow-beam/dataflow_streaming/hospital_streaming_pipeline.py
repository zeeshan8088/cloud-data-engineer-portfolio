"""
Week 6 - Day 4: Hospital Streaming Pipeline
Reads live admission events from Pub/Sub
Applies 5-minute fixed windows
Detects admission surges
Writes to BigQuery in real time

Key difference from batch:
- Source: Pub/Sub (infinite stream) instead of CSV (finite file)
- Windows: group events into 5-minute buckets
- Output: writes continuously, not at the end
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions,
    StandardOptions, SetupOptions
)
from apache_beam.transforms.window import FixedWindows, SlidingWindows
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterProcessingTime, AccumulationMode
)
import json
import logging
import hashlib
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


# ============================================================
# TRANSFORM 1: Parse Pub/Sub message
# ============================================================

class ParseAdmissionEvent(beam.DoFn):
    """
    Pub/Sub delivers messages as raw bytes.
    We decode and parse them into Python dicts.
    
    Think of it like opening an envelope and reading the letter.
    """
    VALID_TAG   = "valid"
    INVALID_TAG = "invalid"

    def process(self, element):
        try:
            # element is raw bytes from Pub/Sub — decode it
            message = json.loads(element.decode("utf-8"))

            # Basic validation
            if not message.get("hospital_id"):
                yield beam.pvalue.TaggedOutput(self.INVALID_TAG, {
                    "raw": str(element),
                    "error": "missing_hospital_id"
                })
                return

            if not message.get("severity"):
                yield beam.pvalue.TaggedOutput(self.INVALID_TAG, {
                    "raw": str(element),
                    "error": "missing_severity"
                })
                return

            yield beam.pvalue.TaggedOutput(self.VALID_TAG, message)

        except Exception as e:
            yield beam.pvalue.TaggedOutput(self.INVALID_TAG, {
                "raw": str(element)[:200],
                "error": str(e)
            })


# ============================================================
# TRANSFORM 2: Enrich admission event
# ============================================================

class EnrichAdmissionEvent(beam.DoFn):
    """
    Add derived fields to each admission event.
    Same concept as Day 2 enrichment — just on streaming data.
    """
    SEVERITY_SCORES = {
        "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4
    }

    def process(self, element):
        severity_score = self.SEVERITY_SCORES.get(
            element.get("severity", "LOW"), 1
        )

        # Mask patient ID for privacy
        raw_patient_id = element.get("patient_id", "")
        masked_patient_id = hashlib.sha256(
            f"salt_zeeshan_{raw_patient_id}".encode()
        ).hexdigest()[:10].upper()

        yield {
            **element,
            "severity_score":    severity_score,
            "patient_id_masked": masked_patient_id,
            "is_critical":       element.get("severity") == "CRITICAL",
            "processed_at":      datetime.utcnow().isoformat(),
            "pipeline_version":  "streaming_v1.0",
        }


# ============================================================
# TRANSFORM 3: Compute window aggregations
# ============================================================

class ComputeWindowStats(beam.DoFn):
    """
    Called once per (hospital, window) combination.
    
    Input:  ("H001", [event1, event2, event3, ...])
            ← all events for H001 in this 5-min window
    
    Output: one summary record with counts and stats
    
    Analogy: At the end of each 5-minute shift slot,
    a nurse summarizes: "H001 had 8 admissions,
    2 critical, average severity 2.5"
    """

    def process(self, element, window=beam.DoFn.WindowParam):
        hospital_id = element[0]
        events      = list(element[1])

        if not events:
            return

        # Window timing
        window_start = datetime.utcfromtimestamp(
            float(window.start)
        ).isoformat()
        window_end = datetime.utcfromtimestamp(
            float(window.end)
        ).isoformat()

        # Compute stats
        total_admissions  = len(events)
        critical_count    = sum(1 for e in events if e.get("is_critical"))
        high_count        = sum(1 for e in events if e.get("severity") == "HIGH")
        avg_severity      = sum(
            e.get("severity_score", 1) for e in events
        ) / total_admissions

        # Surge detection: >5 admissions in 5 minutes = surge
        is_surge = total_admissions > 5

        sample_event = events[0]

        yield {
            "hospital_id":       hospital_id,
            "hospital_name":     sample_event.get("hospital_name", ""),
            "state":             sample_event.get("state", ""),
            "window_start":      window_start,
            "window_end":        window_end,
            "total_admissions":  total_admissions,
            "critical_count":    critical_count,
            "high_count":        high_count,
            "avg_severity_score": round(avg_severity, 2),
            "is_surge":          is_surge,
            "surge_reason":      f"{total_admissions} admissions in 5 min" if is_surge else "",
            "computed_at":       datetime.utcnow().isoformat(),
        }


# ============================================================
# BIGQUERY SCHEMAS
# ============================================================

WINDOWED_ADMISSIONS_SCHEMA = {
    "fields": [
        {"name": "hospital_id",         "type": "STRING"},
        {"name": "hospital_name",       "type": "STRING"},
        {"name": "state",               "type": "STRING"},
        {"name": "window_start",        "type": "STRING"},
        {"name": "window_end",          "type": "STRING"},
        {"name": "total_admissions",    "type": "INTEGER"},
        {"name": "critical_count",      "type": "INTEGER"},
        {"name": "high_count",          "type": "INTEGER"},
        {"name": "avg_severity_score",  "type": "FLOAT"},
        {"name": "is_surge",            "type": "BOOLEAN"},
        {"name": "surge_reason",        "type": "STRING"},
        {"name": "computed_at",         "type": "STRING"},
    ]
}

RAW_EVENTS_SCHEMA = {
    "fields": [
        {"name": "event_id",          "type": "STRING"},
        {"name": "patient_id_masked", "type": "STRING"},
        {"name": "hospital_id",       "type": "STRING"},
        {"name": "hospital_name",     "type": "STRING"},
        {"name": "state",             "type": "STRING"},
        {"name": "condition",         "type": "STRING"},
        {"name": "severity",          "type": "STRING"},
        {"name": "severity_score",    "type": "INTEGER"},
        {"name": "is_emergency",      "type": "BOOLEAN"},
        {"name": "is_critical",       "type": "BOOLEAN"},
        {"name": "timestamp",         "type": "STRING"},
        {"name": "processed_at",      "type": "STRING"},
        {"name": "pipeline_version",  "type": "STRING"},
    ]
}


# ============================================================
# PIPELINE RUNNER
# ============================================================

def run_streaming_pipeline():
    PROJECT      = "intricate-ward-459513-e1"
    REGION       = "us-west1"
    BUCKET       = "zeeshan-hospital-pipeline"
    PUBSUB_TOPIC = f"projects/{PROJECT}/topics/hospital-admissions"
    STAGING      = f"gs://{BUCKET}/staging"
    TEMP         = f"gs://{BUCKET}/temp"

    WINDOWED_TABLE = f"{PROJECT}:hospital_streaming.windowed_admissions"
    RAW_TABLE      = f"{PROJECT}:hospital_streaming.raw_events"

    logger.info("=" * 60)
    logger.info("HOSPITAL STREAMING PIPELINE - Day 4")
    logger.info(f"Source: {PUBSUB_TOPIC}")
    logger.info(f"Window: 5-minute fixed windows")
    logger.info(f"Output: BigQuery → hospital_streaming")
    logger.info("=" * 60)

    options = PipelineOptions()

    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project          = PROJECT
    gcp_options.region           = REGION
    gcp_options.staging_location = STAGING
    gcp_options.temp_location    = TEMP
    gcp_options.job_name         = "hospital-streaming-zeeshan"

    # STREAMING = True is the key flag
    # This tells Beam: data never ends, keep running forever
    options.view_as(StandardOptions).runner    = "DirectRunner"

    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as pipeline:

        # ── READ FROM PUB/SUB ──────────────────────────────
        # This is the ONLY line different from batch
        # Instead of ReadFromText(file), we ReadFromPubSub(topic)
        raw_messages = (
            pipeline
            | "Read from PubSub" >> beam.io.ReadFromPubSub(
                subscription=f"projects/intricate-ward-459513-e1/subscriptions/hospital-admissions-sub"

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

        valid_events   = parsed[ParseAdmissionEvent.VALID_TAG]
        invalid_events = parsed[ParseAdmissionEvent.INVALID_TAG]

        # ── ENRICH ─────────────────────────────────────────
        enriched_events = (
            valid_events
            | "Enrich Events" >> beam.ParDo(EnrichAdmissionEvent())
        )

        # ── WRITE RAW EVENTS TO BIGQUERY ───────────────────
        # Every single event stored individually
        (
            enriched_events
            | "Log Raw Events" >> beam.Map(
                lambda e: print(f"  EVENT: {e['hospital_id']} | {e['severity']} | {e['condition']}")
            )
        )

        # ── APPLY 5-MINUTE FIXED WINDOWS ───────────────────
        # This is the CORE streaming concept
        # Group events into 5-minute buckets
        #
        # Time:   0────5────10────15────20  (minutes)
        # Window: [─W1─][─W2─][─W3─][─W4─]
        #
        windowed_events = (
            enriched_events
            | "Apply 5-min Windows" >> beam.WindowInto(
                FixedWindows(5 * 60),  # 300 seconds = 5 minutes
            )
        )

        # ── GROUP BY HOSPITAL IN EACH WINDOW ───────────────
        # For each 5-minute window, group events by hospital
        # Result: ("H001", [event1, event2, ...]) per window
        keyed_events = (
            windowed_events
            | "Key by Hospital" >> beam.Map(
                lambda e: (e["hospital_id"], e)
            )
            | "Group by Hospital" >> beam.GroupByKey()
        )

        # ── COMPUTE WINDOW STATISTICS ───────────────────────
        # For each (hospital, window): count, severity, surge flag
        window_stats = (
            keyed_events
            | "Compute Stats" >> beam.ParDo(ComputeWindowStats())
        )

# ── PRINT WINDOW STATS (local mode) ────────────────
        (
            window_stats
            | "Print Window Stats" >> beam.Map(lambda s: print(
                f"\n{'='*50}\n"
                f"  WINDOW CLOSED: {s['hospital_name']}\n"
                f"  Period: {s['window_start']} → {s['window_end']}\n"
                f"  Admissions: {s['total_admissions']} | Critical: {s['critical_count']} | High: {s['high_count']}\n"
                f"  Avg Severity: {s['avg_severity_score']} | SURGE: {s['is_surge']}\n"
                f"{'='*50}"
            ))
        )

    logger.info("✅ Streaming job submitted!")
    logger.info(f"Monitor: https://console.cloud.google.com/dataflow/jobs?project={PROJECT}")


if __name__ == "__main__":
    run_streaming_pipeline()