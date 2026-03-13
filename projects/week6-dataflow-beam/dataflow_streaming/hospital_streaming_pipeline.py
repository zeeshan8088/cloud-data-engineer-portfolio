"""
Week 6 - Day 5: Hospital Streaming Pipeline
Upgrades Day 4 with:
- Watermarks + allowed lateness (handle late arriving data)
- Sliding windows (rolling 3-min view, updated every 1 min)
- TagLateEvents DoFn (data quality pattern)

Key concepts:
- Watermark: Beam's best guess of event time progress
- Allowed lateness: still accept events up to 2 min after window closes
- Sliding windows: overlapping windows for rolling aggregations
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
    AfterWatermark, AfterProcessingTime,
    AfterCount, AccumulationMode
)
import json
import hashlib
from datetime import datetime


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
            message = json.loads(element.decode("utf-8"))

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
            "pipeline_version":  "streaming_v2.0",
        }


# ============================================================
# TRANSFORM 3: Compute window aggregations
# ============================================================

class ComputeWindowStats(beam.DoFn):
    """
    Called once per (hospital, window) combination.
    Input:  ("H001", [event1, event2, event3, ...])
    Output: one summary record with counts and stats
    """

    def process(self, element, window=beam.DoFn.WindowParam):
        hospital_id = element[0]
        events      = list(element[1])

        if not events:
            return

        window_start = datetime.utcfromtimestamp(float(window.start)).isoformat()
        window_end   = datetime.utcfromtimestamp(float(window.end)).isoformat()

        total_admissions = len(events)
        critical_count   = sum(1 for e in events if e.get("is_critical"))
        high_count       = sum(1 for e in events if e.get("severity") == "HIGH")
        avg_severity     = sum(
            e.get("severity_score", 1) for e in events
        ) / total_admissions

        is_surge     = total_admissions > 5
        sample_event = events[0]

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
            "computed_at":        datetime.utcnow().isoformat(),
        }


# ============================================================
# TRANSFORM 4: Tag late arriving events  — NEW Day 5
# ============================================================

class TagLateEvents(beam.DoFn):
    """
    Separates on-time events from late-arriving events.
    Late events need special handling in production —
    reprocessing aggregations, alerting data quality teams,
    or storing separately for audit purposes.
    """
    ONTIME_TAG = "ontime"
    LATE_TAG   = "late"

    def process(self, element, window=beam.DoFn.WindowParam):
        event_time_str = element.get("timestamp", "")

        try:
            from datetime import timezone
            event_dt = datetime.fromisoformat(
                event_time_str.replace("Z", "+00:00")
            )

            if event_dt.timestamp() < float(window.end):
                delay_seconds = float(window.end) - event_dt.timestamp()
                yield beam.pvalue.TaggedOutput(self.LATE_TAG, {
                    **element,
                    "delay_seconds": round(delay_seconds, 2),
                    "late_reason":   f"Event arrived {round(delay_seconds)}s after window close",
                    "window_end":    datetime.utcfromtimestamp(
                        float(window.end)
                    ).isoformat(),
                })
                return
        except Exception:
            pass

        yield beam.pvalue.TaggedOutput(self.ONTIME_TAG, element)


# ============================================================
# PRINT HELPERS
# ============================================================

def print_window_result(record):
    """Pretty print fixed window results."""
    surge = "*** SURGE DETECTED ***" if record.get("is_surge") else "Normal"
    print(
        f"\n{'='*52}\n"
        f"  [FIXED WINDOW] {record['hospital_name']}\n"
        f"  Period  : {record['window_start'][11:]} -> {record['window_end'][11:]}\n"
        f"  Total   : {record['total_admissions']} admissions\n"
        f"  Critical: {record['critical_count']} | High: {record['high_count']}\n"
        f"  Avg Sev : {record['avg_severity_score']} | {surge}\n"
        f"{'='*52}"
    )
    return record


def print_sliding_result(record):
    """Pretty print sliding window results."""
    surge = " <-- SURGE" if record.get("is_surge") else ""
    print(
        f"  [SLIDING] {record['hospital_name']:<25}"
        f" | {record['window_start'][11:16]}->{record['window_end'][11:16]}"
        f" | {record['total_admissions']:3d} admissions"
        f" | avg sev: {record['avg_severity_score']}{surge}"
    )
    return record


# ============================================================
# PIPELINE RUNNER
# ============================================================

def run_streaming_pipeline():
    PROJECT = "intricate-ward-459513-e1"
    REGION  = "us-west1"
    BUCKET  = "zeeshan-hospital-pipeline"
    STAGING = f"gs://{BUCKET}/staging"
    TEMP    = f"gs://{BUCKET}/temp"

    print("=" * 52)
    print("  HOSPITAL STREAMING PIPELINE - Day 5")
    print("  Watermarks + Allowed Lateness + Sliding Windows")
    print("=" * 52)

    options = PipelineOptions()

    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project          = PROJECT
    gcp_options.region           = REGION
    gcp_options.staging_location = STAGING
    gcp_options.temp_location    = TEMP
    gcp_options.job_name         = "hospital-streaming-zeeshan-d5"
    os.environ["BEAM_RUNNER"] = "DirectRunner"
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
                lambda e: print(
                    f"  EVENT: {e['hospital_id']} | {e['severity']:<8} | {e['condition']}"
                )
            )
        )

        # ── FIXED WINDOWS WITH WATERMARK + ALLOWED LATENESS ─
        # Day 5 upgrade from Day 4:
        # - AfterWatermark trigger fires when watermark passes window end
        # - allowed_lateness=120s accepts events up to 2 min late
        # - ACCUMULATING mode: late events add to existing results
        windowed_fixed = (
            enriched_events
            | "Fixed Windows + Watermark" >> beam.WindowInto(
                FixedWindows(5 * 60),
                trigger=AfterWatermark(
                    late=AfterCount(1)
                ),
                allowed_lateness=2 * 60,
                accumulation_mode=AccumulationMode.ACCUMULATING,
            )
        )

        # ── SLIDING WINDOWS ────────────────────────────────
        # Window size: 3 minutes, slide every 1 minute
        # Gives a rolling "last 3 minutes" view updated every minute
        # Perfect for real-time hospital dashboards
        windowed_sliding = (
            enriched_events
            | "Sliding Windows" >> beam.WindowInto(
                SlidingWindows(3 * 60, 1 * 60),
            )
        )

        # ── FIXED WINDOW STATS ─────────────────────────────
        (
            windowed_fixed
            | "Key Fixed by Hospital"    >> beam.Map(lambda e: (e["hospital_id"], e))
            | "Group Fixed by Hospital"  >> beam.GroupByKey()
            | "Compute Fixed Stats"      >> beam.ParDo(ComputeWindowStats())
            | "Print Fixed Stats"        >> beam.Map(print_window_result)
        )

        # ── SLIDING WINDOW STATS ───────────────────────────
        (
            windowed_sliding
            | "Key Sliding by Hospital"   >> beam.Map(lambda e: (e["hospital_id"], e))
            | "Group Sliding by Hospital" >> beam.GroupByKey()
            | "Compute Sliding Stats"     >> beam.ParDo(ComputeWindowStats())
            | "Print Sliding Stats"       >> beam.Map(print_sliding_result)
        )

    print("Pipeline stopped.")


if __name__ == "__main__":
    run_streaming_pipeline()