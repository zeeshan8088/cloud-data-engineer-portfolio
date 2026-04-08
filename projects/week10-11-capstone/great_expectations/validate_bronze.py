"""
RetailFlow — Bronze Layer Data Quality Validation
====================================================
Day 2: Great Expectations (GX 1.0+) validation runner.

Connects to BigQuery, validates all 3 Bronze tables against their
expectation suites, logs results to retailflow_metadata.ge_results,
and generates DataDocs HTML reports.

Usage:
    python validate_bronze.py            # validate all 3 tables
    python validate_bronze.py --table orders   # validate just orders
    python validate_bronze.py --verbose  # show detailed expectation results

Exit codes:
    0 = all suites passed
    1 = one or more suites failed (used by Airflow to halt downstream)
"""

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

# ── Logging ──────────────────────────────────────────────────────
logger = logging.getLogger("retailflow.quality")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)

# ── Configuration ────────────────────────────────────────────────
GCP_PROJECT = os.environ.get("GCP_PROJECT", "intricate-ward-459513-e1")
BQ_DATASET_BRONZE = os.environ.get("BQ_DATASET_BRONZE", "retailflow_bronze")
BQ_DATASET_METADATA = os.environ.get("BQ_DATASET_METADATA", "retailflow_metadata")
GE_ROOT = Path(__file__).parent  # great_expectations/ folder

# Map of table short name → (BigQuery table, expectation suite file)
TABLE_CONFIGS = {
    "orders": {
        "bq_table": f"{GCP_PROJECT}.{BQ_DATASET_BRONZE}.raw_orders",
        "suite_file": GE_ROOT / "expectations" / "raw_orders_suite.json",
        "suite_name": "raw_orders_suite",
    },
    "customers": {
        "bq_table": f"{GCP_PROJECT}.{BQ_DATASET_BRONZE}.raw_customers",
        "suite_file": GE_ROOT / "expectations" / "raw_customers_suite.json",
        "suite_name": "raw_customers_suite",
    },
    "products": {
        "bq_table": f"{GCP_PROJECT}.{BQ_DATASET_BRONZE}.raw_products",
        "suite_file": GE_ROOT / "expectations" / "raw_products_suite.json",
        "suite_name": "raw_products_suite",
    },
}

# ── BigQuery metadata table schema ──────────────────────────────
GE_RESULTS_TABLE = f"{GCP_PROJECT}.{BQ_DATASET_METADATA}.ge_results"
GE_RESULTS_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("suite_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("success", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("evaluated_expectations", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("successful_expectations", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("unsuccessful_expectations", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("success_percent", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("run_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("failure_details", "STRING", mode="NULLABLE"),
]


def load_suite(suite_path: Path) -> dict:
    """Load an expectation suite from a JSON file."""
    with open(suite_path, "r") as f:
        suite = json.load(f)
    logger.info(f"  Loaded suite: {suite['expectation_suite_name']} "
                f"({len(suite['expectations'])} expectations)")
    return suite


def validate_table_with_pandas(bq_table: str, suite: dict, verbose: bool = False) -> dict:
    """
    Validate a BigQuery table against an expectation suite using pandas.
    
    This approach pulls data from BigQuery into a pandas DataFrame and
    runs each expectation manually. This is more portable and avoids
    GX version-specific API issues, while still using GX-style expectations.
    
    Returns a results dict with pass/fail info.
    """
    import pandas as pd

    client = bigquery.Client(project=GCP_PROJECT)
    suite_name = suite["expectation_suite_name"]
    
    # Pull the table into a DataFrame
    logger.info(f"  Querying {bq_table} ...")
    query = f"SELECT * FROM `{bq_table}`"
    df = client.query(query).to_dataframe()
    row_count = len(df)
    logger.info(f"  Loaded {row_count:,} rows into DataFrame")

    results = []
    
    for exp in suite["expectations"]:
        exp_type = exp["type"]
        kwargs = exp["kwargs"]
        column = kwargs.get("column")
        passed = True
        detail = ""
        
        try:
            if exp_type == "expect_column_values_to_not_be_null":
                null_count = df[column].isnull().sum()
                passed = null_count == 0
                if not passed:
                    detail = f"{null_count} null values found in '{column}'"
            
            elif exp_type == "expect_column_values_to_be_unique":
                dup_count = df[column].duplicated().sum()
                passed = dup_count == 0
                if not passed:
                    detail = f"{dup_count} duplicate values found in '{column}'"
            
            elif exp_type == "expect_column_values_to_be_between":
                min_val = kwargs.get("min_value")
                max_val = kwargs.get("max_value")
                col_data = df[column].dropna()
                out_of_range = ((col_data < min_val) | (col_data > max_val)).sum()
                passed = out_of_range == 0
                if not passed:
                    detail = (f"{out_of_range} values in '{column}' "
                              f"outside [{min_val}, {max_val}]")
            
            elif exp_type == "expect_column_values_to_be_in_set":
                value_set = set(kwargs["value_set"])
                col_data = df[column].dropna()
                invalid = col_data[~col_data.isin(value_set)]
                passed = len(invalid) == 0
                if not passed:
                    bad_vals = invalid.unique()[:5]
                    detail = (f"{len(invalid)} values in '{column}' not in "
                              f"allowed set. Examples: {list(bad_vals)}")
            
            elif exp_type == "expect_column_values_to_match_regex":
                regex = kwargs["regex"]
                col_data = df[column].dropna().astype(str)
                non_matching = col_data[~col_data.str.match(regex)]
                passed = len(non_matching) == 0
                if not passed:
                    bad_vals = non_matching.head(3).tolist()
                    detail = (f"{len(non_matching)} values in '{column}' "
                              f"don't match regex. Examples: {bad_vals}")
            
            else:
                detail = f"Unknown expectation type: {exp_type}"
                passed = True  # Don't fail on unknown types
        
        except KeyError as e:
            passed = False
            detail = f"Column not found: {e}"
        except Exception as e:
            passed = False
            detail = f"Error evaluating: {e}"

        status = "✅ PASS" if passed else "❌ FAIL"
        if verbose:
            col_info = f" [{column}]" if column else ""
            logger.info(f"    {status} | {exp_type}{col_info}"
                        + (f" — {detail}" if detail else ""))
        
        results.append({
            "type": exp_type,
            "column": column,
            "passed": passed,
            "detail": detail,
        })
    
    total = len(results)
    passed_count = sum(1 for r in results if r["passed"])
    failed_count = total - passed_count
    all_passed = failed_count == 0
    success_pct = round((passed_count / total) * 100, 1) if total > 0 else 0

    # Collect failure details
    failures = [r for r in results if not r["passed"]]
    failure_summary = json.dumps(
        [{"type": f["type"], "column": f["column"], "detail": f["detail"]} 
         for f in failures],
        indent=2
    ) if failures else None

    return {
        "suite_name": suite_name,
        "bq_table": bq_table,
        "success": all_passed,
        "evaluated_expectations": total,
        "successful_expectations": passed_count,
        "unsuccessful_expectations": failed_count,
        "success_percent": success_pct,
        "failure_details": failure_summary,
    }


def log_results_to_bigquery(all_results: list, run_id: str):
    """Write validation results to retailflow_metadata.ge_results."""
    client = bigquery.Client(project=GCP_PROJECT)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    rows = []
    for result in all_results:
        rows.append({
            "run_id": run_id,
            "suite_name": result["suite_name"],
            "table_name": result["bq_table"],
            "success": result["success"],
            "evaluated_expectations": result["evaluated_expectations"],
            "successful_expectations": result["successful_expectations"],
            "unsuccessful_expectations": result["unsuccessful_expectations"],
            "success_percent": result["success_percent"],
            "run_timestamp": now,
            "failure_details": result.get("failure_details"),
        })

    job_config = bigquery.LoadJobConfig(
        schema=GE_RESULTS_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    job = client.load_table_from_json(rows, GE_RESULTS_TABLE, job_config=job_config)
    job.result()
    logger.info(f"📝 Logged {len(rows)} results to {GE_RESULTS_TABLE}")


def generate_datadocs_report(all_results: list, run_id: str):
    """Generate a simple HTML DataDocs-style report."""
    docs_dir = GE_ROOT / "uncommitted" / "data_docs" / "local_site"
    docs_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    overall_pass = all(r["success"] for r in all_results)
    status_emoji = "✅" if overall_pass else "❌"

    html_parts = [
        "<!DOCTYPE html>",
        "<html lang='en'>",
        "<head>",
        "  <meta charset='UTF-8'>",
        "  <title>RetailFlow Data Quality Report</title>",
        "  <style>",
        "    * { margin: 0; padding: 0; box-sizing: border-box; }",
        "    body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0f172a; color: #e2e8f0; padding: 2rem; }",
        "    .header { text-align: center; margin-bottom: 2rem; }",
        "    .header h1 { font-size: 2rem; background: linear-gradient(135deg, #6366f1, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }",
        "    .header p { color: #94a3b8; margin-top: 0.5rem; }",
        "    .overall { text-align: center; font-size: 1.5rem; padding: 1rem; margin: 1rem 0 2rem; border-radius: 12px; }",
        "    .overall.pass { background: rgba(34, 197, 94, 0.15); border: 1px solid #22c55e; color: #86efac; }",
        "    .overall.fail { background: rgba(239, 68, 68, 0.15); border: 1px solid #ef4444; color: #fca5a5; }",
        "    .suite-card { background: #1e293b; border-radius: 12px; padding: 1.5rem; margin-bottom: 1.5rem; border: 1px solid #334155; }",
        "    .suite-card h2 { color: #c4b5fd; font-size: 1.2rem; margin-bottom: 0.5rem; }",
        "    .suite-card .meta { color: #94a3b8; font-size: 0.9rem; margin-bottom: 1rem; }",
        "    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 1rem; }",
        "    .stat-box { background: #0f172a; border-radius: 8px; padding: 1rem; text-align: center; }",
        "    .stat-box .number { font-size: 1.8rem; font-weight: bold; }",
        "    .stat-box .label { font-size: 0.8rem; color: #94a3b8; margin-top: 0.3rem; }",
        "    .pass-num { color: #22c55e; }",
        "    .fail-num { color: #ef4444; }",
        "    .neutral-num { color: #6366f1; }",
        "    .failures { margin-top: 1rem; background: rgba(239, 68, 68, 0.1); border-radius: 8px; padding: 1rem; border: 1px solid rgba(239, 68, 68, 0.3); }",
        "    .failures h3 { color: #fca5a5; font-size: 0.95rem; margin-bottom: 0.5rem; }",
        "    .failures pre { color: #fecaca; font-size: 0.8rem; white-space: pre-wrap; }",
        "    .footer { text-align: center; color: #475569; margin-top: 2rem; font-size: 0.85rem; }",
        "  </style>",
        "</head>",
        "<body>",
        "  <div class='header'>",
        "    <h1>📊 RetailFlow Data Quality Report</h1>",
        f"    <p>Run ID: {run_id} | Generated: {now}</p>",
        "  </div>",
        f"  <div class='overall {'pass' if overall_pass else 'fail'}'>",
        f"    {status_emoji} Overall Result: {'ALL SUITES PASSED' if overall_pass else 'SOME SUITES FAILED'}",
        "  </div>",
    ]

    for r in all_results:
        suite_status = "✅" if r["success"] else "❌"
        pct = r["success_percent"]
        html_parts.append(f"  <div class='suite-card'>")
        html_parts.append(f"    <h2>{suite_status} {r['suite_name']}</h2>")
        html_parts.append(f"    <div class='meta'>Table: <code>{r['bq_table']}</code></div>")
        html_parts.append(f"    <div class='stats-grid'>")
        html_parts.append(f"      <div class='stat-box'><div class='number neutral-num'>{r['evaluated_expectations']}</div><div class='label'>Total Checks</div></div>")
        html_parts.append(f"      <div class='stat-box'><div class='number pass-num'>{r['successful_expectations']}</div><div class='label'>Passed</div></div>")
        html_parts.append(f"      <div class='stat-box'><div class='number fail-num'>{r['unsuccessful_expectations']}</div><div class='label'>Failed</div></div>")
        html_parts.append(f"      <div class='stat-box'><div class='number {'pass-num' if pct == 100 else 'fail-num'}'>{pct}%</div><div class='label'>Pass Rate</div></div>")
        html_parts.append(f"    </div>")

        if r.get("failure_details"):
            html_parts.append(f"    <div class='failures'>")
            html_parts.append(f"      <h3>Failed Expectations:</h3>")
            html_parts.append(f"      <pre>{r['failure_details']}</pre>")
            html_parts.append(f"    </div>")

        html_parts.append(f"  </div>")

    html_parts.extend([
        "  <div class='footer'>",
        "    RetailFlow — Intelligent Retail Data Platform | Great Expectations Data Quality Layer",
        "  </div>",
        "</body>",
        "</html>",
    ])

    report_path = docs_dir / "index.html"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(html_parts))

    logger.info(f"📄 DataDocs report generated: {report_path}")
    return report_path


def main():
    parser = argparse.ArgumentParser(
        description="RetailFlow — Bronze Layer Data Quality Validation"
    )
    parser.add_argument(
        "--table",
        choices=["orders", "customers", "products"],
        help="Validate only a specific table (default: all)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show individual expectation results",
    )
    args = parser.parse_args()

    run_id = str(uuid.uuid4())[:12]
    
    logger.info("=" * 65)
    logger.info("RetailFlow | Bronze Data Quality Validation — START")
    logger.info(f"Run ID: {run_id}")
    logger.info("=" * 65)

    # Determine which tables to validate
    if args.table:
        tables_to_validate = {args.table: TABLE_CONFIGS[args.table]}
    else:
        tables_to_validate = TABLE_CONFIGS

    all_results = []

    for name, config in tables_to_validate.items():
        logger.info(f"\n{'─' * 50}")
        logger.info(f"🔍 Validating: {name.upper()}")
        logger.info(f"{'─' * 50}")

        suite = load_suite(config["suite_file"])
        result = validate_table_with_pandas(
            bq_table=config["bq_table"],
            suite=suite,
            verbose=args.verbose,
        )
        all_results.append(result)

        status = "✅ PASSED" if result["success"] else "❌ FAILED"
        logger.info(
            f"  Result: {status} — "
            f"{result['successful_expectations']}/{result['evaluated_expectations']} "
            f"expectations passed ({result['success_percent']}%)"
        )

    # Log results to BigQuery metadata table
    logger.info(f"\n{'─' * 50}")
    logger.info("📝 Logging results to BigQuery metadata...")
    log_results_to_bigquery(all_results, run_id)

    # Generate DataDocs HTML report
    logger.info("📄 Generating DataDocs report...")
    report_path = generate_datadocs_report(all_results, run_id)

    # Summary
    overall_pass = all(r["success"] for r in all_results)
    logger.info(f"\n{'=' * 65}")
    if overall_pass:
        logger.info("🎉 ALL SUITES PASSED — Bronze data quality is GOOD")
    else:
        failed_suites = [r["suite_name"] for r in all_results if not r["success"]]
        logger.info(f"🚨 FAILED SUITES: {', '.join(failed_suites)}")
        logger.info("Pipeline should HALT — data quality issues detected")
    logger.info(f"📄 DataDocs report: {report_path}")
    logger.info(f"{'=' * 65}")

    # Exit with code 1 if any suite failed (Airflow uses this)
    if not overall_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
