"""
Unit tests for etl_pipeline.py
Run with: pytest tests/ -v --cov=etl_pipeline --cov-report=term-missing
"""

import os
import sys

import pytest

# noqa: E402 is not needed here because we restructure the path setup
# to use a conftest.py instead — cleaner and ruff-compliant
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_pipeline import transform_order, run_pipeline  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures — reusable test data
# Think of fixtures like prep bowls in a cooking show:
# you measure everything before the show starts so tests stay clean and focused
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_order():
    """A normal, complete order record."""
    return {
        "order_id":   "ORD-TEST-001",
        "customer_id": "C999",
        "items":       3,
        "unit_price":  500.00,
        "status":      "completed",
    }

@pytest.fixture
def high_value_order():
    """An order whose total exceeds the default HIGH_VALUE_THRESHOLD of 1000."""
    return {
        "order_id":    "ORD-TEST-002",
        "customer_id": "C999",
        "items":       5,
        "unit_price":  300.00,   # total = 1500.00 — above threshold
        "status":      "completed",
    }

@pytest.fixture
def low_value_order():
    """An order whose total is below the default HIGH_VALUE_THRESHOLD of 1000."""
    return {
        "order_id":    "ORD-TEST-003",
        "customer_id": "C998",
        "items":       2,
        "unit_price":  100.00,   # total = 200.00 — below threshold
        "status":      "pending",
    }


# ---------------------------------------------------------------------------
# Tests for transform_order()
# ---------------------------------------------------------------------------

class TestTransformOrder:

    def test_total_value_calculated_correctly(self, valid_order):
        """items * unit_price must equal total_value."""
        result = transform_order(valid_order)
        assert result["total_value"] == 1500.00

    def test_high_value_flag_true_when_above_threshold(self, high_value_order):
        """Orders at or above HIGH_VALUE_THRESHOLD must be flagged True."""
        result = transform_order(high_value_order)
        assert result["is_high_value"] is True

    def test_high_value_flag_false_when_below_threshold(self, low_value_order):
        """Orders below HIGH_VALUE_THRESHOLD must be flagged False."""
        result = transform_order(low_value_order)
        assert result["is_high_value"] is False

    def test_status_normalised_to_uppercase(self, valid_order):
        """Status must always be uppercased regardless of input casing."""
        valid_order["status"] = "completed"
        result = transform_order(valid_order)
        assert result["status"] == "COMPLETED"

    def test_status_normalised_mixed_case(self, valid_order):
        """Mixed case input should still produce uppercase output."""
        valid_order["status"] = "Pending"
        result = transform_order(valid_order)
        assert result["status"] == "PENDING"

    def test_output_contains_required_fields(self, valid_order):
        """Transformed record must contain all expected fields."""
        result = transform_order(valid_order)
        required_fields = [
            "order_id", "customer_id", "items", "unit_price",
            "total_value", "is_high_value", "status",
            "processed_at", "environment",
        ]
        for field in required_fields:
            assert field in result, f"Missing field: {field}"

    def test_order_id_preserved(self, valid_order):
        """order_id must pass through unchanged."""
        result = transform_order(valid_order)
        assert result["order_id"] == "ORD-TEST-001"

    def test_total_value_rounded_to_two_decimals(self):
        """Floating point totals must be rounded to 2 decimal places."""
        order = {
            "order_id":    "ORD-TEST-004",
            "customer_id": "C997",
            "items":       3,
            "unit_price":  33.333,   # 3 * 33.333 = 99.999 → should round to 100.00
            "status":      "completed",
        }
        result = transform_order(order)
        assert result["total_value"] == 100.00

    def test_missing_field_raises_error(self):
        """A record missing a required field must raise KeyError or TypeError."""
        bad_order = {"order_id": "ORD-BAD"}   # missing items, unit_price, status
        with pytest.raises((KeyError, TypeError)):
            transform_order(bad_order)


# ---------------------------------------------------------------------------
# Tests for run_pipeline()
# ---------------------------------------------------------------------------

class TestRunPipeline:

    def test_pipeline_returns_correct_count(self):
        """run_pipeline must return one transformed record per valid input."""
        orders = [
            {"order_id": "O1", "customer_id": "C1", "items": 2, "unit_price": 100.0, "status": "completed"},
            {"order_id": "O2", "customer_id": "C2", "items": 1, "unit_price": 200.0, "status": "pending"},
        ]
        results = run_pipeline(orders)
        assert len(results) == 2

    def test_pipeline_empty_input(self):
        """run_pipeline with zero records must return an empty list without crashing."""
        results = run_pipeline([])
        assert results == []

    def test_pipeline_all_statuses_handled(self):
        """Pipeline must handle completed, pending, and cancelled statuses."""
        orders = [
            {"order_id": "O1", "customer_id": "C1", "items": 1, "unit_price": 50.0,  "status": "completed"},
            {"order_id": "O2", "customer_id": "C2", "items": 1, "unit_price": 50.0,  "status": "pending"},
            {"order_id": "O3", "customer_id": "C3", "items": 1, "unit_price": 50.0,  "status": "cancelled"},
        ]
        results = run_pipeline(orders)
        statuses = [r["status"] for r in results]
        assert "COMPLETED" in statuses
        assert "PENDING" in statuses
        assert "CANCELLED" in statuses