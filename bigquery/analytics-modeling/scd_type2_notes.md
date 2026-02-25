# Slowly Changing Dimensions — Type 2 (SCD Type 2)

## Problem
Dimension data changes over time.
Examples:
- Customer changes city
- Product changes category
- Employee changes department

Overwriting data causes loss of history.

---

## SCD Type 2 Solution
SCD Type 2 preserves historical changes.

Instead of updating a record:
- Mark old record as inactive
- Insert a new record
- Keep both records

---

## Example: Customer Dimension

| customer_id | city   | start_date | end_date | is_current |
|------------|--------|------------|----------|------------|
| 101 | Delhi  | 2020-01-01 | 2023-06-01 | false |
| 101 | Mumbai | 2023-06-02 | NULL | true |

---

## Core Rules
1. Never update old dimension rows
2. Close old record using end_date
3. Insert a new record
4. Fact tables join to current dimension row

---

## Why SCD Type 2 Matters
- Enables historical analysis
- Maintains business truth
- Required for accurate reporting