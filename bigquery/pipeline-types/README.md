# Week 3 Day 5 — Batch vs Event vs Streaming Pipelines

## Core Idea
Pipeline types differ mainly by latency requirements.

---

## Batch Pipelines
- Process data at scheduled intervals
- Cheapest and simplest
- Used for analytics and reporting

Latency: Hours to days

---

## Event Pipelines
- Triggered by individual events
- Faster than batch
- Balanced cost and speed

Latency: Seconds to minutes

---

## Streaming Pipelines
- Continuous processing
- Near real-time results
- Expensive and complex

Latency: Milliseconds to seconds

---

## Key Rule
Choose pipeline type based on how late data can be before it loses value.