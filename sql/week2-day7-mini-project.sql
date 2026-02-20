🧩 STEP 1 — Understand the table (5–10 min)

Run this (just to see columns):

SELECT *
FROM `bigquery-public-data.stackoverflow.posts_questions`
LIMIT 10;

Important columns to notice:

id

creation_date

score

view_count

answer_count

tags

🧩 STEP 2 — Question 1 (Aggregation)
❓ Question

How many questions are asked per year?

SQL
SELECT
  EXTRACT(YEAR FROM creation_date) AS year,
  COUNT(*) AS total_questions
FROM `bigquery-public-data.stackoverflow.posts_questions`
GROUP BY year
ORDER BY year;

👉 Uses:

aggregation

date extraction

cost-aware column selection

🧩 STEP 3 — Question 2 (Filtering + Aggregation)
❓ Question

Which questions get high engagement?

SQL
SELECT
  id,
  score,
  view_count,
  answer_count
FROM `bigquery-public-data.stackoverflow.posts_questions`
WHERE view_count > 10000
ORDER BY view_count DESC
LIMIT 20;

👉 Uses:

filtering

ordering

limit (very important in BigQuery)

🧩 STEP 4 — Question 3 (Window Function)
❓ Question

Rank questions by views per year

SQL
WITH ranked_questions AS (
  SELECT
    id,
    EXTRACT(YEAR FROM creation_date) AS year,
    view_count,
    RANK() OVER (
      PARTITION BY EXTRACT(YEAR FROM creation_date)
      ORDER BY view_count DESC
    ) AS rank_in_year
  FROM `bigquery-public-data.stackoverflow.posts_questions`
)
SELECT *
FROM ranked_questions
WHERE rank_in_year <= 3
ORDER BY year, rank_in_year;

👉 Uses:

CTE

window function

PARTITION BY

real analytics logic

🧩 STEP 5 — Cost Awareness (VERY IMPORTANT)

Before running queries:

Check bytes processed

Notice:

We avoided SELECT *

We filtered early

We used LIMIT where needed

This is senior behavior.