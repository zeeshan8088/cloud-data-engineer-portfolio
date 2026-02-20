# Week 2 Mini Analytics Project – StackOverflow Questions

## Dataset
bigquery-public-data.stackoverflow.posts_questions

## Objective
Analyze StackOverflow questions to understand activity trends and engagement patterns.

## Key Insights

### 1. Question volume over time
Using aggregation by year, we observed steady growth in the number of questions, showing increasing platform adoption.

### 2. High engagement questions
Questions with very high view counts often receive multiple answers and higher scores, indicating strong community interest.

### 3. Top questions per year
Using window functions, we ranked the most viewed questions each year, highlighting consistently high-impact content.

## Technical Skills Demonstrated
- Cost-aware BigQuery SQL
- Aggregations and GROUP BY
- Window functions with PARTITION BY
- CTE-based modular SQL design

## Conclusion
This project demonstrates the ability to analyze large-scale datasets efficiently while maintaining readable and optimized SQL.