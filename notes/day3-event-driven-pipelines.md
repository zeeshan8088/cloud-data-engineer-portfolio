# Day 3 – Event-Driven Data Pipelines

What I understood today:
- Data pipelines should not be manual
- Events like file uploads trigger pipelines automatically
- Cloud Functions handle small event-driven tasks
- Cloud Run is used when more control or longer processing is needed

End-to-end flow:
- CSV file arrives in Cloud Storage
- Event triggers a Cloud Function
- Data is loaded into BigQuery raw tables
- SQL queries run on ingested data

Why this matters:
Event-driven pipelines scale better and remove manual intervention.
