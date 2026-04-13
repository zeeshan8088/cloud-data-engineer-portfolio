import sys
from google.cloud import bigquery

def estimate_query_cost(sql_query: str):
    """
    Dry runs the provided SQL query and prints the estimated MB processed 
    and approximate cost in USD (assuming standard $6.25 per TB tier).
    """
    client = bigquery.Client()
    
    # Configure the job to be a dry run
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    
    try:
        query_job = client.query(sql_query, job_config=job_config)
    except Exception as e:
        print(f"Error during dry run: {e}")
        return

    # Total bytes processed is extracted from the job
    bytes_processed = query_job.total_bytes_processed
    
    if bytes_processed is None:
        print("Could not estimate bytes processed.")
        return

    # Convert to MB
    mb_processed = bytes_processed / (1024 ** 2)
    
    # Calculate Cost: $6.25 per TB (TiB) for On-demand Pricing
    cost_per_tb = 6.25
    tb_processed = bytes_processed / (1024 ** 4)
    estimated_cost_usd = tb_processed * cost_per_tb

    print(f"Estimated Data Processed: {mb_processed:.2f} MB")
    print(f"Estimated Cost: ${estimated_cost_usd:.6f} USD")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python estimate_query_cost.py \"<SQL_QUERY_STRING>\"")
        sys.exit(1)
        
    sql_query = sys.argv[1]
    estimate_query_cost(sql_query)
