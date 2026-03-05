import json
import random

data = [random.randint(1,200) for _ in range(10)]

with open("/opt/airflow/projects/day4_pipeline/output.json","w") as f:
    json.dump(data,f)

print("Pipeline executed successfully")