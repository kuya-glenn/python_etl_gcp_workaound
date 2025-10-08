# python_etl_gcp_workaound
Python ETL w/ Postgres, Watermarking, Google BigQuery

Upsert into a staging table into prod using id as the primary key and updated_at precedence; Includes a computation for a 7‑day rolling DAU with partition pruning.

Language: Python

Libraries: Pandas, Sqlalchemy, Google Cloud

Included Structured Logging.

Process:
<img width="498" height="1284" alt="image" src="https://github.com/user-attachments/assets/f78d70e2-0b59-4b16-870b-d5f380dfef3d" />
<img width="380" height="1226" alt="image" src="https://github.com/user-attachments/assets/06d98a0b-7733-4ddf-80f9-0c19783fcde4" />

Staging Table (Postgres):
<img width="975" height="512" alt="image" src="https://github.com/user-attachments/assets/277cdd64-dcdd-48e9-8966-c3c63620f091" />

Production Table (Postgres):
<img width="975" height="602" alt="image" src="https://github.com/user-attachments/assets/4f312cd1-a173-4a07-a728-e3917b2f3858" />

Workaround since GCP Account is in Free mode:
Postgres Staging > Upsert to Postgres Prod Table > Truncate and Load to GCP

Google BigQuery Table:
<img width="975" height="373" alt="image" src="https://github.com/user-attachments/assets/fff40a6e-d475-4e16-aca9-6dd2b5d3a06d" />




