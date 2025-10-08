import pandas as pd
from sqlalchemy import create_engine, text
import argparse
import logging
import os
from datetime import datetime
from google.cloud import bigquery
client = bigquery.Client(project="lithe-transport-431116-b4")

dataset = "test"
target_tbl = "u_literature"
staging_tbl = "u_literature_staging"

## Load local parquet files into db
df = pd.read_parquet("info_blogs.parquet")
engine = create_engine("postgresql+psycopg2://postgres:root@localhost:5432/assessment")
df.to_sql("u_literature", engine, if_exists="append", index=False)

## Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

pgres_conn = "postgresql+psycopg2://postgres:root@localhost:5432/assessment"
watermark = "watermark.txt"

def get_watermark(override=None):
    if override:
        logging.info(f"Watermark override --since={override}")
        return datetime.fromisoformat(override)

    if os.path.exists(watermark):
        with open(watermark, "r") as f:
            timeStamp = f.read().strip()
            if timeStamp:
                logging.info(f"Watermark Found: {timeStamp}")
                return datetime.fromisoformat(timeStamp)
    
    logging.info("Zero watermarks found")
    logging.info("Loading *")
    return None

def save_watermark(timeStamp):
    with open(watermark, "w") as f:
        #f.write(timeStamp.format())
        f.write(timeStamp.isoformat())
    
    logging.info(f"Updating watermark: {timeStamp}")

def main(override=None):
    #engine = create_engine(pgress_conn)
    engine = create_engine(pgres_conn)
    prev_watermark = get_watermark(override)
    
    print(f"{prev_watermark}")
    with engine.connect() as conn:
        if prev_watermark:
            query = text("""SELECT * FROM public.u_literature 
            WHERE date_loaded > :prev_watermark ORDER BY date_loaded""")

            #df = pd.read_sql(query, conn_params={"last_watermark": prev_watermark})
            df = pd.read_sql(query, conn, params={"prev_watermark": prev_watermark})
        
        else:
            query = text("""SELECT * FROM public.u_literature 
            ORDER BY date_loaded""")
            df = pd.read_sql(query,conn)

    if df.empty:
        logging.info("No rows to be loaded. Exiting")
        return
    
    ## Upsert rows to postgres u_literature_prod table (Incremental Load -> Postgres)
    ## Workaround (GCP account is in Free Mode)
    with engine.begin() as conn:
        for _, row in df.iterrows():
            upsert = text("""
            INSERT INTO public.u_literature_prod(
            user_id, title, content_text, photo_url, description,
            id, content_html,category, updated_at, created_at,
            random_users_count, created_date, date_accessed, date_loaded
            ) VALUES (
            :user_id, :title, :content_text, :photo_url, :description,
            :id, :content_html, :category, :updated_at, :created_at,
            :random_users_count, :created_date, :date_accessed, :date_loaded
            )
            ON CONFLICT (user_id) DO UPDATE SET
                          title = EXCLUDED.title,
                          content_text = EXCLUDED.content_text,
                          photo_url = EXCLUDED.photo_url,
                          description = EXCLUDED.description,
                          id = EXCLUDED.id,
                          content_html = EXCLUDED.content_html,
                          category = EXCLUDED.category,
                          updated_at = EXCLUDED.updated_at,
                          created_at = EXCLUDED.created_at,
                          random_users_count = EXCLUDED.random_users_count,
                          created_date = EXCLUDED.created_date,
                          date_accessed = EXCLUDED.date_accessed,
                          date_loaded = EXCLUDED.date_loaded
            
            """)
            conn.execute(upsert, row.to_dict())

    logging.info(f"Upsert complete in postgres prod staging table public.u_literature_prod")
    logging.info(f"{len(df)} new or updated rows")

    logging.info("New data loaded successfully to public.u_literature_prod")

    new_watermark = df["date_loaded"].max()

    if pd.notnull(new_watermark):
        save_watermark(pd.to_datetime(new_watermark))

    logging.info("Loading data to BigQuery u_literature")
    ## Truncate load to google big query as current account is on free mode
    ## Get data from postgres public.u_literature_prod
    query = "SELECT * FROM public.u_literature_prod ORDER BY date_loaded"
    df = pd.read_sql(query, engine)

    table_id = f"{client.project}.{dataset}.{target_tbl}"
    conf = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    load = client.load_table_from_dataframe(df, table_id, job_config=conf)
    load.result()
    logging.info("Data loaded to BigQuery u_literature")

    ## Check if data is properly loaded
    query_test = "SELECT * FROM `lithe-transport-431116-b4.test.u_literature` LIMIT 5"
    qf = client.query(query_test).to_dataframe()
    print(df)

if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument("--since", help="Override last watermark (ex. 2025-10-03T00:00:00)", required=False)
    args = parse.parse_args()

    try:
        main(args.since)
    except Exception as e:
        logging.exception(f"Execution failed {e}")
