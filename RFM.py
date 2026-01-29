import os
import logging
from datetime import date
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ------------------ LOGGING SETUP ------------------
logging.basicConfig(
    filename="rfm_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting RFM pipeline")

# ------------------ LOAD ENV ------------------
try:
    load_dotenv()
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_PORT = os.getenv("DB_PORT")

    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT]):
        raise ValueError("One or more environment variables are missing")

    logging.info("Environment variables loaded successfully")

except Exception as e:
    logging.error(f"Failed loading environment variables: {e}")
    raise

# ------------------ DB CONNECTION ------------------
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    cursor = conn.cursor()
    logging.info("Database connection established")

except Exception as e:
    logging.error(f"Database connection failed: {e}")
    raise

run_date = date.today()

# ------------------ MAIN LOGIC ------------------
try:
    rfm_query = f"""
    WITH base AS (
        SELECT
            customer_id,
            MAX(created_at) AS last_order_date,
            COUNT(order_id) AS frequency_orders,
            SUM(total) AS monetary_value
        FROM ecom.orders
        WHERE status != 'cancelled'
        GROUP BY customer_id
    ),

    rfm AS (
        SELECT
            customer_id,
            DATE '{run_date}' AS run_date,
            DATE_PART('day', DATE '{run_date}' - last_order_date::date) AS recency_days,
            frequency_orders,
            monetary_value
        FROM base
    ),

    scored AS (
        SELECT
            *,
            NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
            NTILE(5) OVER (ORDER BY frequency_orders) AS f_score,
            NTILE(5) OVER (ORDER BY monetary_value) AS m_score
        FROM rfm
    ),

    segmented AS (
        SELECT
            *,
            (r_score + f_score + m_score) AS rfm_score,
            CASE
                WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
                WHEN f_score >= 4 AND m_score >= 4 AND r_score < 4 THEN 'Loyal High Spenders'
                WHEN f_score >= 4 AND m_score <= 3 THEN 'Loyal Budget Spenders'
                WHEN m_score >= 4 AND f_score <= 3 AND r_score >= 3 THEN 'Big Spenders'
                WHEN r_score = 5 AND f_score <= 2 AND m_score >= 4 THEN 'New High Value Customers'
                WHEN r_score = 5 AND f_score <= 2 AND m_score <= 2 THEN 'New Customers'
                WHEN r_score >= 4 AND f_score = 3 AND m_score = 3 THEN 'Potential Loyalists'
                WHEN r_score = 4 AND f_score = 2 AND m_score = 2 THEN 'Promising Customers'
                WHEN r_score <= 2 AND f_score >= 4 THEN 'Loyal At Risk'
                WHEN r_score <= 2 AND m_score >= 4 THEN 'Big Spenders At Risk'
                WHEN r_score = 2 AND f_score <= 2 AND m_score <= 2 THEN 'About to Churn'
                WHEN r_score = 1 AND f_score <= 2 AND m_score <= 2 THEN 'Hibernating'
                WHEN f_score = 1 AND m_score <= 2 THEN 'One-time Buyers'
                WHEN f_score >= 3 AND m_score = 1 THEN 'Bargain Hunters'
                WHEN r_score <= 3 AND f_score <= 2 AND m_score <= 2 THEN 'Low Value Customers'
                ELSE 'Others'
            END AS comments
        FROM scored
    )

    SELECT
        run_date,
        customer_id,
        recency_days,
        frequency_orders,
        monetary_value,
        r_score,
        f_score,
        m_score,
        rfm_score,
        CASE
            WHEN rfm_score >=10 THEN 'Loyal Customers'
            WHEN rfm_score BETWEEN 8 AND 9 THEN 'Potential Loyalists'
            WHEN rfm_score BETWEEN 6 AND 7 THEN 'Promising Customers'
            WHEN rfm_score BETWEEN 4 AND 5 THEN 'At Risk'
            WHEN rfm_score = 3 THEN 'Hibernating'
            ELSE 'Others'
        END AS rfm_segment,
        comments
    FROM segmented;
    """

    df = pd.read_sql(rfm_query, conn)

    if df.empty:
        raise ValueError("RFM query returned no data")

    df["created_at"] = pd.Timestamp.now()
    df["updated_at"] = pd.Timestamp.now()
    df["logs"] = '{"source":"daily_rfm_pipeline"}'

    logging.info(f"Fetched {len(df)} RFM records")

    insert_query = """
    INSERT INTO ecom.customer_rfm_daily (
        run_date, customer_id, recency_days, frequency_orders, monetary_value,
        r_score, f_score, m_score, rfm_score, rfm_segment, comments,
        created_at, updated_at, logs
    )
    VALUES %s
    ON CONFLICT (run_date, customer_id)
    DO UPDATE SET
        recency_days = EXCLUDED.recency_days,
        frequency_orders = EXCLUDED.frequency_orders,
        monetary_value = EXCLUDED.monetary_value,
        r_score = EXCLUDED.r_score,
        f_score = EXCLUDED.f_score,
        m_score = EXCLUDED.m_score,
        rfm_score = EXCLUDED.rfm_score,
        rfm_segment = EXCLUDED.rfm_segment,
        comments = EXCLUDED.comments,
        updated_at = CURRENT_TIMESTAMP,
        logs = EXCLUDED.logs;
    """

    records = df.to_records(index=False).tolist()
    execute_values(cursor, insert_query, records)
    conn.commit()

    logging.info(f"Inserted/updated {len(df)} records for {run_date}")
    print(f"Inserted/updated {len(df)} RFM records for {run_date}")

except Exception as e:
    conn.rollback()
    logging.error(f"Pipeline failed: {e}", exc_info=True)
    print("Pipeline failed. Check logs.")

finally:
    cursor.close()
    conn.close()
    logging.info("Database connection closed")
