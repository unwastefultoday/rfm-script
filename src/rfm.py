# src/rfm.py

import pandas as pd
from datetime import date
from psycopg2.extras import execute_values
from database import get_connection


def get_rfm_query(run_date):

    return f"""
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
        SELECT *,
            NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
            NTILE(5) OVER (ORDER BY frequency_orders) AS f_score,
            NTILE(5) OVER (ORDER BY monetary_value) AS m_score
        FROM rfm
    ),

    segmented AS (
        SELECT *,
            (r_score + f_score + m_score) AS rfm_score,
            CASE
                WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
                WHEN f_score >= 4 AND m_score >= 4 THEN 'Loyal High Spenders'
                WHEN f_score >= 4 AND m_score < 4 THEN 'Loyal Budget Spenders'
                WHEN r_score <= 2 AND f_score > 3 THEN 'Loyal At Risk'
                WHEN r_score <= 2 THEN 'At Risk'
                ELSE 'Others'
            END AS rfm_segment
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
        rfm_segment
    FROM segmented;
    """


def run_rfm_pipeline():

    run_date = date.today()
    conn = get_connection()

    df = pd.read_sql(get_rfm_query(run_date), conn)

    insert_query = """
    INSERT INTO ecom.customer_rfm_daily (
        run_date, customer_id, recency_days, frequency_orders,
        monetary_value, r_score, f_score, m_score, rfm_score, rfm_segment
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
        updated_at = CURRENT_TIMESTAMP;
    """

    records = df.to_records(index=False).tolist()
    cursor = conn.cursor()
    execute_values(cursor, insert_query, records)
    conn.commit()

    cursor.close()
    conn.close()

    print(f"Inserted {len(df)} RFM records for {run_date}")
