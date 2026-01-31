This project computes daily RFM (Recency, Frequency, Monetary) metrics for customers from the ecom.orders table and stores the results in ecom.customer_rfm_daily.
The pipeline is designed to run once per day using a scheduled job (cron / Task Scheduler).

**Logic Behind the script**

   a. The script connects to postresql database using the credentials mentioned in the .env file.
                                 
    import os
    
    import psycopg2
    
    from dotenv import load_dotenv

    load_dotenv()

    def get_connection():
        return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT"))

   b. It then runs an sql script to calculate R (how recent was the last order date as compared to running date), F (Volume of Orders made by customer) and M (Value of Orders made by the customer.) factors for each customer. All three are integers ranging from 1 to 5. 
     
      import pandas as pd
      from datetime import date
      from psycopg2.extras import execute_values
      from database import get_connection


      def get_rfm_query(run_date):

          return f""" WITH base AS (
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
   
   c. R, F and M values are then used to create dual level of segmentation. One is based on sum of these three factors while another assigns them characteristics based on individual values. This is done to increase scope of insights obtained from current customer base. Lastly it upserts the values inside the customer_rfm_daily database. There is no manual intervention required for BAU use.

      ere
   
**Steps for Use**

   **0. Create a virtual environment inside your root folder.**

   **1. Environment Variables Setup** -Create a .env file in the project root directory:

      DB_HOST=localhost

      DB_NAME=postgres

      DB_USER=your_username

      DB_PASSWORD=your_password

      DB_PORT=5432


   **2. Install required packages:**

      pip install psycopg2 pandas python-dotenv matplotlib seaborn

   **3. Run the Project** - Inside project directory, run "python rfm_pipeline.py" after virtual environment activation.

   **3. To schedule this task-**

      Windows - Open Task Scheduler and click "Create Basic Task". Name the task, Choose trigger as Daily and set the desired time and choose action as "Start a program"
       
       Set Program/script to the Python executable inside the virtual environment: C:\path\to\project\venv\Scripts\python.exe
       
       Set Add arguments RFM.py
   
       Set Start in (working directory) to: C:\path\to\project
   

     Mac -Edit Crontab (crontab -e) , Add a daily job (this runs at 2AM): 0 2 * * * /path/to/project/venv/bin/python /path/to/project/rfm_pipeline.py >> /path/to/project/rfm_pipeline.log 2>&1


   **4. Notes** - Ensure the .env file exists in the project root so credentials load correctly and that the database user has INSERT and UPDATE permissions on ecom.customer_rfm_daily. Lastly, test the script manually before scheduling.

**That's All!**

   
  
