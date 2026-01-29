This project computes daily RFM (Recency, Frequency, Monetary) metrics for customers from the ecom.orders table and stores the results in ecom.customer_rfm_daily.
The pipeline is designed to run once per day using a scheduled job (cron / Task Scheduler).

**0. Create a virtual environment inside your root folder.**

**1. Environment Variables Setup**

Create a .env file in the project root directory:
DB_HOST=localhost
DB_NAME=postgres
DB_USER=your_username
DB_PASSWORD=your_password
DB_PORT=5432


Install required packages:
pip install psycopg2 pandas python-dotenv matplotlib seaborn

**2. Run the Project**
   Inside project directory, run "python rfm_pipeline.py" after virtual environment activation.

**3. To schedule this task-**

   **Windows**
   Open Task Scheduler and click "Create Basic Task"
   Name the task
   Choose trigger as Daily and set the desired time
   Choose action as "Start a program"
   Set Program/script to the Python executable inside the virtual environment: C:\path\to\project\venv\Scripts\python.exe
   Set Add arguments RFM.py
   Set Start in (working directory) to: C:\path\to\project

   **Mac**
   Edit Crontab (crontab -e)
   Add a daily job (this runs at 2AM): 0 2 * * * /path/to/project/venv/bin/python /path/to/project/rfm_pipeline.py >> /path/to/project/rfm_pipeline.log 2>&1


**Notes**
Ensure the .env file exists in the project root so credentials load correctly
Ensure the database user has INSERT and UPDATE permissions on ecom.customer_rfm_daily
Test the script manually before scheduling
Logging is recommended for monitoring failures and reruns
   
  
