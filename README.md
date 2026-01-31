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
     


   

   c. R, F and M values are then used to create dual level of segmentation. One is based on sum of these three factors while another assigns them characteristics based on individual values. This is done to increase scope of insights obtained from current customer base.

   d. Lastly it upserts the values inside the customer_rfm_daily database. There is no manual intervention required for BAU use.
   
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

   
  
