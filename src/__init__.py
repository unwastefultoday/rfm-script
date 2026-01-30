# src/__init__.py

from rfm import run_rfm_pipeline
import logging

logging.basicConfig(
    filename="rfm_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    try:
        count = run_rfm_pipeline()
        logging.info(f"RFM pipeline ran successfully. Records processed: {count}")
        print("RFM pipeline completed successfully")

    except Exception as e:
        logging.error("RFM pipeline failed", exc_info=True)
        print("RFM pipeline failed. Check logs.")


if __name__ == "__main__":
    main()
