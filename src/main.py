from pyspark.sql import SparkSession, DataFrame
from os.path import join
import logging

CSV_PATH = "./task_one/data_extracts"
LOG_LEVEL = logging.INFO

logger = logging.getLogger("main")
spark = SparkSession.builder.getOrCreate()

def read_csv(filename: str) -> DataFrame:
  csv_path = join(CSV_PATH, f"{filename}.csv")
  logger.debug(f"Loading: ${csv_path}")
  return spark.read.csv(csv_path, header=True, inferSchema=True, mode='FAILFAST')

def main() -> None:
  logging.basicConfig(level=LOG_LEVEL)
  logger.info("Starting execution...")
  try:
    accounts = read_csv("accounts")
    invoices = read_csv("invoices")
    invoice_line_items = read_csv("invoice_line_items")
    skus = read_csv("skus")

    logger.debug(f"Accounts: ${accounts}")
    logger.debug(f"Invoices: ${invoices}")
    logger.debug(f"Invoice Line Items: ${invoice_line_items}")
    logger.debug(f"SKUs: ${skus}")

  except Exception as ex:
    logger.error(f"Error: ${ex}")
  
  logger.info("Execution Complete")

if __name__ == '__main__':
  main()
