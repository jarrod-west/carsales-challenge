from pyspark.sql import SparkSession, DataFrame
from os.path import join

import logging

from challenge.invoices import (
  upcoming_invoices,
  account_invoice_totals,
  InvoiceDataFrames,
)

CSV_PATH = "./docs/task_one/data_extracts"
LOG_LEVEL = "INFO"

logger = logging.getLogger()

spark = SparkSession.builder.getOrCreate()


def read_csv(filename: str) -> DataFrame:
  csv_path = join(CSV_PATH, f"{filename}.csv")
  logger.debug(f"Loading: ${csv_path}")
  return spark.read.options(multiline=True).csv(
    csv_path, header=True, inferSchema=True, mode="FAILFAST"
  )


def main() -> None:
  logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
  )
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

    data_frames: InvoiceDataFrames = {
      "accounts": accounts,
      "invoices": invoices,
      "invoice_lines": invoice_line_items,
      "skus": skus,
    }

    logger.info("Upcoming invoices")
    upcoming_invoices(data_frames).show()

    logger.info("Account invoice totals")
    account_invoice_totals(data_frames).show()

  except Exception as ex:
    logger.error(f"Error: ${ex}")

  logger.info("Execution Complete")


if __name__ == "__main__":
  main()
