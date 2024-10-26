import logging
from os.path import join
from pyspark.sql import SparkSession, DataFrame

from challenge.invoices import (
  outstanding_invoices,
  outstanding_account_totals,
  InvoiceDataFrames,
)

CSV_PATH = "./docs/task_one/data_extracts"
LOG_LEVEL = "INFO"

logger = logging.getLogger()

spark = SparkSession.builder.getOrCreate()


def read_csv(name: str) -> DataFrame:
  """Read one of the sample CSV files into a data frame.

  Args:
      name (str): The name of the CSV (not including extension)

  Returns:
      pyspark.sql.DataFrame: The CSV data as a data frame
  """
  csv_path = join(CSV_PATH, f"{name}.csv")
  logger.debug(f"Loading: ${csv_path}")
  return spark.read.options(multiline=True).csv(
    csv_path, header=True, inferSchema=True, mode="FAILFAST"
  )


def main() -> None:
  """Run an example using the provided CSV lines, showing the results."""
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
    outstanding_invoices(data_frames).show()

    logger.info("Account invoice totals")
    outstanding_account_totals(data_frames).show()

  except Exception as ex:
    logger.error(f"Error: ${ex}")

  logger.info("Execution Complete")


if __name__ == "__main__":
  main()
