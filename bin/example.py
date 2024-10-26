from argparse import ArgumentParser
from datetime import datetime, timezone
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
OUT_DIR = "out"

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


def write_csv(name: str, timestamp: datetime.date, data: DataFrame) -> None:
  file_path = join(OUT_DIR, f"{name}_{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}.csv")
  data.coalesce(1).write.csv(file_path, header=True)


def main(write_to_file: bool) -> None:
  """Run an example using the provided CSV lines, showing the results."""
  logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
  )
  logger.debug("Starting execution...")
  try:
    data_frames: InvoiceDataFrames = {
      "accounts": read_csv("accounts"),
      "invoices": read_csv("invoices"),
      "invoice_lines": read_csv("invoice_line_items"),
      "skus": read_csv("skus"),
    }

    outstanding_inv = outstanding_invoices(data_frames)
    outstanding_acc = outstanding_account_totals(data_frames)

    if write_to_file:
      timestamp = datetime.now(timezone.utc)
      logger.info(f"Writing to files at '{OUT_DIR}'")
      # outstanding_inv.coalesce(1).write.format('com.databricks.spark.csv').save(join(OUT_DIR, f"outstanding_invoices_{file_suffix}.csv"))
      # outstanding_acc.coalesce(1).write.format('com.databricks.spark.csv').save(join(OUT_DIR, f"outstanding_account_totals_{file_suffix}.csv"))

      # outstanding_inv.toPandas().to_csv(join(OUT_DIR, f"outstanding_invoices_{file_suffix}.csv"), index=False)
      write_csv("OutstandingInvoices", timestamp, outstanding_inv)
      write_csv("OutstandingAccountTotals", timestamp, outstanding_acc)
    else:
      logger.info("Upcoming invoices")
      outstanding_inv.show()

      logger.info("Account invoice totals")
      outstanding_acc.show()

  except Exception as ex:
    logger.error(f"Error: ${ex}")

  logger.debug("Execution Complete")


if __name__ == "__main__":
  parser = ArgumentParser(
    prog="Carsales tech challenge sample execution",
    description="Runs an example using the provided CSV files and outputs to console or file",
  )

  parser.add_argument("-w", "--write", action="store_true")
  args = parser.parse_args()

  main(args.write)
