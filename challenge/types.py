from pyspark.sql import DataFrame
from typing import TypedDict


class InvoiceDataFrames(TypedDict):
  """Container type for the relevant data frames."""

  invoices: DataFrame
  invoice_lines: DataFrame
  skus: DataFrame
  accounts: DataFrame
