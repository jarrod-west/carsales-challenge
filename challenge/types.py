from pyspark.sql import DataFrame
from typing import TypedDict


class InvoiceDataFrames(TypedDict):
  invoices: DataFrame
  invoice_lines: DataFrame
  skus: DataFrame
  accounts: DataFrame
