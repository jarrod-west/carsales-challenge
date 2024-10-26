from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, round, min, col

from .types import InvoiceDataFrames


def _invoice_totals(data_frames: InvoiceDataFrames) -> None:
  line_item_costs = data_frames["invoice_lines"].join(data_frames["skus"], "item_id")
  line_item_costs = line_item_costs.withColumn(
    "invoice_sub_total", line_item_costs.quantity * line_item_costs.item_retail_price
  )
  return line_item_costs.groupBy("invoice_id").agg(
    round(sum("invoice_sub_total"), 2).alias("invoice_total")
  )


def _detailed_invoices(data_frames: InvoiceDataFrames) -> DataFrame:
  invoice_totals = _invoice_totals(data_frames)
  return (
    invoice_totals.join(data_frames["invoices"], "invoice_id")
    .join(data_frames["accounts"], "account_id")
    .orderBy("date_issued")
  )


def upcoming_invoices(data_frames: InvoiceDataFrames) -> DataFrame:
  detailed_invoices = _detailed_invoices(data_frames)
  return detailed_invoices.select(
    "invoice_id",
    "invoice_total",
    "date_issued",
    "company_name",
    "contact_person",
    "contact_phone",
  )


def account_invoice_totals(data_frames: InvoiceDataFrames) -> DataFrame:
  detailed_invoices = _detailed_invoices(data_frames)
  account_totals = detailed_invoices.groupBy("account_id").agg(
    round(sum("invoice_total"), 2).alias("account_total")
  )
  account_earliest_issued = detailed_invoices.groupBy("account_id").agg(
    min("date_issued").alias("oldest_issue_date")
  )
  account_details = account_totals.join(account_earliest_issued, "account_id").join(
    data_frames["accounts"], "account_id"
  )
  return account_details.sort(col("account_total").desc()).select(
    "account_total",
    "oldest_issue_date",
    "company_name",
    "contact_person",
    "contact_phone",
  )