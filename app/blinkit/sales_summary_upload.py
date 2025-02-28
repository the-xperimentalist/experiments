
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value, get_mapper_file
from ..config import DEMO_DB_CONFIG


def upload_sales_summary_data(ss_df, client_name):

    last_value = get_last_value()
    last_value = last_value if last_value != None else 0

    ss_df["invoice_id"] = ss_df["Invoice ID"]
    ss_df["order_id"] = ss_df["Order ID"]
    ss_df["item_id"] = ss_df["Item ID"]

    ss_df["order_date"] = pd.to_datetime(ss_df["Order Date"])

    ss_df["order_city"] = ss_df["Customer City "]
    ss_df["order_qty"] = ss_df["Quantity"]
    ss_df["gross_bill_amount"] = ss_df["Total Gross Bill Amount"]

    ss_df = ss_df[["invoice_id", "order_id", "item_id", "order_date", "order_city", "order_qty", "gross_bill_amount"]]
    pass
