
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value, get_mapper_file
from ..config import DEMO_DB_CONFIG


def upload_sales_summary_data(ss_df, client_name):

    last_value = get_last_value()
    last_value = last_value if last_value != None else 0
    pass
