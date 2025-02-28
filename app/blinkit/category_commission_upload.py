
import json
import pandas as pd
import psycopg2


def upload_category_commission(client_name, commission_df):

    last_value = get_last_value()
    last_value = last_value if last_value != None else 0

    data = check_if_exists(client_name, file_type, date)
    data = check_if_exists(
      data_type="Category_commission", "Blinkit_Reporting")

