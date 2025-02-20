
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value
from ..config import DEMO_DB_CONFIG


def upload_asin_cat_map(asin_mapper, client_name):
    """
    :asin_mapper: pd dataframe
    :client_name: str

    # client_id is 2
    # id
    # date
    # dashboard type
    # constant
    # values
    # created_at
    # updated_at
    """

    last_value = get_last_value()
    last_value = last_value if last_value != None else 0
    asin_mapper = asin_mapper[asin_mapper["Category"].notnull()]

    asin_cat_map = {}
    for i in range(asin_mapper.shape[0]):
        v = asin_mapper.iloc[i]
        asin_cat_map[asin_mapper.iloc[i]["ASIN"]] = {
            "category": asin_mapper.iloc[i]["Category"] if asin_mapper.iloc[i]["Category"] else "",
            "brand": asin_mapper.iloc[i]["Brand"] if asin_mapper.iloc[i]["Brand"] else "",
            "name": asin_mapper.iloc[i]["Item Name"] if asin_mapper.iloc[i]["Item Name"] else "",
            "code": asin_mapper.iloc[i]["Item Code"] if asin_mapper.iloc[i]["Item Code"] else "",
            "bau": float(asin_mapper.iloc[i]["Final BAU"]) if asin_mapper.iloc[i]["Final BAU"] else "",
            "mrp": float(asin_mapper.iloc[i]["MRP"]) if asin_mapper.iloc[i]["MRP"] else ""
        }

    client_id = 2
    id_val = last_value + 1
    dashboard_type = "AZ_REPORTING"
    constant_val = {
      "client_name": client_name,
      "data_type": "asin_mapper"
    }

    values = asin_cat_map
    insert_query_val = f"""
    ({id_val}, CURRENT_TIMESTAMP, {client_id}, '{dashboard_type}', '{json.dumps(constant_val)}'::jsonb, '{json.dumps(values)}'::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """

    demo_conn = psycopg2.connect(**DEMO_DB_CONFIG)
    demo_cur = demo_conn.cursor()
    insert_query = f"""
    INSERT INTO public.api_marketplaceclientsinternaldata (
        id,
        date,
        client_id,
        dashboard_type,
        constant,
        values,
        created_at,
        updated_at
    )
    VALUES {insert_query_val};
    """

    demo_cur.execute(insert_query)
    demo_conn.commit()

    demo_cur.close()
    demo_conn.close()
    return {"Message": "Upload successful"}
