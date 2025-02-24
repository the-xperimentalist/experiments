
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value, get_mapper_file, split_json_list
from ..config import DEMO_DB_CONFIG


def upload_fk_orders(calculated_df, client_name):
    """
    :sd: pandas df
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

    calculated_df["order_item_id"] = calculated_df["order_item_id"].apply(lambda x: x.replace("OI:", ""))
    calculated_df["order_id"] = calculated_df["order_id"].apply(lambda x: x.replace("OD", ""))
    calculated_df["sku_id"] = calculated_df["sku"].apply(lambda x: x.replace("\"\"\"", "").replace("SKU:", ""))
    calculated_df["return_id"] = calculated_df["return_id"].apply(lambda x: x.replace("R:", "") if not isinstance(x, float) else x)
    calculated_df["order_date"] = calculated_df["order_date"].apply(lambda x: x[0:10])
    calculated_df = calculated_df[["order_item_id", "order_id", "sku_id", "fulfilment_source", "fulfilment_type", "order_date", "order_item_status", "fsn", "quantity", "return_id", "return_reason", "procurement_dispatch_sla", "dispatch_after_date", "dispatch_by_date", "delivery_sla", "deliver_by_date", "order_delivery_date", "delivery_sla_breached"]]

    values_list = []
    all_dates = calculated_df.order_date.unique().tolist()

    id_val = last_value
    for date in all_dates:
        id_val += 1
        date_val = date
        client_id = 2
        dashboard_type = "FK_REPORTING"
        constant_val = {
            "client_name": client_name,
            "data_type": "orders"
        }
        values = calculated_df[calculated_df["order_date"] == date].to_json()
        values_list.append(
            (
                id_val,
                date_val,
                client_id,
                dashboard_type,
                json.dumps(constant_val),
                json.dumps(values)))

    demo_conn = psycopg2.connect(**DEMO_DB_CONFIG)
    demo_cur = demo_conn.cursor()

    insert_query_val = f"""
    ({id_val}, '{date_val}'::timestamp, {client_id}, '{dashboard_type}', '{json.dumps(constant_val)}'::jsonb, '{json.dumps(values)}'::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """
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
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        );
    """

    demo_cur.executemany(insert_query, values_list)
    demo_conn.commit()

    demo_cur.close()
    demo_conn.close()

    return {"Message": "Successfully uploaded"}
