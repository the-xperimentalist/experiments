
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value, get_mapper_file, split_json_list
from ..config import DEMO_DB_CONFIG


def upload_pla_consolidated_daily_report(pla_consolidated_df, client_name):
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

    pla_consolidated_df["campaign_id"] = pla_consolidated_df["Campaign ID"]
    pla_consolidated_df["campaign_name"] = pla_consolidated_df["Campaign Name"]
    pla_consolidated_df["date"] = pla_consolidated_df["Date"]
    pla_consolidated_df["ad_spend"] = pla_consolidated_df["Ad Spend"]
    pla_consolidated_df["views"] = pla_consolidated_df["Views"]
    pla_consolidated_df["clicks"] = pla_consolidated_df["Clicks"]
    pla_consolidated_df["units_sold"] = pla_consolidated_df["Total converted units"]
    pla_consolidated_df["product_sales"] = pla_consolidated_df["Total Revenue (Rs.)"]
    pla_consolidated_df["roi"] = pla_consolidated_df["ROI"]

    pla_consolidated_df = pla_consolidated_df[["campaign_id", "campaign_name", "date", "ad_spend", "views", "clicks", "units_sold", "product_sales", "roi"]]

    values_list = []
    all_dates = pla_consolidated_df.date.unique().tolist()

    id_val = last_value
    for date in all_dates:
        id_val += 1
        date_val = date
        client_id = 2
        dashboard_type = "FK_REPORTING"
        constant_val = {
            "client_name": client_name,
            "data_type": "pla_consolidated"
        }
        values = pla_consolidated_df[pla_consolidated_df["date"] == date].to_json()
        values_list.append(
            (
                id_val,
                date_val,
                client_id,
                dashboard_type,
                json.dumps(constant_val),
                json.dumps(values))
            )

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

