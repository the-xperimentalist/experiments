
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value, get_mapper_file, split_json_list
from ..config import DEMO_DB_CONFIG


def upload_pla_campaign_report(pla_campaign_df, client_name):
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

    pla_campaign_df["campaign_id"] = pla_campaign_df["Campaign ID"]
    pla_campaign_df["ad_group_name"] = pla_campaign_df["AdGroup Name"]
    pla_campaign_df["listing_id"] = pla_campaign_df["Listing ID"]
    pla_campaign_df["fsn"] = pla_campaign_df["FSN ID"]
    pla_campaign_df["date"] = pla_campaign_df["Date"]
    pla_campaign_df["revenue"] = pla_campaign_df["Total Revenue (Rs.)"]
    pla_campaign_df["direct_units_sold"] = pla_campaign_df["Direct Units Sold"]
    pla_campaign_df["indirect_units_sold"] = pla_campaign_df["Indirect Units Sold"]
    pla_campaign_df["cpc"] = pla_campaign_df["AdGroup CPC"]
    pla_campaign_df["expected_cpc"] = pla_campaign_df["Expected ROI"]

    pla_campaign_df = pla_campaign_df[["campaign_id", "ad_group_name", "listing_id", "fsn", "date", "revenue", "direct_units_sold", "indirect_units_sold", "cpc", "expected_cpc"]]

    values_list = []
    all_dates = pla_campaign_df.date.unique().tolist()

    id_val = last_value
    for date in all_dates:
        id_val += 1
        date_val = date
        client_id = 2
        dashboard_type = "FK_REPORTING"
        constant_val = {
            "client_name": client_name,
            "data_type": "pla_campaign"
        }
        values = pla_campaign_df[pla_campaign_df["date"] == date].to_json()
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
