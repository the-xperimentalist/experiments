
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value, get_mapper_file
from ..config import DEMO_DB_CONFIG


def upload_sb_data(sb, client_name):
    """
    :sb: pandas df
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

    campaign_cat_map = get_mapper_file(client_name, "campaign_mapper", "AZ_REPORTING")

    sb["date"] = pd.to_datetime(sb["Date"])
    del sb["Date"]

    sb["campaign_name"] = sb["Campaign Name"]
    del sb["Campaign Name"]
    sb = sb[sb["campaign_name"].notna()]

    sb["cost_type"] = sb["Cost Type"]
    del sb["Cost Type"]

    sb["clicks"] = sb["Clicks"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sb["Clicks"]

    sb["ad_spend"] = sb["Spend"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sb["Spend"]

    sb["impressions"] = sb["Impressions"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sb["Impressions"]

    sb["units_ordered"] = sb["14 Day Total Orders (#)"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sb["14 Day Total Orders (#)"]

    sb["product_sales"] = sb["14 Day Total Sales (₹)"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sb["14 Day Total Sales (₹)"]

    sb["category"] = sb["campaign_name"].apply(lambda x: campaign_cat_map.get(x, ""))
    sb["brand"] = sb["campaign_name"].apply(lambda x: campaign_cat_map.get(x, ""))

    sb = sb[["campaign_name", "date", "cost_type", "clicks", "ad_spend", "impressions", "units_ordered", "product_sales", "category", "brand"]]

    # id - last value
    # date - looped through
    # dashboard type - az reporting
    # constant - {'client_name': 'himanshu', 'type': 'sponsored_brands'}
    # values - // as caught from the above loop
    # created_at - now
    # updated_at - now

    values_list = []
    all_dates = sb.date.unique().tolist()

    id_val = last_value

    for date in all_dates:
        id_val = id_val + 1
        date_val = date
        client_id = 2
        dashboard_type = "AZ_REPORTING"
        constant_val = {
            "client_name": client_name,
            "data_type": "sponsored_brands"
        }
        values = sb[sb["date"] == date].to_json()
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

