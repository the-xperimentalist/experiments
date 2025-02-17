
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value, get_mapper_file, split_json_list
from ..config import DEMO_DB_CONFIG


def upload_sd_data(sd, client_name):
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
    client_name = "Himanshu"

    asin_cat_map = get_mapper_file(client_name, "asin_mapper")

    sd["date"] = pd.to_datetime(sd["Date"])
    del sd["Date"]

    sd["campaign_name"] = sd["Campaign Name"]
    del sd["Campaign Name"]

    sd["cost_type"] = sd["Cost Type"]
    del sd["Cost Type"]

    sd["asin"] = sd["Advertised ASIN"]
    del sd["Advertised ASIN"]

    sd["impressions"] = sd["Impressions"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sd["Impressions"]

    sd["clicks"] = sd["Clicks"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sd["Clicks"]

    sd["units_ordered"] = sd["14 Day Total Units (#)"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sd["14 Day Total Units (#)"]

    sd["product_sales"] = sd["14 Day Total Sales (₹)"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sd["14 Day Total Sales (₹)"]

    sd["ad_spend"] = sd["Spend"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sd["Spend"]
    print(2)


    sd["category"] = sd["asin"].apply(lambda x: asin_cat_map.get(x, {}).get("category"))
    sd["brand"] = sd["asin"].apply(lambda x: asin_cat_map.get(x, {}).get("brand"))

    sd = sd[["campaign_name", "date", "cost_type", "clicks", "ad_spend", "impressions", "units_ordered", "product_sales", "category", "brand", "asin"]]

    # id - last value
    # date - looped through
    # dashboard type - az reporting
    # constant - {'client_name': 'himanshu', 'type': 'sponsored_brands'}
    # values - // as caught from the above loop
    # created_at - now
    # updated_at - now

    values_list = []
    all_dates = sd.date.unique().tolist()
    print(all_dates)

    id_val = last_value

    for date in all_dates:
        id_val = id_val + 1
        date_val = date
        client_id = 2
        dashboard_type = "AZ_REPORTING"
        constant_val = {
            "client_name": client_name,
            "data_type": "sponsored_display"
        }
        values = sd[sd["date"] == date].to_json()
        values_list.append(
            (
                id_val,
                date_val,
                client_id,
                dashboard_type,
                json.dumps(constant_val),
                json.dumps(values)))
        print(id_val)
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

