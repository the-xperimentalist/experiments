
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value, get_mapper_file, split_json_list
from ..config import DEMO_DB_CONFIG


def upload_sp_data(sp, client_name):
    """
    :sp: pandas df
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

    asin_cat_map = get_mapper_file(client_name, "asin_mapper")

    sp["date"] = pd.to_datetime(sp["Date"])
    del sp["Date"]

    sp["campaign_name"] = sp["Campaign Name"]
    del sp["Campaign Name"]

    sp["asin"] = sp["Advertised ASIN"]
    del sp["Advertised ASIN"]

    sp["impressions"] = sp["Impressions"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sp["Impressions"]

    sp["clicks"] = sp["Clicks"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sp["Clicks"]

    sp["ad_spend"] = sp["Spend"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sp["Spend"]

    sp["units_ordered"] = sp["14 Day Total Units (#)"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sp["14 Day Total Units (#)"]

    sp["product_sales"] = sp["14 Day Total Sales (₹)"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del sp["14 Day Total Sales (₹)"]


    sp["category"] = sp["asin"].apply(lambda x: asin_cat_map.get(x, {}).get("category"))
    sp["brand"] = sp["asin"].apply(lambda x: asin_cat_map.get(x, {}).get("brand"))

    sp = sp[["campaign_name", "date", "asin", "clicks", "ad_spend", "impressions", "units_ordered", "product_sales", "category", "brand"]]

    # id - last value
    # date - looped through
    # dashboard type - az reporting
    # constant - {'client_name': 'himanshu', 'type': 'sponsored_brands'}
    # values - // as caught from the above loop
    # created_at - now
    # updated_at - now

    values_list = []
    all_dates = sp.date.unique().tolist()
    print(all_dates)

    id_val = last_value

    for date in all_dates:
        id_val = id_val + 1
        date_val = date
        client_id = 2
        dashboard_type = "AZ_REPORTING"
        constant_val = {
            "client_name": client_name,
            "data_type": "sponsored_products"
        }
        values = sp[sp["date"] == date].to_json()
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

