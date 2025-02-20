
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value, get_mapper_file
from ..config import DEMO_DB_CONFIG


def upload_br_data(br, client_name):
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

    asin_cat_map = get_mapper_file(client_name, "asin_mapper")

    br["date"] = pd.to_datetime(br["Date"])
    del br["Date"]

    # Clean business report
    br["total_sessions"] = br["Sessions - Total"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del br["Sessions - Total"]

    # br["app_page_views"] = br["Page Views - Mobile App"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    # del br["Page Views - Mobile App"]

    # br["app_page_views_b2b"] = br["Page Views - Mobile APP - B2B"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    # del br["Page Views - Mobile APP - B2B"]

    # br["browser_page_views"] = br["Page Views - Browser"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    # del br["Page Views - Browser"]

    # br["browser_page_views_b2b"] = br["Page Views - Browser - B2B"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    # del br["Page Views - Browser - B2B"]

    br["total_page_views"] = br["Page Views - Total"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del br["Page Views - Total"]

    # br["total_page_views_b2b"] = br["Page Views - Total - B2B"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    # del br["Page Views - Total - B2B"]

    br["units_ordered"] = br["Units Ordered"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del br["Units Ordered"]

    # br["units_ordered_b2b"] = br["Units Ordered - B2B"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    # del br["Units Ordered - B2B"]

    br["product_sales"] = br["Ordered Product Sales"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    del br["Ordered Product Sales"]

    # br["product_sales_b2b"] = br["Ordered Product Sales - B2B"].apply(lambda x: float(x.replace(',', '').replace('₹', '')) if isinstance(x, str) else x)
    # del br["Ordered Product Sales - B2B"]

    br["asin"] = br["(Child) ASIN"]
    del br["(Child) ASIN"]

    br["category"] = br["asin"].apply(lambda x: asin_cat_map.get(x, {}).get("category"))
    br["brand"] = br["asin"].apply(lambda x: asin_cat_map.get(x, {}).get("brand"))

    br["title"] = br["Title"]
    del br["Title"]

    # useful br columns - app_sessions, app_b2b_sessions, browser_sessions, browser_sessions_b2b, total_sessions,
    # total_b2b_sessions, app_page_views, app_page_views_b2b, browser_page_views, browser_page_views_b2b,
    # total_page_views, total_page_views_b2b, units_ordered, units_ordered_b2b, product_sales, product_sales_b2b,
    # asin, category, brand, title

    br = br[["category", "brand", "title", "asin", "product_sales", "units_ordered", "total_page_views", "total_sessions", "date"]]

    values_list = []
    all_dates = br.date.unique().tolist()

    id_val = last_value

    for date in all_dates:
        id_val += 1
        date_val = date
        client_id = 2
        dashboard_type = "AZ_REPORTING"
        constant_val = {
            "client_name": client_name,
            "data_type": "business_report"
        }
        values = br[br["date"] == date].to_json()
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
