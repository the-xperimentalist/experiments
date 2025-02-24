
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value
from ..config import DEMO_DB_CONFIG


def upload_fsn_cat_map(fsn_map_df, client_name):
    """
    :fsn_map_df: pd dataframe
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

    fsn_map_df["title"] = fsn_map_df["Product Title"].apply(lambda x: x.replace("'", ""))
    fsn_map_df["sku_id"] = fsn_map_df["Seller SKU Id"].apply(lambda x: x.replace("'", ""))
    fsn_map_df["sub_category"] = fsn_map_df["Sub-category"].apply(lambda x: x.replace("'", ""))
    fsn_map_df["fsn"] = fsn_map_df["Flipkart Serial Number"].apply(lambda x: x.replace("'", ""))
    fsn_map_df["lid"] = fsn_map_df["Listing ID"].apply(lambda x: x.replace("'", ""))
    fsn_map_df["mrp"] = fsn_map_df["MRP"]
    fsn_map_df["selling_price"] = fsn_map_df["Your Selling Price"]
    fsn_map_df["fulfilled_by"] = fsn_map_df["Fulfillment By"].apply(lambda x: x.replace("'", ""))
    fsn_map_df["manufacturer"] = fsn_map_df["Manufacturer Details"].apply(lambda x: x.replace("'", ""))
    fsn_map_df["tax_code"] = fsn_map_df["Tax Code"]
    fsn_map_df["shelf_life"] = fsn_map_df["Shelf Life in Months"]
    fsn_map_df["manufacture_date"] = fsn_map_df["Date of Manufacture in dd/MM/yyyy"]
    fsn_map_df["packer"] = fsn_map_df["Packer Details"].apply(lambda x: x.replace("'", ""))

    fsn_map_df = fsn_map_df[["title", "sku_id", "sub_category", "fsn", "lid", "mrp", "selling_price", "fulfilled_by", "manufacturer", "tax_code", "shelf_life", "manufacture_date", "packer"]]
    fsn_map_df.drop(index=fsn_map_df.index[0], axis=0, inplace=True)

    client_id = 2
    id_val = last_value + 1
    dashboard_type = 'FK_REPORTING'
    constant_val = {
      "client_name": client_name,
      "data_type": "fsn_mapper"
    }

    values = json.loads(fsn_map_df.to_json(orient="records"))
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
