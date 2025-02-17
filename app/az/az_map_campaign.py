
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value
from ..config import DEMO_DB_CONFIG

INSERT_QUERY = """
    INSERT INTO public.api_marketplaceclientsinternaldata (
        id,
        date,
        dashboard_type,
        constant,
        values,
        created_at,
        updated_at
    )
    VALUES (
        1,
        '2025-01-01 00:00:00'::timestamp,
        'az',
        '{"client_name": "c1", "data_type": "sb"}'::jsonb,
        '{"cost_type": "cpc", "clicks": 0, "impressions": 0}'::jsonb,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    );
"""

def upload_campaign_cat_map(campaign_mapper, client_name):
    """
    :campaign_mapper: pd dataframe
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
    campaign_mapper = campaign_mapper[campaign_mapper["Category"].notnull()]

    campaign_mapper = campaign_mapper[["Campaigns", "Category"]]

    campaign_cat_map = {}
    for i in range(campaign_mapper.shape[0]):
        v = campaign_mapper.iloc[i]
        campaign_cat_map[v["Campaigns"]] = v["Category"]

    client_id = 2
    id_val = last_value + 1
    dashboard_type = "AZ_REPORTING"
    constant_val = {
      "client_name": client_name,
      "data_type": "campaign_mapper"
    }

    values = campaign_cat_map
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
