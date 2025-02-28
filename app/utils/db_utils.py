
import psycopg2
from ..config import DEMO_DB_CONFIG


def get_value_at_time(client_name, file_type, dashboard_type, date_val):
    demo_conn = psycopg2.connect(**DEMO_DB_CONFIG)
    demo_cur = demo_conn.cursor()
    query = f"""
    SELECT id FROM public.api_marketplaceclientsinternaldata
    WHERE constant->>'data_type' = '{file_type}' AND constant->>'client_name' = '{client_name}'
    AND dashboard_type = '{dashboard_type}' AND date = '{date_val}'::DATE
    """
    demo_cur.execute(query)
    data = demo_cur.fetchall(query)
    pass


def check_if_exists(data_type, dashboard_type, client_name=None, date_val=None, delete_val=False):
    demo_conn = psycopg2.connect(**DEMO_DB_CONFIG)
    demo_cur = demo_conn.cursor()

    data_type_const = f" AND constant->>'data_type' = {data_type}"
    client_name_const = f" AND constant->>'client_name' = {client_name}" if client_name != None else ""
    date_const = f" AND date = {date_val}" if date_val != None else ""
    dashboard_type = f"dashboard_type = {dashboard_type}"
    query = f"""
    SELECT *
    FROM public.api_marketplaceclientsinternaldata
    WHERE {dashboard_type} {data_type_const} {client_name_const} {date_const}
    """

    demo_cur.execute(query)
    data = demo_cur.fetchall()

    demo_conn.commit()
    demo_cur.close()
    demo_conn.close()

    if delete_val:
        demo_conn = psycopg2.connect(**DEMO_DB_CONFIG)
        demo_cur = demo_conn.cursor()

        query = f"""
        DELETE FROM public.api_marketplaceclientsinternaldata
        WHERE {dashboard_type} {data_type_const} {client_name_const} {date_const}
        """

        demo_cur.execute(query)
        data = demo_cur.fetchall()

        demo_conn.commit()
        demo_cur.close()
        demo_conn.close()

    return data
