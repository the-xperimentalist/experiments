
import psycopg2
from ..config import DEMO_DB_CONFIG


def get_mapper_file_check(client_name, dashboard_type, data_type):
    demo_conn = psycopg2.connect(**DEMO_DB_CONFIG)
    demo_cur = demo_conn.cursor()
    query = "SELECT MAX(id) frOM public.api_marketplaceclientsinternaldata"
    # query = f"DELETE FROM public.api_"
    demo_cur.execute(query)
    data = demo_cur.fetchall()

    demo_conn.commit()
    demo_cur.close()
    demo_conn.close()

def get_mapper_file(client_name, file_type):
    demo_conn = psycopg2.connect(**DEMO_DB_CONFIG)
    demo_cur = demo_conn.cursor()
    query = f"""
        SELECT values
        FROM public.api_marketplaceclientsinternaldata
        WHERE constant->>'data_type' = '{file_type}' AND constant->>'client_name' = '{client_name}'
        AND dashboard_type = 'AZ_REPORTING'
    """
    demo_cur.execute(query)
    data = demo_cur.fetchall()

    demo_conn.commit()
    demo_cur.close()
    demo_conn.close()
    return data[0][0]

def get_date_file_with_type(client_name, file_type, start_date, end_date):
    demo_conn = psycopg2.connect(**DEMO_DB_CONFIG)
    demo_cur = demo_conn.cursor()
    query = f"""
        SELECT date, values
        FROM public.api_marketplaceclientsinternaldata
        WHERE constant->>'data_type' = '{file_type}' AND constant->>'client_name' = '{client_name}'
        AND dashboard_type = 'AZ_REPORTING'
        AND date >= '{start_date}' AND date <= '{end_date}'
    """
    demo_cur.execute(query)
    data = demo_cur.fetchall()

    demo_conn.commit()
    demo_cur.close()
    demo_conn.close()
    return data
