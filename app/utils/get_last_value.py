
import psycopg2
from ..config import DEMO_DB_CONFIG


def get_last_value():
    demo_conn = psycopg2.connect(**DEMO_DB_CONFIG)
    demo_cur = demo_conn.cursor()
    query = "SELECT MAX(id) frOM public.api_marketplaceclientsinternaldata"
    demo_cur.execute(query)
    data = demo_cur.fetchall()

    demo_conn.commit()
    demo_cur.close()
    demo_conn.close()

    return data[0][0]
