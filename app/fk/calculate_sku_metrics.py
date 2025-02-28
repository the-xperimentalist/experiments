
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value, get_mapper_file, split_json_list, get_date_file_with_type
from ..config import DEMO_DB_CONFIG


def calculate_fk_complete_sku_metrics(client_name, start_date, end_date, fsn_list):

    fsn_cat_map = get_mapper_file(client_name, "fsn_mapper", "FK_REPORTING")
    fsn_dict = {}
    for item in fsn_cat_map:
        fsn_dict[item["fsn"]] = {}
        fsn_dict[item["fsn"]]["category"] = item["sub_category"]
        fsn_dict[item["fsn"]]["price"] = item["selling_price"]

    orders = get_date_file_with_type(client_name, "orders", start_date, end_date, "FK_REPORTING")
    orders_data = []

    for item in orders:
        date = item[0]
        val_df = pd.DataFrame(json.loads(item[1]))
        val_df["category"] = val_df["fsn"].apply(lambda x: fsn_dict[x]["category"])
        val_df["price"] = val_df["fsn"].apply(lambda x: fsn_dict[x]["price"])
        val_df["revenue"] = val_df["quantity"] * val_df["price"]
        val_df = val_df[["fsn", "category", "quantity", "revenue", "order_item_status"]]
        all_fsns = val_df.fsn.unique().tolist()

        delivered_val_df = val_df[val_df["order_item_status"] == "DELIVERED"]
        cancelled_val_df = val_df[val_df["order_item_status"] == "CANCELLED"]

        for fsn in all_fsns:
            fsn_delivered_val_df = delivered_val_df[delivered_val_df["fsn"] == fsn]
            fsn_cancelled_val_df = cancelled_val_df[cancelled_val_df["fsn"] == fsn]
            orders_dict = {}
            orders_dict["date"] = date
            orders_dict["fsn"] = fsn
            orders_dict["category"] = fsn_dict[fsn]["category"]
            orders_dict["units_sold"] = fsn_delivered_val_df.quantity.sum()
            orders_dict["product_sales"] = fsn_delivered_val_df.revenue.sum()
            orders_dict["cancelled_units"] = fsn_cancelled_val_df.quantity.sum()
            orders_data.append(orders_dict)
    orders_df = pd.DataFrame(orders_data)
    orders_df = orders_df[orders_df["fsn"].isin(fsn_list)]
    date_list = orders_df.date.unique().tolist()
    date_list.sort()

    pla_consolidated = get_date_file_with_type(
        client_name, "pla_consolidated", start_date, end_date, "FK_REPORTING")
    pla_consolidated_df = pd.DataFrame()

    for item in pla_consolidated:
        pla_consolidated_dict = {}
        date = item[0]
        val_df = pd.DataFrame(json.loads(item[1]))
        val_df["date"] = date
        pla_consolidated_df = pd.concat([pla_consolidated_df, val_df])

    pla_campaign = get_date_file_with_type(client_name, "pla_campaign", start_date, end_date, "FK_REPORTING")
    pla_campaign_df = pd.DataFrame()

    for item in pla_campaign:
        date = item[0]
        val_df = pd.DataFrame(json.loads(item[1]))
        val_df["category"] = val_df["fsn"].apply(lambda x: fsn_dict[x]["category"])
        val_df["price"] = val_df["fsn"].apply(lambda x: fsn_dict[x]["price"])
        val_df["date"] = date
        pla_campaign_df = pd.concat([pla_campaign_df, val_df])

    ad_df_merged = pd.merge(pla_consolidated_df, pla_campaign_df, on=["campaign_id", "date"], how="outer")
    ad_df_merged["category"] = ad_df_merged["fsn"].apply(lambda x: fsn_dict.get(x, {}).get("category", ""))
    ad_df_merged = ad_df_merged[ad_df_merged["fsn"].isin(fsn_list)]
    ad_df_merged["price"] = ad_df_merged["fsn"].apply(lambda x: fsn_dict[x]["price"])
    ad_df_merged["ad_revenue"] = ad_df_merged["price"]*(ad_df_merged["direct_units_sold"] + ad_df_merged["indirect_units_sold"])

    output_data = {}
    output_data["units_sold"] = []
    output_data["cancelled_units"] = []
    output_data["product_sales"] = []
    output_data["ad_clicks"] = []
    output_data["ad_spend"] = []
    output_data["ad_units_ordered"] = []
    output_data["ad_product_sales"] = []
    output_data["acos"] = []
    output_data["tacos"] = []
    output_data["aov"] = []

    for date_val in date_list:
        filtered_orders_df = orders_df[orders_df["date"] == date_val]
        filtered_ad_df = ad_df_merged[ad_df_merged["date"] == date_val]

        output_data["units_sold"].append(int(filtered_orders_df.units_sold.sum()))
        output_data["cancelled_units"].append(int(filtered_orders_df.cancelled_units.sum()))
        output_data["product_sales"].append(float(filtered_orders_df.product_sales.sum()))
        output_data["ad_spend"].append(float(ad_df_merged.ad_spend.sum()))

        net_ad_units_ordered = int(filtered_ad_df.direct_units_sold.sum()) + int(filtered_ad_df.indirect_units_sold.sum())
        output_data["ad_units_ordered"].append(net_ad_units_ordered)
        output_data["ad_product_sales"].append(float(filtered_ad_df.ad_revenue.sum()))
        output_data["acos"].append(float(filtered_ad_df.ad_spend.sum()) / float(filtered_ad_df.ad_revenue.sum()) if float(filtered_ad_df.ad_revenue.sum()) != 0.0 else 0.0)
        output_data["tacos"].append(float(filtered_ad_df.ad_spend.sum()) / int(filtered_orders_df.product_sales.sum()) if float(filtered_orders_df.product_sales.sum()) != 0.0 else 0)
        output_data["aov"].append(float(filtered_orders_df.product_sales.sum()) / int(filtered_orders_df.units_sold.sum()) if int(filtered_orders_df.units_sold.sum()) != 0 else 0)

    output_data["dates"] =  [i.strftime('%Y-%m-%d') for i in date_list]

    output_fsn_list = []
    for fsn in fsn_list:
        fsn_ad_df = filtered_ad_df[filtered_ad_df["fsn"] == fsn]
        fsn_orders_df = filtered_orders_df[filtered_orders_df["fsn"] == fsn]

        fsn_dict = {}
        fsn_dict["fsn"] = fsn
        fsn_dict["units_sold"] = int(fsn_orders_df.units_sold.sum())
        fsn_dict["cancelled_units"] = int(fsn_orders_df.cancelled_units.sum())
        fsn_dict["product_sales"] = float(fsn_orders_df.product_sales.sum())
        fsn_dict["ad_spend"] = float(fsn_ad_df.ad_spend.sum())
        fsn_dict["ad_units_ordered"] = int(fsn_ad_df.direct_units_sold.sum()) + int(fsn_ad_df.indirect_units_sold.sum())
        fsn_dict["ad_product_sales"] = float(fsn_ad_df.ad_revenue.sum())
        fsn_dict["acos"] = float(fsn_ad_df.ad_spend.sum()) / float(fsn_ad_df.ad_revenue.sum()) if float(fsn_ad_df.ad_revenue.sum()) != 0.0 else 0.0
        fsn_dict["tacos"] = float(fsn_ad_df.ad_spend.sum()) / float(fsn_orders_df.product_sales.sum()) if float(fsn_orders_df.product_sales.sum()) != 0.0 else 0.0
        fsn_dict["aov"] = float(fsn_orders_df.product_sales.sum()) / int(fsn_orders_df.units_sold.sum()) if int(fsn_orders_df.units_sold.sum()) != 0 else 0
        output_fsn_list.append(fsn_dict)

    return {"date": output_data, "category": output_fsn_list}
