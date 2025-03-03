
import json
import pandas as pd
import psycopg2

from ..utils import get_mapper_file, split_json_list, get_date_file_with_type
from ..config import DEMO_DB_CONFIG


def calculate_complete_category_metrics(client_name, start_date, end_date, category_list):

    business_report = get_date_file_with_type(client_name, "business_report", start_date, end_date, "AZ_REPORTING")
    br_data = []
    for item in business_report:
        date = item[0]
        val = json.loads(item[1])
        all_keys = list(val["asin"].keys())
        for key_val in all_keys:
            br_dict = {}
            br_dict["date"] = date
            br_dict["asin"] = val["asin"][key_val]
            br_dict["category"] = val["category"][key_val]
            br_dict["brand"] = val["brand"][key_val]
            br_dict["title"] = val["title"][key_val]
            br_dict["product_sales"] = val["product_sales"][key_val]
            br_dict["units_ordered"] = val["units_ordered"][key_val]
            br_dict["total_page_views"] = val["total_page_views"][key_val]
            br_dict["total_sessions"] = val["total_sessions"][key_val]
            br_data.append(br_dict)
    br = pd.DataFrame(br_data)
    br["date"] = pd.to_datetime(br["date"])

    dates_list = br.date.unique().tolist()

    sponsored_brands = get_date_file_with_type(client_name, "sponsored_brands", start_date, end_date, "AZ_REPORTING")
    sd_data = []
    for item in sponsored_brands:
        date = item[0]
        val = json.loads(item[1])
        all_keys = list(val["campaign_name"].keys())
        for key_val in all_keys:
            sd_dict = {}
            sd_dict["date"] = date
            sd_dict["campaign_name"] = val["campaign_name"][key_val]
            sd_dict["clicks"] = val["clicks"][key_val]
            sd_dict["ad_spend"] = val["ad_spend"][key_val]
            sd_dict["impressions"] = val["impressions"][key_val]
            sd_dict["units_ordered"] = val["units_ordered"][key_val]
            sd_dict["product_sales"] = val["product_sales"][key_val]
            sd_dict["category"] = val["category"][key_val]
            sd_dict["brand"] = val["brand"][key_val]
            sd_data.append(sd_dict)
    sb = pd.DataFrame(sd_data)
    sb["date"] = pd.to_datetime(sb["date"])

    sponsored_display = get_date_file_with_type(client_name, "sponsored_display", start_date, end_date, "AZ_REPORTING")
    sd_data = []
    for item in sponsored_display:
        date = item[0]
        val = json.loads(item[1])
        all_keys = list(val["campaign_name"].keys())
        for key_val in all_keys:
            sd_dict = {}
            sd_dict["date"] = date
            sd_dict["campaign_name"] = val["campaign_name"][key_val]
            sd_dict["asin"] = val["asin"][key_val]
            sd_dict["clicks"] = val["clicks"][key_val]
            sd_dict["ad_spend"] = val["ad_spend"][key_val]
            sd_dict["impressions"] = val["impressions"][key_val]
            sd_dict["units_ordered"] = val["units_ordered"][key_val]
            sd_dict["product_sales"] = val["product_sales"][key_val]
            sd_dict["category"] = val["category"][key_val]
            sd_dict["brand"] = val["brand"][key_val]
            sd_data.append(sd_dict)
    sd = pd.DataFrame(sd_data)
    sd["date"] = pd.to_datetime(sd["date"])

    sponsored_products = get_date_file_with_type(client_name, "sponsored_products", start_date, end_date, "AZ_REPORTING")
    sp_data = []
    for item in sponsored_display:
        date = item[0]
        val = json.loads(item[1])
        all_keys = list(val["campaign_name"].keys())
        for key_val in all_keys:
            sp_dict = {}
            sp_dict["date"] = date
            sp_dict["campaign_name"] = val["campaign_name"][key_val]
            sp_dict["asin"] = val["asin"][key_val]
            sp_dict["clicks"] = val["clicks"][key_val]
            sp_dict["ad_spend"] = val["ad_spend"][key_val]
            sp_dict["impressions"] = val["impressions"][key_val]
            sp_dict["units_ordered"] = val["units_ordered"][key_val]
            sp_dict["product_sales"] = val["product_sales"][key_val]
            sp_dict["category"] = val["category"][key_val]
            sp_dict["brand"] = val["brand"][key_val]
            sp_data.append(sp_dict)
    sp = pd.DataFrame(sp_data)
    sp["date"] = pd.to_datetime(sp["date"])

    # to_calculate_values - units_sold, sales, aov, page_views, sessions, cr_percent, ad_spends, organic_sales,
    # ad_sales, impressions, clicks, ad_units, ctr, ad_cr_percent, cpc, acos, tacos

    filtered_sd = sd[(sd["date"] >= start_date) & (sd["date"] <= end_date) & sd["category"].isin(category_list)]
    filtered_sp = sp[(sp["date"] >= start_date) & (sp["date"] <= end_date) & sp["category"].isin(category_list)]
    filtered_sb = sb[(sb["date"] >= start_date) & (sb["date"] <= end_date) & sb["category"].isin(category_list)]
    filtered_br = br[(br["date"] >= start_date) & (br["date"] <= end_date) & br["category"].isin(category_list)]

    output_data = {}
    output_data["units_sold"] = []
    output_data["product_sales"] = []
    output_data["total_page_views"] = []
    output_data["total_sessions"] = []
    output_data["ad_impressions"] = []
    output_data["ad_clicks"] = []
    output_data["ad_spend"] = []
    output_data["ad_units_ordered"] = []
    output_data["ad_product_sales"] = []
    output_data["cr_percent"] = []
    output_data["ctr_percent"] = []
    output_data["ad_acos"] = []
    output_data["tacos"] = []
    output_data["aov"] = []

    for date_val in dates_list:
        filtered_date_sd = filtered_sd[filtered_sd["date"] == date_val]
        filtered_date_sp = filtered_sp[filtered_sp["date"] == date_val]
        filtered_date_sb = filtered_sb[filtered_sb["date"] == date_val]
        filtered_date_br = filtered_br[filtered_br["date"] == date_val]

        date_units_sold = int(filtered_date_br.units_ordered.sum())
        output_data["units_sold"].append(date_units_sold)

        date_product_sales = float(filtered_date_br.product_sales.sum())
        output_data["product_sales"].append(date_product_sales)

        date_total_page_views = int(filtered_date_br.total_page_views.sum())
        output_data["total_page_views"].append(date_total_page_views)

        date_total_sessions = int(filtered_date_br.total_sessions.sum())
        output_data["total_sessions"].append(date_total_sessions)

        date_ad_impressions = int(filtered_date_sb.impressions.sum() + filtered_date_sp.impressions.sum() + filtered_date_sd.impressions.sum())
        output_data["ad_impressions"].append(date_ad_impressions)

        date_ad_clicks = int(filtered_date_sd.clicks.sum() + filtered_date_sp.clicks.sum() + filtered_date_sb.clicks.sum())
        output_data["ad_clicks"].append(date_ad_clicks)

        date_ad_spend = float(filtered_date_sb.ad_spend.sum() + filtered_date_sp.ad_spend.sum() + filtered_date_sd.ad_spend.sum())
        output_data["ad_spend"].append(date_ad_spend)

        date_ad_units_ordered = int(filtered_date_sd.units_ordered.sum() + filtered_date_sp.units_ordered.sum() + filtered_date_sb.units_ordered.sum())
        output_data["ad_units_ordered"].append(date_ad_units_ordered)

        date_ad_product_sales = float(filtered_date_sb.product_sales.sum() + filtered_date_sp.product_sales.sum() + filtered_date_sd.product_sales.sum())
        output_data["ad_product_sales"].append(date_ad_product_sales)

        date_aov = round(date_product_sales / date_units_sold, 2) if date_units_sold != 0 else 0
        output_data["aov"].append(date_aov)

        date_cr_percent = round(date_units_sold * 100 / date_total_sessions if date_total_sessions != 0 else 0, 2)
        output_data["cr_percent"].append(date_cr_percent)

        date_ad_acos = round(date_ad_spend * 100 / date_ad_product_sales, 2) if date_ad_product_sales != 0 else 0
        output_data["ad_acos"].append(date_ad_acos)

        date_tacos = round(date_ad_spend * 100 / date_product_sales, 2) if date_product_sales != 0 else 0
        output_data["tacos"].append(date_tacos)

    output_data["dates"] = [i.strftime('%Y-%m-%d') for i in dates_list]

    output_category_list = []
    for category in category_list:
        filtered_category_sb = filtered_sb[filtered_sb["category"] == category]
        filtered_category_sp = filtered_sp[filtered_sp["category"] == category]
        filtered_category_sd = filtered_sd[filtered_sd["category"] == category]
        category_br = br[br["category"] == category]
        cat_dict = {}
        cat_dict["category"] = category
        cat_dict["units_sold"] = int(category_br.units_ordered.sum())
        cat_dict["product_sales"] = float(category_br.product_sales.sum())
        cat_dict["total_page_views"] = int(category_br.total_page_views.sum())
        cat_dict["total_sessions"] = int(category_br.total_sessions.sum())
        cat_dict["ad_impressions"] = int(filtered_category_sb.impressions.sum() + filtered_category_sp.impressions.sum() + filtered_category_sd.impressions.sum())
        cat_dict["ad_clicks"] = int(filtered_category_sb.clicks.sum() + filtered_category_sp.clicks.sum() + filtered_category_sd.clicks.sum())
        cat_dict["ad_spend"] = round(float(filtered_category_sb.ad_spend.sum() + filtered_category_sp.ad_spend.sum() + filtered_category_sd.ad_spend.sum()), 2)
        cat_dict["ad_units_ordered"] = int(filtered_category_sb.units_ordered.sum() + filtered_category_sp.units_ordered.sum() + filtered_category_sd.units_ordered.sum())
        cat_dict["ad_product_sales"] = round(float(filtered_category_sb.product_sales.sum() + filtered_category_sp.product_sales.sum() + filtered_category_sd.product_sales.sum()), 2)
        cat_dict["aov"] = float(round((cat_dict["product_sales"]) / (cat_dict["units_sold"]), 2)) if (cat_dict["units_sold"]) != 0 else "-1"
        cat_dict["cr_percent"] = float(round( category_br.units_ordered.sum() * 100 / category_br.total_sessions.sum(), 2 )) if category_br.total_sessions.sum() != 0 else -1
        cat_dict["ctr_percent"] = float(round( cat_dict["ad_clicks"] * 100 / cat_dict["ad_impressions"], 2 )) if cat_dict["ad_impressions"] != 0 else -1
        cat_dict["ad_cr_percent"] = float(round( cat_dict["ad_units_ordered"] * 100 / cat_dict["ad_impressions"], 2 )) if cat_dict["ad_impressions"] != 0 else -1
        cat_dict["ad_cpc"] = float(round( cat_dict["ad_spend"] / cat_dict["ad_clicks"], 2 )) if cat_dict["ad_clicks"] != 0 else "-1"
        cat_dict["ad_acos"] = float(round( cat_dict["ad_spend"] * 100 / cat_dict["ad_product_sales"], 2 )) if cat_dict["ad_product_sales"] != 0 else "-1"
        cat_dict["tacos"] = float(round( cat_dict["ad_spend"] * 100 / cat_dict["product_sales"], 2 )) if cat_dict["product_sales"] != 0 else "-1"

        output_category_list.append(cat_dict)

    return {"date": output_data, "category": output_category_list}
