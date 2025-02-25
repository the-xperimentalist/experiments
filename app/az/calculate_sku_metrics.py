
import json
import pandas as pd
import psycopg2

from ..utils import get_last_value, get_mapper_file, split_json_list, get_date_file_with_type
from ..config import DEMO_DB_CONFIG


def calculate_complete_sku_metrics(client_name, start_date, end_date, asin_list):

    last_value = get_last_value()
    last_value = last_value if last_value != None else 0

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

    category_list = br.category.unique().tolist()
    category_list = [i for i in category_list if i != None]
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

    sb_dates_list = sb.date.unique().tolist()
    sb_dates_list.sort()

    req_out_list = []
    for category in category_list:
        for dt in sb_dates_list:
            out_dict = {}
            filtered_br = br[(br["date"] == dt) & (br["category"] == category)]
            br_units_sold_total = filtered_br.units_ordered.sum()
            filtered_br["sales_share"] = filtered_br["units_ordered"].apply(lambda x: round(x / br_units_sold_total, 2))
            sales_share_dict = {}
            for i in range(filtered_br.shape[0]):
                sales_share_dict[filtered_br.iloc[i].asin] = float(filtered_br.iloc[i].sales_share)
            filtered_sb = sb[(sb["date"] == dt) & (br["category"] == category)]
            campaign_list = filtered_sb.campaign_name.unique().tolist()
            for campaign in campaign_list:
                filtered_campaign_sb = filtered_sb[filtered_sb["campaign_name"] == campaign]
                for asin in asin_list:
                    out_dict = {
                        "campaign_name": campaign,
                        "asin": asin,
                        "ad_spend": round(float(filtered_campaign_sb.ad_spend.sum()) * sales_share_dict.get(asin, 0), 2),
                        "impressions": round(int(filtered_campaign_sb.impressions.sum()) * sales_share_dict.get(asin, 0), 0),
                        "clicks": round(int(filtered_campaign_sb.clicks.sum()) * sales_share_dict.get(asin, 0), 0),
                        "units_ordered": round(int(filtered_campaign_sb.units_ordered.sum()) * sales_share_dict.get(asin, 0), 0),
                        "product_sales": round(float(filtered_campaign_sb.product_sales.sum()) * sales_share_dict.get(asin, 0), 0),
                        "date": dt,
                        "category": category
                    }
                    req_out_list.append(out_dict)
    sb = pd.DataFrame(req_out_list)

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

    filtered_sd = sd[(sd["date"] >= start_date) & (sd["date"] <= end_date) & sd["asin"].isin(asin_list)]
    filtered_sp = sp[(sp["date"] >= start_date) & (sp["date"] <= end_date) & sp["asin"].isin(asin_list)]
    filtered_br = br[(br["date"] >= start_date) & (br["date"] <= end_date) & br["asin"].isin(asin_list)]
    filtered_sb = sb[sb["asin"].isin(asin_list)] # ASIN filter not added yet

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
    output_data["ad_clicks"] = []

    for date_val in dates_list:
        filtered_date_sd = filtered_sd[filtered_sd["date"] == date_val]
        filtered_date_sp = filtered_sp[filtered_sp["date"] == date_val]
        filtered_date_br = filtered_br[filtered_br["date"] == date_val]
        filtered_date_sb = filtered_sb[filtered_sb["date"] == date_val] # Without asin filter

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

    output_asin_list = []
    for asin in asin_list:
        filtered_asin_sb = filtered_sb[filtered_sb["asin"] == asin]
        filtered_asin_sp = filtered_sp[filtered_sp["asin"] == asin]
        filtered_asin_sd = filtered_sd[filtered_sd["asin"] == asin]
        asin_br = br[br["asin"] == asin]
        asin_dict = {}
        asin_dict["asin"] = asin
        asin_dict["units_sold"] = int(asin_br.units_ordered.sum())
        asin_dict["product_sales"] = float(asin_br.product_sales.sum())
        asin_dict["total_page_views"] = int(asin_br.total_page_views.sum())
        asin_dict["total_sessions"] = int(asin_br.total_sessions.sum())
        asin_dict["ad_impressions"] = int(filtered_asin_sb.impressions.sum() + filtered_asin_sp.impressions.sum() + filtered_asin_sd.impressions.sum())
        asin_dict["ad_clicks"] = int(filtered_asin_sb.clicks.sum() + filtered_asin_sp.clicks.sum() + filtered_asin_sd.clicks.sum())
        asin_dict["ad_spend"] = round(float(filtered_asin_sb.ad_spend.sum() + filtered_asin_sp.ad_spend.sum() + filtered_asin_sd.ad_spend.sum()), 2)
        asin_dict["ad_units_ordered"] = int(filtered_asin_sb.units_ordered.sum() + filtered_asin_sp.units_ordered.sum() + filtered_asin_sd.units_ordered.sum())
        asin_dict["ad_product_sales"] = round(float(filtered_asin_sb.product_sales.sum() + filtered_asin_sp.product_sales.sum() + filtered_asin_sd.product_sales.sum()), 2)
        asin_dict["aov"] = float(round((asin_dict["product_sales"]) / (asin_dict["units_sold"]), 2)) if (asin_dict["units_sold"]) != 0 else "-1"
        asin_dict["cr_percent"] = float(round( asin_br.units_ordered.sum() * 100 / asin_br.total_sessions.sum(), 2 )) if asin_br.total_sessions.sum() != 0 else -1
        asin_dict["ctr_percent"] = float(round( asin_dict["ad_clicks"] * 100 / asin_dict["ad_impressions"], 2 )) if asin_dict["ad_impressions"] != 0 else -1
        asin_dict["ad_cr_percent"] = float(round( asin_dict["ad_units_ordered"] * 100 / asin_dict["ad_impressions"], 2 )) if asin_dict["ad_impressions"] != 0 else -1
        asin_dict["ad_cpc"] = float(round( asin_dict["ad_spend"] / asin_dict["ad_clicks"], 2 )) if asin_dict["ad_clicks"] != 0 else "-1"
        asin_dict["ad_acos"] = float(round( asin_dict["ad_spend"] * 100 / asin_dict["ad_product_sales"], 2 )) if asin_dict["ad_product_sales"] != 0 else "-1"
        asin_dict["tacos"] = float(round( asin_dict["ad_spend"] * 100 / asin_dict["product_sales"], 2 )) if asin_dict["product_sales"] != 0 else "-1"

        output_asin_list.append(asin_dict)

    return {"date": output_data, "category": output_asin_list}

