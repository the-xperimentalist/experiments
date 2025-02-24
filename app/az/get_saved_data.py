
import json
import pandas as pd

from ..utils import get_mapper_file, split_json_list, get_date_file_with_type
from ..config import DEMO_DB_CONFIG


def get_saved_data(client_name, start_date, end_date, category_list, file_type_list):

    outData = {}
    merge_on_cols = ["date", "category"]
    base_df = pd.DataFrame(columns = ["date", "clicks", "ad_spend", "impressions", "units_ordered", "product_sales", "category"])
    for file_type in file_type_list:
        data_file = get_date_file_with_type(client_name, file_type, start_date, end_date, "AZ_REPORTING")
        sd_data = []
        for item in data_file:
            date = item[0]
            val = json.loads(item[1])
            all_keys = list(val["campaign_name"].keys())
            for key_val in all_keys:
                sd_dict = {}
                sd_dict["date"] = date.strftime('%Y-%m-%d')
                sd_dict["clicks"] = val["clicks"][key_val]
                sd_dict["ad_spend"] = val["ad_spend"][key_val]
                sd_dict["impressions"] = val["impressions"][key_val]
                sd_dict["units_ordered"] = val["units_ordered"][key_val]
                sd_dict["product_sales"] = val["product_sales"][key_val]
                sd_dict["category"] = val["category"][key_val]
                sd_data.append(sd_dict)
        sb = pd.DataFrame(sd_data)
        sb = sb[sb["category"].isin(category_list)]
        base_df = pd.concat([base_df, sb])
        base_df = base_df.groupby(["date", "category"]).sum().reset_index()

    return base_df.to_json(orient="records")
