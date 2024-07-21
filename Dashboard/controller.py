from datetime import datetime
import pandas as pd
from .utils import fetch_notion_data


class DataManager:
    def __init__(self):
        pass

    def get_notion_data(self, notion_client, database_id):
        data = fetch_notion_data(notion_client, database_id)
        return data

    def get_data_in_scope(self, data, source_title):
        data_in_scope = []
        for item in data:
            if item["properties"]["Title"]["title"][0]["text"]["content"] == source_title:
                data_in_scope.append(item)
        return data_in_scope

    def filter_data_by_datae_range(self, data, start_date, end_date, source_title):
        filtered_data = []
        for item in data:
            try:
                if (item['properties']['Type']['select']['name'] != 'Source') and (
                        start_date <= datetime.strptime(item['properties']['Date']['date']['start'],
                                                        '%Y-%m-%d').date() <= end_date) and (
                        item["properties"]["Title"]["title"][0]["text"]["content"] == source_title
                ):
                    filtered_data.append(item)
            except Exception as e:
                print(e)
        return filtered_data
