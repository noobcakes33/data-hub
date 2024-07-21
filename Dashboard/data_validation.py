from datetime import datetime, timedelta
import os
import time
import pandas as pd 
from .categories import genify_category_list
from .countries import genify_country_list
import requests
import io


class FileValidator:
    def __init__(self, notion_client, database_id):
        self.notion_client = notion_client
        self.database_id = database_id
        self.categories_list = genify_category_list
        self.new_merchants_columns = ['name', 'id', 'category', 'subcategory', 'website', 'logo_url', 'country', 'validation_date', 'status', 'comment']
        self.trx_review_columns = ['description','extracted_merchant_for_review','merchant_id']
        self.country_list = genify_country_list
        self.allowed_exts = ['.png','.jpg','.jpeg']

    # Function to get the latest entries
    def get_latest_entries(self, database_id, last_checked):
        print("[get_latest_entries] init...")
        results = self.notion_client.databases.query(
            **{
                "database_id": database_id,
                "filter": {
                    "and": [
                        {
                            "timestamp": "created_time",
                            "created_time": {
                                "after": (last_checked- timedelta(days=10)).isoformat() 
                            }
                        },
                        {
                            "property": "Validation Comment",
                            "multi_select": {
                                "does_not_contain": "OK"
                            }
                        }
                    ]
                }
            }
        )
        print("[get_latest_entries] ", results['results'])
        return results['results']
    # def get_latest_entries(self, database_id, last_checked):
    #     print("[get_latest_entries] init...")
    #     results = self.notion_client.databases.query(
    #         **{
    #             "database_id": database_id,
    #             "filter": {
    #                 "timestamp": "created_time",
    #                 "created_time": {
    #                     "after": (last_checked - timedelta(days=10)).isoformat()
    #                 }
    #             }
    #         }
    #     )
    #     print("[get_latest_entries] ", results['results'])
    #     return results['results']

    def update_submission_validation(self, page_id, flag):
        self.notion_client.pages.update(
            page_id=page_id,
            properties={
                "Submission Validation": {
                    "select": {
                        "name": flag
                    }
                }
            }
        )
    # Function to update the validation comment
    def update_validation_comment(self, page_id, validation_comments_list):
        print("[update_validation_comment] page_id: ", page_id )
        comments_labels_to_add = []
        for comment in validation_comments_list:
            comments_labels_to_add.append(
                {"name": comment}
            )
        self.notion_client.pages.update(
            page_id=page_id,
            properties={
                "Validation Comment": {
                    "multi_select": comments_labels_to_add
                }
            }
        )
    # Function to poll the Notion database
    def poll_notion_database_and_validate(self):
        print("[poll_notion_database] init...")
        # Load last checked timestamp from file
        if os.path.exists("last_checked.txt"):
            with open("last_checked.txt", "r") as file:
                last_checked_str = file.read().strip()
                last_checked = datetime.fromisoformat(last_checked_str)
        else:
            last_checked = datetime.now() - timedelta(days=10)  # Initialize with the time 5 minutes ago
            print("[last_checked] ", last_checked)

        while True:
            try:
                new_entries = self.get_latest_entries(self.database_id, last_checked)
                for entry in new_entries:
                    validation_comments_list = []
                    if entry['properties']['Type']['select']['name'] == 'Submission':
                        file_url = entry['properties']['Files & media']['files'][0]['file']['url']
                        page_id = entry['id']
                        print("[PAGE_ID] ", page_id)
                        print("[FILE_URL] ", file_url)

                        ## TODO: 
                        ## 1. read the csv/excel file
                        df = self.read_csv_from_url(url=file_url)
                        print("[dataframe]")
                        print(df)
                        print()

                        ## 2. run the validation on the file
                        file_data_type = entry['properties']['Data Type']['select']['name']

                        if file_data_type == "Reviewed Transactions":
                            valid_column_names_txns = self.validate_columns_trx_review(df)
                            if not valid_column_names_txns:
                                validation_comments_list.append("Invalid Column Name")
                        elif file_data_type == "Merchants":
                            valid_column_names_merchants = self.validate_columns_new_merchants(df)
                            if not valid_column_names_merchants:
                                validation_comments_list.append("Invalid Column Name")

                            valid_categories = self.validate_category(df)
                            if not valid_categories:
                                validation_comments_list.append("Invalid Category")

                            valid_countries = self.validate_country(df)
                            if not valid_countries:
                                validation_comments_list.append("Invalid Country")

                            # not important -- the url will be skipped while db population
                            # valid_logo_urls = self.validate_logo_url(df)
                            # if not valid_logo_urls:
                            #     validation_comments_list.append("Invalid Logo URL")

                        ## 3. assign validation comments based on the outcome of the validation
                        ## and update the validation_comment column of the Notion Db for that entry
                        if not len(validation_comments_list):
                            validation_comments_list.append("OK")

                        print("[poll_notion_database] update_validation_comment init...")
                        if validation_comments_list[0] == "OK":
                            self.update_submission_validation(page_id, flag="True")
                        else:
                            self.update_submission_validation(page_id, flag="False")
                        self.update_validation_comment(page_id, validation_comments_list)
                last_checked = datetime.now()
                # Save the last checked timestamp to file
                with open("last_checked.txt", "w") as file:
                    file.write(last_checked.isoformat())
            except Exception as e:
                print(f"Error: {e}")
            time.sleep(300)  # Wait for 5 minutes before checking again
            print("Re-check: init..................................................")
    
    def read_csv_from_url(self, url):
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors
        content = response.content
        if ".csv" in url:
            df = pd.read_csv(io.BytesIO(content))
        else:
            df = pd.read_excel(io.BytesIO(content))
        return df
    
    def validate_logo_url(self, df):
    
        # Get the logo url list from the DataFrame
        logos_url = df['logo_url'].tolist()
        
        # Creat  list for invalid logos URL
        invalid_urls = []

        # Find invalid logos
        for logo in logos_url :
            is_valid = False
            for ext in self.allowed_exts : 
                if type(logo) != float and (ext in logo) :
                    is_valid = True
            if not is_valid and type(logo) != float:
                invalid_urls.append(logo)

        # Validation result
        validation = not invalid_urls
        
        return validation

    def validate_category(self, df):
    
        # Get the category column from the DataFrame
        actual_categories = set(df['category'].tolist())
        
        # Check each category is a Genify Category
        Wrong_categories = [category for category in actual_categories if category not in  self.categories_list]
        
        # Validation result
        is_valid = not Wrong_categories 
       
        return is_valid
    
    def validate_country(self, df):
    
        # Get the country column from the DataFrame in lower case formation 
        actual_countries = set(df['country'].str.lower().tolist()) 
        
        # Find Wrong countries
        Wrong_countries = [country for country in actual_countries if country not in  self.country_list]
        
        # Validation result
        is_valid = not Wrong_countries
        
        return is_valid

    def validate_columns_new_merchants(self, df):
    
        # Get the actual columns from the DataFrame
        actual_columns = df.columns.tolist()
        
        # Find missing columns
        missing_columns = [column for column in self.new_merchants_columns if column not in actual_columns]
        
        # Validation result
        is_valid = not missing_columns 
        
        return is_valid
    
    def validate_columns_trx_review(self, df):
    
        # Get the actual columns from the DataFrame
        actual_columns = df.columns.tolist()
        
        # Find missing columns
        missing_columns = [column for column in self.trx_review_columns if column not in actual_columns]
       
        # Validation result
        is_valid = not missing_columns 
        
        print(missing_columns)

        return is_valid 
    
    def validate_new_merchants_file(self, df):

        v1 = self.validate_columns_new_merchants(df)
        v2 = self.validate_category(df)
        v3 = self.validate_country(df)
        v4 = self.validate_logo_url(df)

        if v1 and v2 and v3 and v4 :
            is_valid = True
        else :
            is_valid = False

        return is_valid
