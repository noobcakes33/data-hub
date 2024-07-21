import pandas as pd
import psycopg2
import boto3
import uuid
from datetime import datetime
import os
from PIL import Image
import requests
import time
from fake_useragent import UserAgent
import io
from dotenv import load_dotenv

load_dotenv()

## Connect to PostgreSQL database
db = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            database=os.getenv('POSTGRES_DB'),
        )

bucket_name = "pfm-logos"
folder_name="logos"
AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY')
REGION_NAME=os.getenv('REGION_NAME')

## Connect to Amazon S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION_NAME,
)

class TxnPopulationManager:

    def __init__(self, notion_client, database_id) -> None:
        self.database_id = database_id
        self.notion_client=notion_client

    def add_merchant_to_db(self, row, logo_id):
        try:
            country_id, next_genify_merchant_id = self.get_country_id_and_genify_merchant_id(country_name=row["country"].lower())
            category_id, genify_category_id = self.get_category_id_and_genify_category_id(category_name=row["category"])
            row_website = row["website"] if not isinstance(row["website"], float) else ""

            with db.cursor() as cur:
                cur.execute("SELECT MAX(id) FROM merchant")
                next_merchant_id = cur.fetchone()[0] + 1
                cur.execute("""
                    INSERT INTO merchant (id, uuid, date_created, validated, validation_comment, name, type, subtype, website, country_id, source_id, logo_id, genify_merchant_id, category_id, genify_category_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    next_merchant_id,
                    str(uuid.uuid4()),
                    str(datetime.now().astimezone()),
                    True,
                    row["comment"],
                    row["name"],
                    row["category"],
                    row["subcategory"],
                    row_website,
                    country_id,
                    4,  # Source ID placeholder
                    logo_id,
                    next_genify_merchant_id,
                    category_id,
                    genify_category_id,
                ))
                db.commit()
                return next_merchant_id
        except Exception as e:
            print("An error occurred while adding merchant to database:", e)
            db.rollback()
            return None

    def insert_logo_to_db(self, logo_url):
        try:
            with db.cursor() as cur:
                file_url = "logos/" + os.path.basename(logo_url)
                # Check if the logo with the same file_url already exists
                cur.execute("SELECT id FROM logo WHERE file_url = %s", (file_url,))
                existing_logo_id = cur.fetchone()
                
                if existing_logo_id:
                    # Handle the case where the logo with the same file_url already exists
                    print("Logo with file_url already exists. Skipping insertion.")
                    return existing_logo_id[0]  # Return the ID of the existing logo
                
                # Insert the new logo into the database
                cur.execute("INSERT INTO logo (logo_url, file_url) VALUES (%s, %s) RETURNING id", (logo_url, file_url))
                new_logo_id = cur.fetchone()[0]
                
                # Commit the transaction
                db.commit()
                
                return new_logo_id  # Return the ID of the newly inserted logo

        except Exception as e:
            print("An error occurred while inserting logo to database:", e)
            db.rollback()
            return None

    def upload_logo_to_s3(self, row):
        try:
            if isinstance(row["logo_url"], float):
                return None
            print("Attempting to upload logo with URL:", row["logo_url"])
            logo_ext = ".png" # extract_logo_extension(row["logo_url"])
            logo_key = f"logos/{row['name']}{logo_ext}"
    #         logo_content = requests.get(logo_url, headers=headers).content
            
            logo_content = self.convert_jpeg_to_png(logo_url=row["logo_url"])

            print("[uploading] init...")
            response = s3.put_object(
                Bucket=bucket_name,
                Key=logo_key,
                Body=logo_content,
                ACL="public-read",
                ContentType="image/png",  # Assuming PNG format, change if necessary
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return f"https://{bucket_name}.s3.eu-central-1.amazonaws.com/{logo_key}"
            else:
                return None
        except Exception as e:
            print("An error occurred while uploading logo to S3:", e)
            return None

    def extract_logo_extension(self, logo_url):
        """
        Extracts the file extension from the given logo URL.
        
        Args:
        - logo_url (str): The URL of the logo.
        
        Returns:
        - str: The file extension (including the dot), or None if the extension cannot be determined.
        """
        try:
            _, extension = os.path.splitext(logo_url)
            return extension.lower()  # Convert to lowercase for consistency
        except Exception as e:
            print("An error occurred while extracting logo extension:", e)
            return None

    def convert_jpeg_to_png(self, logo_url):
        try:
            # Download the JPEG image
            try:
                ua = UserAgent()
                headers = {'User-Agent': ua.random}
                response = requests.get(logo_url, headers=headers)
            except Exception as e:
                print("Error: Exception 1")
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
                response = requests.get(logo_url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to download image: {response.status_code}")
                return None
            if logo_url.lower().endswith(".png"):
                png_content = response.content
                return png_content

            # Open the image using PIL
            image = Image.open(io.BytesIO(response.content))
            
            # Convert the image to PNG format
            if image.format in ['JPEG', 'JPG']:
                # Create an in-memory byte stream to hold the PNG image
                png_buffer = io.BytesIO()
                # Convert the image to PNG and save it to the byte stream
                image.save(png_buffer, format='PNG')
                # Get the content of the byte stream
                png_content = png_buffer.getvalue()
                return png_content
            else:
                print("The image is not in JPEG format.")
                return None
        except Exception as e:
            print("An error occurred during image conversion:", e)
            return None

    def get_country_id_and_genify_merchant_id(self, country_name):
        try:
            country_id, next_genify_merchant_id = None, None
            # Query the country table to retrieve the country_id
            cur = db.cursor()
            cur.execute(f"SELECT id, iso_2 FROM country WHERE name = %s", (country_name,))
            result = cur.fetchone()
            if result:
                country_id = result[0]
                country_code = result[1].lower()
            else:
                # Handle the case when the country is not found
                print(f"Country '{country_name}' not found in the database.")
                return country_id, next_genify_merchant_id
            
            # Now that we have the country_id, proceed to generate the genify_merchant_id
            # ... (Your logic for generating genify_merchant_id)
            cur.execute("""
                SELECT MAX(CAST(SPLIT_PART(genify_merchant_id, '-', 2) AS INT))
                FROM merchant 
                WHERE genify_merchant_id LIKE %s
            """, (f'{country_code}-%',))
            max_id = cur.fetchone()[0]  # Fetch the maximum ID directly from the database
            next_genify_merchant_id = f"{country_code}-{max_id + 1 if max_id is not None else 0}"
            print(f"[genify_merchant_id] {next_genify_merchant_id}")
            return country_id, next_genify_merchant_id  # Return country_id
        except Exception as e:
            print("An error occurred while retrieving country ID:", e)
            return country_id, next_genify_merchant_id

    def get_category_id_and_genify_category_id(self, category_name):
        try:
            cur = db.cursor()
            cur.execute("SELECT id, genify_category_id FROM category WHERE name_eng = %s", (category_name,))
            result = cur.fetchone()
            if result:
                category_id, genify_category_id = result
                return category_id, genify_category_id
            else:
                print(f"Category '{category_name}' not found in the database.")
                return None, None
        except Exception as e:
            print("An error occurred while retrieving category information:", e)
            return None, None

    def populate_logos_and_merchants(self, df):
        # failed_merchants = []
        # failed_logos = []
        # Iterate over each row
        for index, row in df.iterrows():
            print(f"[index] {index}")
            # Upload logo to S3 and retrieve S3 URL
            s3_logo_url = self.upload_logo_to_s3(row)
            print(f"[s3_logo_url] {s3_logo_url}")
            if s3_logo_url is not None:
                # Insert logo URL into the logo table and retrieve logo ID
                logo_id = self.insert_logo_to_db(s3_logo_url)
                print(f"[logo_id] {logo_id}")
                if logo_id is not None:
                    # Add merchant to database with the obtained logo ID
                    merchant_id = self.add_merchant_to_db(row, logo_id)
                    if merchant_id is not None:
                        # Update DataFrame with merchant ID
                        df.at[index, "merchant_id"] = merchant_id
                        df.at[index, "logo_s3_urls"] = s3_logo_url
                    else:
                        print("Failed to add merchant to database.")
                        # failed_merchants.append(index)
                else:
                    print("Failed to insert logo to database.")
                    # failed_logos.append(index)
            else:
                merchant_id = self.add_merchant_to_db(row, logo_id=None)
                if merchant_id is not None:
                    # Update DataFrame with merchant ID
                    df.at[index, "merchant_id"] = merchant_id
                    df.at[index, "logo_s3_urls"] = s3_logo_url
            print()
            time.sleep(1)
    
    # Function to connect to the database
    def connect_to_db(self):
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            database=os.getenv('POSTGRES_DB'),
        )
        return conn

    # Function to check if a transaction exists and is validated
    def transaction_exists_and_validated(self, conn, description):
        cur = conn.cursor()
        cur.execute("SELECT * FROM transaction WHERE raw_description = %s AND validated = True", (description,))
        return cur.fetchone() is not None

    def insert_transaction(self, conn, description, merchant_name, merchant_details):
        # Fetch merchant details
        merchant_id, category, subtype, website, logo_id = merchant_details

        # Fetch logo URL from the logo table using logo_id
        with conn.cursor() as cur:
            cur.execute("SELECT logo_url FROM logo WHERE id = %s", (logo_id,))
            logo_row = cur.fetchone()
            logo_url = logo_row[0] if logo_row else None

            # Fetch category_id from the category table using category name (name_eng)
            cur.execute("SELECT genify_category_id FROM category WHERE name_eng = %s", (category,))
            category_row = cur.fetchone()
            category_id = category_row[0] if category_row else None

            # Generate UUID for the transaction
            txn_uuid = str(uuid.uuid4())

        # Define transaction data
        country = None
        carbon_footprint = None
        client_id = 1
        status = "complete"
        date = datetime.today().strftime("%Y-%m-%d")
        clean_description = merchant_name
        subcategory_name = subtype
        display_description = merchant_name
        validated = True
        validation_date = datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f")
        validation_comment = None
        merchant_ids = [merchant_id]
        logo_status = "found" if logo_id else "not_found"
        genify_clean_description = merchant_name

        try:
            # Insert transaction into the database
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO transaction (
                        raw_description, category_id, uuid, country, category_name, 
                        merchant_website, logo, carbon_footprint, client_id, status, 
                        date, clean_description, subcategory_name, display_description, 
                        validated, validation_date, validation_comment, merchant_ids, 
                        logo_status, genify_clean_description
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    description, category_id, txn_uuid, country, category, 
                    website, logo_url, carbon_footprint, client_id, status, 
                    date, clean_description, subcategory_name, display_description, 
                    validated, validation_date, validation_comment, merchant_ids, 
                    logo_status, genify_clean_description
                ))
                conn.commit()
        except Exception as e:
            print("Error inserting transaction:", e)
            conn.rollback()
    
    def populate_validated_transaction(self, transaction_df):
        # Connect to the database
        conn = self.connect_to_db()

        # Iterate over the DataFrame
        for index, row in transaction_df.iterrows():
        #     if index < 362:
        #         continue
            time.sleep(0.25)
            description = row["description"]
            if type(description) == float:
                continue
            merchant_name = row["extracted_merchant_for_review"]
            print(f"{index} - [description] {description}")
            print(f"[merchant_name] {merchant_name}")
            
            # Query the merchant table for the required merchant details
            cur = conn.cursor()
            cur.execute("SELECT id, type, subtype, website, logo_id FROM merchant WHERE name = %s AND validated = True", (merchant_name,))
            merchant_details = cur.fetchone()
            print(f"[merchant_details] {merchant_details}")
            
            if merchant_details:
                # Check if the transaction exists and is validated
                if not self.transaction_exists_and_validated(conn, description):
                    # Insert a new transaction record
                    self.insert_transaction(conn, description, merchant_name, merchant_details)
            print()
        # Close the database connection
        conn.close()
    
    def get_entries_to_populate(self, database_id):
        print("[get_entries_to_populate] init...")
        results = self.notion_client.databases.query(
            **{
                "database_id": database_id,
                "filter": {
                    "and": [
                        {
                            "property": "Type",
                            "select": {
                                "equals": "Submission"
                            }
                        },
                        {
                            "property": "Populated",
                            "select": {
                                "is_empty": True
                            }
                        }
                    ]
                },
                "sorts": [
                    {
                        "property": "Date",
                        "direction": "ascending"
                    }
                ]
            }
        )
        print("[get_entries_to_populate] ", results['results'])
        return results['results']

    def read_csv_from_url(self, url):
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors
        content = response.content
        if ".csv" in url:
            df = pd.read_csv(io.BytesIO(content))
        else:
            df = pd.read_excel(io.BytesIO(content))
        return df

    def update_population_flag(self, page_id: str, comment: str) -> None:
        """
        Update the Population flag of a given Notion page with a comment.

        :param page_id: The ID of the Notion page to update.
        :param comment: The comment to set for the Population flag.
        """
        try:
            print("[update_population_flag] page_id:", page_id)
            response = self.notion_client.pages.update(
                page_id=page_id,
                properties={
                    "Populated": {
                        "select": {
                            "name": comment
                        }
                    }
                }
            )
            print("[update_population_flag] Success:", response)
        except Exception as e:
            print(f"[update_population_flag] Error updating page {page_id}: {e}")


    def run_population_pipeline(self):
        print("[run_population_pipeline] init...")
        entries = self.get_entries_to_populate(database_id=self.database_id)

        merchants_entries = []
        reviewed_transactions_entries = []

        for entry in entries:
            file_data_type = entry['properties']['Data Type']['select']['name']
            if file_data_type == "Merchants":
                merchants_entries.append(entry)
            elif file_data_type == "Reviewed Transactions":
                reviewed_transactions_entries.append(entry)

        # Process "Merchants" entries first
        for entry in merchants_entries:
            file_url = entry['properties']['Files & media']['files'][0]['file']['url']
            df_merchants = self.read_csv_from_url(file_url)
            self.populate_logos_and_merchants(df=df_merchants)
            self.update_population_flag(page_id=entry["id"], comment="Done")

        # Process "Reviewed Transactions" entries next
        for entry in reviewed_transactions_entries:
            file_url = entry['properties']['Files & media']['files'][0]['file']['url']
            df_transactions = self.read_csv_from_url(file_url)
            self.populate_validated_transaction(df_transactions)
            self.update_population_flag(page_id=entry["id"], comment="Done")
