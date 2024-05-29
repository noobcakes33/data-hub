import io
import pandas as pd
import requests
import streamlit as st


# Function to fetch data from Notion
@st.cache_data
def fetch_notion_data(_notion_client, database_id):
    response = _notion_client.databases.query(database_id=database_id)
    return response['results']


# Function to read gzipped CSV file from a URL
@st.cache_data
def read_gzipped_csv_file(file_path):
    try:
        # Read the gzipped content as binary
        with requests.get(file_path, stream=True) as response:
            response.raise_for_status()
            df = pd.read_csv(io.BytesIO(response.content), compression='gzip')
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


# Function to download and read CSV/Excel files from URLs
@st.cache_data
def read_file_from_url(url):
    response = requests.get(url)
    if response.status_code == 200:
        content_type = response.headers['Content-Type']
        if 'text/csv' in content_type or url.endswith('.csv'):
            df = pd.read_csv(io.StringIO(response.text))
        elif 'application/gzip' in content_type or url.endswith('.csv.gz'):
            df = read_gzipped_csv_file(file_path=url)
        elif 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type or url.endswith(
                '.xlsx'):
            df = pd.read_excel(io.BytesIO(response.content))
        else:
            raise ValueError("Unsupported file format")
        return df
    elif response.status_code == 403:
        raise ValueError(f"Access denied to file: {url}. Please check if the URL is correct and accessible.")
    else:
        raise ValueError(f"Failed to download file: status code {response.status_code}")


def get_dataframes_and_properties(properties, df_list):
    team_member = properties['Team Member']['select']['name']
    file_name = properties['Files & media']['files'][0]['name']
    file_date = properties['Date']['date']['start']
    file_url = properties['Files & media']['files'][0]['file']['url']

    try:
        df = read_file_from_url(file_url)
        df_list.append(
            (
                team_member,
                file_name,
                file_date,
                df
            )
        )
    except ValueError as e:
        st.error(f"Error reading file from {file_url}: {e}")


def read_and_display_source_file(source_file_url, source_title, source_filename):
    try:
        source_df = read_file_from_url(source_file_url)
        st.write("### Source Data")
        st.write(f"#### {source_title}")
        st.write(f"##### Source filename: {source_filename}")
        st.dataframe(source_df)
        return source_df
    except ValueError as e:
        st.error(f"Error reading source file: {e}")
        return None


def process_new_merchants_data(dfs_new_merchants):
    team_progress_merchants = {}
    overall_collected = set()

    for member_name, member_filename, submission_date, member_df in dfs_new_merchants:
        member_merchant_names = set(member_df['name'].str.lower())
        # Calculate individual progress
        collected_count = len(member_merchant_names)
        team_progress_merchants[member_name] = team_progress_merchants.get(member_name, 0) + collected_count
        # Update overall collected merchants
        overall_collected.update(member_merchant_names)

    # Calculate overall progress
    collected_merchants = len(overall_collected)
    st.write("## Overall Progress")
    st.write(f"- **Total Merchants Collected:** {collected_merchants}")

    overall_collected_merchants_df = pd.DataFrame(columns=["Merchant Name"])
    overall_collected_merchants_df["Merchant Name"] = [merchant.title() for merchant in list(overall_collected)]
    st.dataframe(overall_collected_merchants_df)

    # Display individual progress
    st.write("## Individual Progress")
    st.write("### Merchants")
    progress_df_merchants = pd.DataFrame(list(team_progress_merchants.items()),
                                         columns=['Team Member', 'Collected Merchants'])
    st.dataframe(progress_df_merchants)

