import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from notion_client import Client
import requests
import io

# Streamlit page configuration
st.set_page_config(page_title="Data Hub Team Progress", layout="wide")

# Initialize the Notion client
notion = Client(auth=st.secrets["NOTION_TOKEN"])


# Function to fetch data from Notion
@st.cache
def fetch_notion_data(database_id):
    response = notion.databases.query(database_id=database_id)
    return response['results']


# Function to download and read CSV/Excel files from URLs
def read_file_from_url(url):
    response = requests.get(url)
    if url.endswith('.csv'):
        df = pd.read_csv(io.StringIO(response.text))
    elif url.endswith('.xlsx'):
        df = pd.read_excel(io.BytesIO(response.content))
    else:
        raise ValueError("Unsupported file format")
    return df


# Streamlit sidebar input for database ID
st.sidebar.title("Settings")
database_id = st.sidebar.text_input("Notion Database ID", st.secrets["DATABASE_ID"])

# Fetch and process data from the "Data Hub Progress" database
data = fetch_notion_data(database_id)

# Process the fetched data to extract file URLs and read them into DataFrames
dfs = []
source_file_url = None
for item in data:
    properties = item['properties']
    file_url = properties['Files & media']['files'][0]['file']['url']

    if properties['Type']['select']['name'] == 'Source':
        source_file_url = file_url
    elif properties['Type']['select']['name'] == 'Submission' and properties['Data Type']['select'][
        'name'] == 'Merchants':
        df = read_file_from_url(file_url)
        dfs.append((properties['Team member']['select']['name'], df))

# Process the source file
if source_file_url:
    source_df = read_file_from_url(source_file_url)
    st.write("Source Data")
    st.dataframe(source_df)

    # Get merchant names from the source file
    merchant_names = set(source_df['extracted_merchant_for_review'].str.lower())

    if dfs:
        team_progress = {}
        overall_collected = set()

        for member_name, member_df in dfs:
            member_merchant_names = set(member_df['name'].str.lower())

            # Calculate individual progress
            collected_count = len(member_merchant_names)
            team_progress[member_name] = collected_count

            # Update overall collected merchants
            overall_collected.update(member_merchant_names)

        # Calculate overall progress
        total_merchants = len(merchant_names)
        collected_merchants = len(overall_collected)
        overall_progress = (collected_merchants / total_merchants) * 100

        st.write("Overall Progress")
        st.write(f"Collected {collected_merchants} out of {total_merchants} merchants.")
        st.write(f"Overall Coverage: {overall_progress:.2f}%")

        # Display individual progress
        st.write("Individual Progress")
        progress_df = pd.DataFrame(list(team_progress.items()), columns=['Team Member', 'Collected Merchants'])
        progress_df['Coverage (%)'] = (progress_df['Collected Merchants'] / total_merchants) * 100
        st.dataframe(progress_df)

        # Plotting the progress
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(progress_df['Team Member'], progress_df['Coverage (%)'], color='skyblue')
        ax.set_title('Team Members Progress')
        ax.set_xlabel('Team Member')
        ax.set_ylabel('Coverage (%)')
        ax.set_ylim(0, 100)
        st.pyplot(fig)
    else:
        st.write("No submitted files found.")
else:
    st.write("Source file not found.")
