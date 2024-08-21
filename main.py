import threading
import time
import streamlit as st
import pandas as pd
from pprint import pprint
from notion_client import Client
from Dashboard.dashboard_visualization import DashboardVisualization
from Dashboard.data_validation import FileValidator
from Dashboard.transaction_population import TxnPopulationManager
from Dashboard.utils import read_file_from_url, get_dataframes_and_properties, read_and_display_source_file, \
    process_new_merchants_data, find_overlapping_descriptions, process_filtered_data, load_cache
from Dashboard.dashboard_generator import DashboardGenerator
from Dashboard.controller import DataManager

# Read secrets
notion_token = st.secrets["NOTION_TOKEN"]
database_id = st.secrets["DATABASE_ID"]

# Initialize Dashboard Visualizer
visualizer = DashboardVisualization()

# Initialize Dashboard Generator
dashboard_generator = DashboardGenerator()

# Initialize Data Manager
data_manager = DataManager()

# Initialize the Notion client
notion_client = Client(auth=notion_token)

# Initialize the Data Validator
validator = FileValidator(notion_client=notion_client, database_id=database_id)

# Initialize the txn population manager
txn_population_manager = TxnPopulationManager(notion_client=notion_client, database_id=database_id)

# Start the polling in a separate thread
threading.Thread(target=validator.poll_notion_database_and_validate, daemon=True).start()

# Function to run the population pipeline periodically
def run_population_periodically():
    while True:
        txn_population_manager.run_population_pipeline()
        time.sleep(60 * 30)  # Sleep for 30 minutes

# Start the periodic population thread
periodic_thread = threading.Thread(target=run_population_periodically)
periodic_thread.daemon = True
periodic_thread.start()


# Start the polling in a separate thread
# threading.Thread(target=txn_population_manager.run_population_pipeline, daemon=True).start()


# Streamlit page configuration
st.set_page_config(page_title="Data Hub Team Progress", layout="wide")

# Fetch and process data from the "Data Hub Progress" Notion Database
data = data_manager.get_notion_data(notion_client, database_id)
date_range, source_file_selection = dashboard_generator.init_sidebar(data)
start_date, end_date = date_range
source_title, source_file_url, source_filename = source_file_selection

# Filter data based on source title
data_in_scope = data_manager.get_data_in_scope(data, source_title)

# Initialize or load the processed files cache
if 'processed_files_cache' not in st.session_state:
    st.session_state.processed_files_cache = load_cache()
processed_files_cache = st.session_state.processed_files_cache

# Filter data based on date range and also get all Ngrams, Merchants, and Reviewed Transactions files
filtered_data = data_manager.filter_data_by_datae_range(data, start_date=start_date, end_date=end_date,
                                                        source_title=source_title)

# Process the fetched data to extract file URLs and read them into DataFrames
dfs_new_merchants, dfs_reviewed_transactions, dfs_ngrams = process_filtered_data(filtered_data, processed_files_cache)

# Process the source file
if source_file_url:
    try:
        source_df = read_and_display_source_file(source_file_url, source_title, source_filename)
        source_transactions = source_df["description"].values.tolist()

        if dfs_new_merchants:
            process_new_merchants_data(dfs_new_merchants)

        if dfs_reviewed_transactions:
            team_progress_transactions = {}
            overall_reviewed_transactions = []
            team_progress_ngrams = {}
            overall_reviewed_ngrams = []
            team_progress_ngram_transactions = {}

            # This is a list of dataframes for reviewed transactions used to check for overlap in reviews
            overall_reviewed_ngram_transactions_dfs = [member_df for
                                                       member_name, member_filename, submission_date, member_df in
                                                       dfs_reviewed_transactions]
            unique_overlapped_reviewed_transactions = find_overlapping_descriptions(
                dfs=overall_reviewed_ngram_transactions_dfs)
            overlapped_reviewed_transactions = source_df[
                source_df["description"].isin(unique_overlapped_reviewed_transactions)]["description"].values.tolist()
            overlapped_reviewed_transactions_count = len(overlapped_reviewed_transactions)

            for member_name, member_filename, submission_date, member_df in dfs_reviewed_transactions:
                member_df["overlapping_txn"] = 0
                overlapping_txn_idx = member_df[member_df['description'].isin(overlapped_reviewed_transactions)].index
                member_df.loc[overlapping_txn_idx, "overlapping_txn"] = 1
                # member_df = member_df[~member_df['description'].isin(overlapped_reviewed_transactions)].reset_index(
                #     drop=True)
                member_reviewed_transactions = member_df[member_df["overlapping_txn"] == 0][
                    'description'].values.tolist()
                # Member Valid Ngrams
                merchant_id_col = "merchant_id"
                if merchant_id_col not in member_df:
                    merchant_id_col = "merchant_Id"
                if merchant_id_col not in member_df:
                    merchant_id_col = "Merchant ID"

                ngram_col = "key"
                if ngram_col not in member_df.columns.tolist():
                    if "extracted_merchant_for_review" in member_df.columns.tolist():
                        ngram_col = "extracted_merchant_for_review"
                    elif "merchant_for_review" in member_df.columns.tolist():
                        ngram_col = "merchant_for_review"
                # print(member_df.columns.tolist())
                # print("[ngram_col] ", ngram_col)
                invalid_ngram_query = (
                        (member_df[merchant_id_col].isin([0, "0", "?"])) |
                        (member_df[merchant_id_col].isna())
                )
                # Member Valid Ngrams
                member_valid_ngrams = member_df[~invalid_ngram_query][ngram_col].unique().tolist()

                # Member Invalid Ngrams
                member_invalid_ngrams = member_df[invalid_ngram_query][ngram_col].unique().tolist()

                # Valid Ngrams Transactions and Coverage
                member_valid_ngrams_transactions = member_df[~invalid_ngram_query]["description"]
                member_valid_ngrams_transactions_coverage = len(member_valid_ngrams_transactions) / len(member_df)

                # Invalid Ngrams Transactions and Coverage
                member_invalid_ngrams_transactions = member_df[invalid_ngram_query]["description"]
                member_invalid_ngrams_transactions_coverage = len(member_invalid_ngrams_transactions) / len(member_df)

                # Number of merchants
                number_of_merchants = member_df[~invalid_ngram_query][ngram_col].nunique()

                # Number of new merchants
                number_of_new_merchants = member_df[
                    (~member_df[merchant_id_col].isna()) &
                    (member_df[merchant_id_col].astype(str).str.startswith("n-"))
                    ][ngram_col].nunique()

                # Initialize the nested dictionary structure if it does not exist
                if member_name not in team_progress_ngram_transactions:
                    team_progress_ngram_transactions[member_name] = {}

                if submission_date not in team_progress_ngram_transactions[member_name]:
                    team_progress_ngram_transactions[member_name][submission_date] = {}

                # Store the calculated metrics
                team_progress_ngram_transactions[member_name][submission_date][member_filename] = {
                    "valid_ngrams_transactions": len(member_valid_ngrams_transactions),
                    "invalid_ngrams_transactions": len(member_invalid_ngrams_transactions),
                    "valid_ngrams_transactions_coverage": member_valid_ngrams_transactions_coverage,
                    "invalid_ngrams_transactions_coverage": member_invalid_ngrams_transactions_coverage,
                    "number_of_merchants": number_of_merchants,
                    "number_of_new_merchants": number_of_new_merchants
                }
                print("[team_progress_ngram_transactions]")
                pprint(team_progress_ngram_transactions)

                # Calculate individual progress
                # reviewed_transactions_count = len(member_reviewed_transactions)
                reviewed_transactions_count = len(
                    source_df[source_df["description"].isin(member_reviewed_transactions)])
                if member_name not in team_progress_transactions:
                    team_progress_transactions[member_name] = reviewed_transactions_count
                else:
                    team_progress_transactions[member_name] += reviewed_transactions_count

                if member_name not in team_progress_ngrams:
                    team_progress_ngrams[member_name] = {
                        "valid_ngrams": len(member_valid_ngrams),
                        "invalid_ngrams": len(member_invalid_ngrams)
                    }
                else:
                    team_progress_ngrams[member_name]["valid_ngrams"] += len(member_valid_ngrams)
                    team_progress_ngrams[member_name]["invalid_ngrams"] += len(member_invalid_ngrams)

                # Update overall reviewed transactions
                overall_reviewed_transactions += member_reviewed_transactions
                overall_reviewed_ngrams += member_valid_ngrams
                overall_reviewed_ngrams += member_invalid_ngrams

            # Update overall reviewed transactions by adding the overlapped transactions if any
            overall_reviewed_transactions += overlapped_reviewed_transactions

            # Calculate overall progress
            total_transactions_count = len(source_transactions)
            # overall_reviewed_transactions_count = len(overall_reviewed_transactions)
            overall_reviewed_transactions_count = len(
                source_df[source_df["description"].isin(overall_reviewed_transactions)])
            overall_reviewed_transactions_progress = (
                                                             overall_reviewed_transactions_count / total_transactions_count
                                                     ) * 100

            st.write("## Overall Reviewed Transactions Progress")
            st.write(
                f"- **Total Reviewed Transactions:** {overall_reviewed_transactions_count} out of {total_transactions_count}")
            st.write(f"- **Overall Coverage:** {overall_reviewed_transactions_progress:.2f}%")

            st.write("### Reviewed Transactions")
            progress_df_transactions = pd.DataFrame(list(team_progress_transactions.items()),
                                                    columns=['Team Member', 'Reviewed Transactions'])
            if overlapped_reviewed_transactions:
                progress_df_transactions.loc[-1, "Team Member"] = "Overlapped Reviewed Transactions"
                progress_df_transactions.loc[-1, "Reviewed Transactions"] = overlapped_reviewed_transactions_count

            progress_df_transactions['Coverage (%)'] = (progress_df_transactions[
                                                            'Reviewed Transactions'] / total_transactions_count) * 100
            st.dataframe(progress_df_transactions)

            st.write("### Reviewed Ngrams")
            # Transform the dictionary into a list of tuples
            data = [(member, info['valid_ngrams'], info['invalid_ngrams']) for member, info in
                    team_progress_ngrams.items()]

            # Create Ngram progress DataFrame
            progress_df_ngrams = pd.DataFrame(data, columns=['Team Member', 'Valid Ngrams', 'Invalid Ngrams'])
            progress_df_ngrams["Total Ngrams"] = progress_df_ngrams["Valid Ngrams"] + progress_df_ngrams[
                "Invalid Ngrams"]
            st.dataframe(progress_df_ngrams)

            # Plotting the progress on Pie Chart
            st.write("## Progress Charts")
            fig = visualizer.plot_detailed_pie_chart(progress_df_transactions, total_txn_count=total_transactions_count)
            st.plotly_chart(fig)

            # Flatten the nested dictionary into two separate lists for two DataFrames
            data_ngrams = []
            data_merchants = []

            for member_name, dates in team_progress_ngram_transactions.items():
                for date, files in dates.items():
                    for filename, metrics in files.items():
                        # Data for ngrams transactions
                        data_ngrams.append({
                            "Team Member": member_name,
                            "Date": date,
                            "File": filename,
                            "valid_ngrams_transactions": metrics["valid_ngrams_transactions"],
                            "invalid_ngrams_transactions": metrics["invalid_ngrams_transactions"],
                            "valid_ngrams_transactions_coverage": metrics["valid_ngrams_transactions_coverage"],
                            "invalid_ngrams_transactions_coverage": metrics["invalid_ngrams_transactions_coverage"]
                        })
                        # Data for merchants
                        data_merchants.append({
                            "Team Member": member_name,
                            "Date": date,
                            "File": filename,
                            "number_of_merchants": metrics["number_of_merchants"],
                            "number_of_new_merchants": metrics["number_of_new_merchants"]
                        })

            # Create DataFrames
            df_ngrams = pd.DataFrame(data_ngrams)
            df_merchants = pd.DataFrame(data_merchants)

            # Convert the Date columns to datetime type
            df_ngrams['Date'] = pd.to_datetime(df_ngrams['Date'])
            df_merchants['Date'] = pd.to_datetime(df_merchants['Date'])

            # Streamlit app
            st.title("Team Progress Ngram Transactions and Merchants")

            # Filter data for a specific team member if needed
            selected_member = st.selectbox("Select Team Member:", df_ngrams["Team Member"].unique())
            filtered_df_ngrams = df_ngrams[df_ngrams["Team Member"] == selected_member]
            filtered_df_merchants = df_merchants[df_merchants["Team Member"] == selected_member]
            # print("[filtered_df_ngrams] \n")
            # print(filtered_df_ngrams)

            filtered_df_ngrams = filtered_df_ngrams.sort_values("Date", ascending=True)

            # Plot for reviewed ngrams transactions
            fig_ngrams = visualizer.plot_reviewed_txns_scatter_plot(filtered_df_ngrams, selected_member)

            # Plot for merchants
            fig_merchants = visualizer.plot_merchants_scatter_plot(filtered_df_merchants, selected_member)

            # Show the plots in Streamlit
            st.plotly_chart(fig_ngrams)
            st.plotly_chart(fig_merchants)

        else:
            st.write("No submitted files found.")
    except ValueError as e:
        st.error(f"Error reading source file: {e}")
else:
    st.write("Source file not found.")
