from datetime import datetime
from .dashboard_visualization import DashboardVisualization
import streamlit as st


class DashboardGenerator:
    def __init__(self):
        self.visualizer = DashboardVisualization()

    def generate_full_dashboard(self):
        # TODO:
        pass

    def init_sidebar(self, data):
        # Streamlit sidebar input for database ID
        st.sidebar.title("Data Hub Dashboard Settings")

        # Sidebar date range filter
        date_range = st.sidebar.date_input("Select Date Range",
                                           [datetime.now().replace(month=1, day=1).date(), datetime.now().date()])

        # Extract source files for dropdown selection
        source_files = [(item['properties']['Title']['title'][0]['text']['content'],
                         item['properties']['Files & media']['files'][0]['file']['url'],
                         item['properties']['Files & media']['files'][0]['name'])
                        for item in data if item['properties']['Type']['select']['name'] == 'Source']

        # Sidebar source file selection
        source_file_selection = st.sidebar.selectbox("Select Source File", source_files, format_func=lambda x: x[0])
        print("[source_file_selection] ", source_file_selection)

        return date_range, source_file_selection
