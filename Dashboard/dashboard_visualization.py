import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


class DashboardVisualization:
    def __init__(self):
        pass

    # Plotting the progress as a detailed pie chart
    def plot_detailed_pie_chart(self, df, total_txn_count):
        """ Plot detailed pie-chart
        :param df: dataframe
        :param total_txn_count: int
        :return: fig
        """
        # Define the data
        team_members = df['Team Member']
        coverage = df['Coverage (%)']
        reviewed_txn_count = df['Reviewed Transactions']

        # Calculate remaining transactions
        total_reviewed_transactions = reviewed_txn_count.sum()
        remaining_txn_count = total_txn_count - total_reviewed_transactions
        remaining_coverage = (remaining_txn_count / total_txn_count) * 100

        # Append remaining transactions data
        team_members = pd.concat([team_members, pd.Series(['Remaining'])], ignore_index=True)
        coverage = pd.concat([coverage, pd.Series([remaining_coverage])], ignore_index=True)
        reviewed_txn_count = pd.concat([reviewed_txn_count, pd.Series([remaining_txn_count])], ignore_index=True)

        # Create a DataFrame for Plotly
        plot_df = pd.DataFrame({
            'Team Member': team_members,
            'Coverage (%)': coverage,
            'Reviewed Transactions': reviewed_txn_count
        })

        # Create the interactive pie chart
        fig = px.pie(
            plot_df,
            names='Team Member',
            values='Coverage (%)',
            title='Team Members Progress',
            hole=0.3
        )

        # Add custom hover data
        fig.update_traces(
            hoverinfo='label+percent+value',
            textinfo='percent',
            textposition='inside',
            insidetextorientation='radial'
        )

        return fig

    def plot_reviewed_txns_scatter_plot(self, filtered_df_ngrams, selected_member):
        # Plot for ngrams transactions
        fig_ngrams = go.Figure()

        # Add scatter plot for valid transactions with hover text
        fig_ngrams.add_scatter(x=filtered_df_ngrams['Date'],
                               y=filtered_df_ngrams['valid_ngrams_transactions'],
                               mode='markers+lines',
                               name='Valid Transactions',
                               hovertemplate=
                               # '\n<b>Date</b>: %{x}<br>' +
                               '<b>Valid Transactions</b>: %{y}<br>' +
                               '<b>Valid Coverage</b>: %{text}',
                               text=[f'{coverage:.2%}' for coverage in
                                     filtered_df_ngrams['valid_ngrams_transactions_coverage']])

        # Add scatter plot for invalid transactions with hover text
        fig_ngrams.add_scatter(x=filtered_df_ngrams['Date'],
                               y=filtered_df_ngrams['invalid_ngrams_transactions'],
                               mode='markers+lines',
                               name='Invalid Transactions',
                               hovertemplate=
                               # '\n<b>Date</b>: %{x}<br>' +
                               '<b>Invalid Transactions</b>: %{y}<br>' +
                               '<b>Invalid Coverage</b>: %{text}',
                               text=[f'{coverage:.2%}' for coverage in
                                     filtered_df_ngrams['invalid_ngrams_transactions_coverage']])

        # Customize layout for ngrams
        fig_ngrams.update_layout(
            title=f"Progress of {selected_member} - Ngrams Transactions Over Time",
            xaxis_title="Date",
            yaxis_title="Number of Transactions",
            legend_title="Transaction Type",
            hovermode="x unified"
        )

        return fig_ngrams

    def plot_merchants_scatter_plot(self, filtered_df_merchants, selected_member):
        # Plot for merchants
        fig_merchants = go.Figure()

        filtered_df_merchants = filtered_df_merchants.sort_values("Date", ascending=True)
        fig_merchants.add_trace(go.Scatter(
            x=filtered_df_merchants['Date'],
            y=filtered_df_merchants['number_of_merchants'],
            mode='lines+markers',
            name='Number of Merchants',
            text=filtered_df_merchants['File'],
            hovertemplate=
            # '\n<b>Date</b>: %{x}<br>' +
            '<b>Number of Merchants</b>: %{y}<br>',
            hoverinfo='text+y'
        ))

        fig_merchants.add_trace(go.Scatter(
            x=filtered_df_merchants['Date'],
            y=filtered_df_merchants['number_of_new_merchants'],
            mode='lines+markers',
            name='Number of New Merchants',
            text=filtered_df_merchants['File'],
            hovertemplate=
            # '\n<b>Date</b>: %{x}<br>' +
            '<b>Number of New Merchants</b>: %{y}<br>',
            hoverinfo='text+y'
        ))

        # Customize layout for merchants
        fig_merchants.update_layout(
            title=f"Progress of {selected_member} - Merchants Over Time",
            xaxis_title="Date",
            yaxis_title="Number of Merchants",
            legend_title="Merchants Type",
            hovermode="x unified"
        )

        return fig_merchants
