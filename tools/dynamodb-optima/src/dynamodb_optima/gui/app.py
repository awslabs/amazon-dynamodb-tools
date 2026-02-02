"""
Main Streamlit application for dynamodb-optima GUI.

Provides multi-page dashboard for DynamoDB cost optimization recommendations.
"""

import streamlit as st
from datetime import datetime

from dynamodb_optima.database.connection import get_connection
from dynamodb_optima.gui.models import RecommendationFilter
from dynamodb_optima.gui.database import get_available_regions, get_available_tables, get_available_accounts


def main():
    """Main Streamlit application entry point."""
    # Configure page
    st.set_page_config(
        page_title="DynamoDB Cost Optimizer",
        page_icon="💰",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Custom CSS for better styling
    st.markdown(
        """
        <style>
        .main > div {
            padding-top: 2rem;
        }
        .stMetric {
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 0.5rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Initialize database connection
    try:
        connection = get_connection()
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        st.info(
            "Please run discovery and analysis commands first:\n\n"
            "```bash\n"
            "dynamodb-optima discover\n"
            "dynamodb-optima collect\n"
            "dynamodb-optima analyze-capacity\n"
            "dynamodb-optima analyze-table-class\n"
            "dynamodb-optima analyze-utilization\n"
            "```"
        )
        return

    # Render sidebar and get navigation selection
    selected_page, filters = render_sidebar(connection)

    # Route to appropriate page
    if selected_page == "Dashboard":
        from dynamodb_optima.gui.page_modules.dashboard import render_dashboard

        render_dashboard(connection, filters)
    elif selected_page == "Capacity Mode Analysis":
        from dynamodb_optima.gui.page_modules.capacity_analysis import render_capacity_analysis

        render_capacity_analysis(connection, filters)
    elif selected_page == "Table Class Analysis":
        from dynamodb_optima.gui.page_modules.table_class_analysis import render_table_class_analysis

        render_table_class_analysis(connection, filters)
    elif selected_page == "Utilization Analysis":
        from dynamodb_optima.gui.page_modules.utilization_analysis import render_utilization_analysis

        render_utilization_analysis(connection, filters)


def render_sidebar(connection):
    """Render navigation sidebar and return selected page and filters."""
    with st.sidebar:
        st.title("💰 DynamoDB Cost Optimizer")
        st.markdown("---")

        # Navigation
        st.subheader("Navigation")
        selected_page = st.radio(
            "Select Page",
            [
                "Dashboard",
                "Capacity Mode Analysis",
                "Table Class Analysis",
                "Utilization Analysis",
            ],
            label_visibility="collapsed",
        )

        st.markdown("---")

        # Filters
        st.subheader("Filters")

        # Minimum savings filter
        min_savings = st.number_input(
            "Minimum Monthly Savings ($)",
            min_value=0.0,
            value=0.0,
            step=10.0,
            help="Show only recommendations with savings above this threshold",
        )

        # Region filter
        try:
            available_regions = get_available_regions(connection)
            region_filter = None
            if available_regions:
                region_options = ["All Regions"] + available_regions
                selected_region = st.selectbox("Region", region_options)
                if selected_region != "All Regions":
                    region_filter = selected_region
        except Exception:
            region_filter = None

        # Account filter
        try:
            available_accounts = get_available_accounts(connection)
            account_filter = None
            if available_accounts:
                account_options = ["All Accounts"] + available_accounts
                selected_account = st.selectbox("Account", account_options)
                if selected_account != "All Accounts":
                    account_filter = selected_account
        except Exception:
            account_filter = None

        # Table filter (regex support)
        table_filter = st.text_input(
            "Table Name (regex)",
            placeholder="e.g., ^prod- or test",
            help="Filter tables using regex patterns (RE2 syntax). Examples: '^prod-' (starts with prod-), 'test' (contains test), '^(dev|staging)-' (starts with dev- or staging-), '(?i)PROD' (case-insensitive)",
        )
        if not table_filter:
            table_filter = None

        # Status filter
        status_options = ["All Statuses", "pending", "accepted", "rejected", "implemented"]
        selected_status = st.selectbox("Status", status_options)
        status_filter = None if selected_status == "All Statuses" else selected_status

        # Create filter object
        filters = RecommendationFilter(
            min_savings=min_savings,
            region_filter=region_filter,
            table_filter=table_filter,
            status_filter=status_filter,
            account_filter=account_filter,
        )

        st.markdown("---")

        # Footer
        st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if st.button("🔄 Refresh Data"):
            st.rerun()

    return selected_page, filters


if __name__ == "__main__":
    main()
