"""
Dashboard page - summary overview of all recommendations.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

from metrics_collector.gui.models import RecommendationFilter
from metrics_collector.gui.database import (
    get_summary_stats,
    get_capacity_recommendations,
    get_table_class_recommendations,
    get_utilization_recommendations,
)


def render_dashboard(connection, filters: RecommendationFilter):
    """Render summary dashboard page."""
    st.title("📊 Cost Optimization Dashboard")
    st.markdown("Overview of DynamoDB cost optimization opportunities across all tables")

    try:
        # Get summary statistics
        stats = get_summary_stats(connection)

        # Display key metrics in columns
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                label="💰 Total Monthly Savings",
                value=f"${stats.total_monthly_savings:,.2f}",
                delta=f"${stats.total_annual_savings:,.2f}/year",
            )

        with col2:
            st.metric(
                label="📋 Total Recommendations",
                value=stats.total_recommendations,
                delta=f"{stats.not_optimized_count} need action",
            )

        with col3:
            st.metric(
                label="🗂️ Tables Analyzed",
                value=stats.total_tables,
                help="Unique tables with recommendations",
            )

        with col4:
            optimization_rate = (
                (stats.optimized_count / stats.total_recommendations * 100)
                if stats.total_recommendations > 0
                else 0
            )
            st.metric(
                label="✅ Optimization Rate",
                value=f"{optimization_rate:.1f}%",
                help="Percentage of tables already optimized",
            )

        st.markdown("---")

        # Visualization section
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Savings by Category")
            if stats.total_monthly_savings > 0:
                fig = create_savings_pie_chart(stats)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No savings opportunities found. Run analysis commands first.")

        with col2:
            st.subheader("Recommendations by Type")
            if stats.total_recommendations > 0:
                fig = create_recommendations_bar_chart(stats)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No recommendations found. Run analysis commands first.")

        st.markdown("---")

        # Top savings opportunities
        st.subheader("🏆 Top Savings Opportunities")
        render_top_recommendations(connection, filters)

    except Exception as e:
        st.error(f"Error loading dashboard data: {e}")
        st.info(
            "Please ensure you have run the analysis commands:\n\n"
            "```bash\n"
            "metrics-collector analyze-capacity\n"
            "metrics-collector analyze-table-class\n"
            "metrics-collector analyze-utilization\n"
            "```"
        )


def create_savings_pie_chart(stats):
    """Create pie chart showing savings distribution by category."""
    labels = []
    values = []

    if stats.capacity_savings > 0:
        labels.append(f"Capacity Mode (${stats.capacity_savings:,.2f})")
        values.append(stats.capacity_savings)

    if stats.table_class_savings > 0:
        labels.append(f"Table Class (${stats.table_class_savings:,.2f})")
        values.append(stats.table_class_savings)

    if stats.utilization_savings > 0:
        labels.append(f"Utilization (${stats.utilization_savings:,.2f})")
        values.append(stats.utilization_savings)

    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.3,
                marker=dict(colors=["#FF6B6B", "#4ECDC4", "#45B7D1"]),
            )
        ]
    )

    fig.update_layout(
        height=400,
        margin=dict(l=20, r=20, t=40, b=20),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
    )

    return fig


def create_recommendations_bar_chart(stats):
    """Create bar chart showing recommendation counts by type."""
    categories = []
    counts = []

    if stats.capacity_count > 0:
        categories.append("Capacity Mode")
        counts.append(stats.capacity_count)

    if stats.table_class_count > 0:
        categories.append("Table Class")
        counts.append(stats.table_class_count)

    if stats.utilization_count > 0:
        categories.append("Utilization")
        counts.append(stats.utilization_count)

    fig = go.Figure(
        data=[
            go.Bar(
                x=categories,
                y=counts,
                marker=dict(color=["#FF6B6B", "#4ECDC4", "#45B7D1"]),
                text=counts,
                textposition="auto",
            )
        ]
    )

    fig.update_layout(
        height=400,
        margin=dict(l=20, r=20, t=40, b=20),
        xaxis_title="Recommendation Type",
        yaxis_title="Count",
        showlegend=False,
    )

    return fig


def render_top_recommendations(connection, filters: RecommendationFilter):
    """Render table of top recommendations across all types."""
    # Get all recommendations
    capacity_recs = get_capacity_recommendations(connection, filters)
    table_class_recs = get_table_class_recommendations(connection, filters)
    utilization_recs = get_utilization_recommendations(connection, filters)

    # Combine into single list with type
    all_recommendations = []

    for rec in capacity_recs:
        all_recommendations.append(
            {
                "Type": "Capacity Mode",
                "Table": rec.table_name,
                "Region": rec.region,
                "Current": rec.current_billing_mode,
                "Recommended": rec.recommended_billing_mode,
                "Monthly Savings": rec.monthly_savings_usd,
                "Annual Savings": rec.annual_savings_usd,
            }
        )

    for rec in table_class_recs:
        all_recommendations.append(
            {
                "Type": "Table Class",
                "Table": rec.table_name,
                "Region": rec.region,
                "Current": rec.current_table_class,
                "Recommended": rec.recommended_table_class,
                "Monthly Savings": rec.monthly_savings_usd,
                "Annual Savings": rec.annual_savings_usd,
            }
        )

    for rec in utilization_recs:
        all_recommendations.append(
            {
                "Type": "Utilization",
                "Table": rec.table_name,
                "Region": rec.region,
                "Current": f"{rec.current_provisioned_rcu}R/{rec.current_provisioned_wcu}W",
                "Recommended": f"{rec.recommended_provisioned_rcu}R/{rec.recommended_provisioned_wcu}W",
                "Monthly Savings": rec.monthly_savings_usd,
                "Annual Savings": rec.annual_savings_usd,
            }
        )

    if not all_recommendations:
        st.info("No recommendations found matching the current filters.")
        return

    # Convert to DataFrame and sort by savings
    df = pd.DataFrame(all_recommendations)
    df = df.sort_values("Monthly Savings", ascending=False)

    # Show top 10
    top_df = df.head(10)

    # Format currency columns
    top_df["Monthly Savings"] = top_df["Monthly Savings"].apply(lambda x: f"${x:,.2f}")
    top_df["Annual Savings"] = top_df["Annual Savings"].apply(lambda x: f"${x:,.2f}")

    # Display table
    st.dataframe(
        top_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Type": st.column_config.TextColumn("Type", width="small"),
            "Table": st.column_config.TextColumn("Table Name", width="medium"),
            "Region": st.column_config.TextColumn("Region", width="small"),
            "Current": st.column_config.TextColumn("Current", width="small"),
            "Recommended": st.column_config.TextColumn("Recommended", width="small"),
            "Monthly Savings": st.column_config.TextColumn("Monthly $", width="small"),
            "Annual Savings": st.column_config.TextColumn("Annual $", width="small"),
        },
    )

    # Export button
    if len(df) > 0:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download All Recommendations (CSV)",
            data=csv,
            file_name="dynamodb_recommendations.csv",
            mime="text/csv",
        )
