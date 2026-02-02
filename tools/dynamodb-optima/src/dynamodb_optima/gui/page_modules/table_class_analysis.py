"""
Table Class Analysis page - STANDARD vs STANDARD_IA recommendations.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from dynamodb_optima.gui.models import RecommendationFilter
from dynamodb_optima.gui.database import (
    get_capacity_recommendations,
    get_table_class_recommendations,
    get_utilization_recommendations,
    deduplicate_recommendations,
)


def render_table_class_analysis(connection, filters: RecommendationFilter):
    """Render table class analysis page."""
    st.title("📦 Table Class Analysis")
    st.markdown(
        "Recommendations for switching between STANDARD and STANDARD_IA table classes"
    )

    try:
        # Get recommendations from all analyzers for deduplication
        capacity_recs = get_capacity_recommendations(connection, filters)
        table_class_recs = get_table_class_recommendations(connection, filters)
        utilization_recs = get_utilization_recommendations(connection, filters)
        
        # Deduplicate: when same savings, prioritize Capacity > Utilization > Table Class
        _, table_class_recs, _ = deduplicate_recommendations(
            capacity_recs, table_class_recs, utilization_recs
        )
        
        # Use only table class recommendations for this page
        recommendations = table_class_recs

        if not recommendations:
            st.info(
                "No table class recommendations found matching the current filters.\n\n"
                "Run `dynamodb-optima analyze-table-class` to generate recommendations."
            )
            return

        # Calculate summary metrics
        total_savings = sum(rec.monthly_savings_usd for rec in recommendations)
        total_annual = sum(rec.annual_savings_usd for rec in recommendations)
        avg_savings_pct = (
            sum(rec.savings_percentage for rec in recommendations) / len(recommendations)
            if recommendations
            else 0
        )

        # Display summary metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                label="💰 Monthly Savings",
                value=f"${total_savings:,.2f}",
                delta=f"${total_annual:,.2f}/year",
            )

        with col2:
            st.metric(label="📋 Recommendations", value=len(recommendations))

        with col3:
            st.metric(label="📊 Avg Savings", value=f"{avg_savings_pct:.1f}%")

        with col4:
            to_ia = sum(
                1
                for r in recommendations
                if r.recommended_table_class == "STANDARD_IA"
            )
            to_standard = len(recommendations) - to_ia
            st.metric(
                label="🔄 Class Changes",
                value=f"{to_ia} → IA",
                delta=f"{to_standard} → Standard",
            )

        st.markdown("---")

        # Visualization
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Top 10 Tables by Savings")
            fig = create_top_tables_chart(recommendations)
            st.plotly_chart(fig, width='stretch')

        with col2:
            st.subheader("Cost Breakdown")
            fig = create_cost_breakdown_chart(recommendations)
            st.plotly_chart(fig, width='stretch')

        st.markdown("---")

        # Detailed recommendations table
        st.subheader("📝 Detailed Recommendations")
        df = create_recommendations_dataframe(recommendations)

        # Format for display
        display_df = df.copy()
        display_df["Current Monthly Cost"] = display_df["Current Monthly Cost"].apply(
            lambda x: f"${x:,.2f}"
        )
        display_df["Projected Monthly Cost"] = display_df[
            "Projected Monthly Cost"
        ].apply(lambda x: f"${x:,.2f}")
        display_df["Monthly Savings"] = display_df["Monthly Savings"].apply(
            lambda x: f"${x:,.2f}"
        )
        display_df["Annual Savings"] = display_df["Annual Savings"].apply(
            lambda x: f"${x:,.2f}"
        )
        display_df["Savings %"] = display_df["Savings %"].apply(lambda x: f"{x:.1f}%")
        display_df["Avg Table Size (GB)"] = display_df["Avg Table Size (GB)"].apply(
            lambda x: f"{x:,.1f}"
        )

        st.dataframe(
            display_df[
                [
                    "Table Name",
                    "Region",
                    "Current Class",
                    "Recommended Class",
                    "Monthly Savings",
                    "Annual Savings",
                    "Savings %",
                    "Avg Table Size (GB)",
                ]
            ],
            width='stretch',
            hide_index=True,
        )

        # Expandable details for each recommendation
        st.markdown("---")
        st.subheader("📊 Detailed Analysis")

        for rec in recommendations:
            with st.expander(
                f"**{rec.table_name}** ({rec.region}) [Account: {rec.account_id}] - Save ${rec.monthly_savings_usd:,.2f}/month"
            ):
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("### Current Configuration")
                    st.write(f"**Table Class:** {rec.current_table_class}")
                    st.write(
                        f"**Total Monthly Cost:** ${rec.current_monthly_total_cost_usd:,.2f}"
                    )
                    st.write(
                        f"**Storage Cost:** ${rec.current_monthly_storage_cost_usd:,.2f}"
                    )
                    st.write(
                        f"**Throughput Cost:** ${rec.current_monthly_throughput_cost_usd:,.2f}"
                    )
                    st.write(f"**Avg Table Size:** {rec.avg_table_size_gb:,.1f} GB")

                with col2:
                    st.markdown("### Recommended Configuration")
                    st.write(f"**Table Class:** {rec.recommended_table_class}")
                    st.write(
                        f"**Projected Monthly Cost:** ${rec.projected_monthly_total_cost_usd:,.2f}"
                    )
                    st.write(
                        f"**Projected Storage:** ${rec.projected_monthly_storage_cost_usd:,.2f}"
                    )
                    st.write(
                        f"**Projected Throughput:** ${rec.projected_monthly_throughput_cost_usd:,.2f}"
                    )
                    st.write(
                        f"**Monthly Savings:** ${rec.monthly_savings_usd:,.2f} ({rec.savings_percentage:.1f}%)"
                    )

                st.markdown("---")
                st.markdown("**📊 Breakeven Analysis:**")
                st.write(
                    f"Storage-to-Throughput Ratio: {rec.storage_to_throughput_ratio:.2f}"
                )
                st.write(f"Breakeven Ratio: {rec.breakeven_ratio:.2f}")
                st.write(
                    f"Above Breakeven: {'✅ Yes' if rec.is_above_breakeven else '❌ No'}"
                )

                st.markdown("**💡 Recommendation Reason:**")
                st.info(rec.recommendation_reason)

                st.markdown("**📅 Analysis Details:**")
                st.write(f"Analysis Period: {rec.analysis_months} months")
                st.write(f"Account: {rec.account_id}")
                st.write(f"Confidence: {rec.confidence_score:.0f}%")
                st.write(f"Generated: {rec.created_at.strftime('%Y-%m-%d %H:%M')}")

        st.markdown("---")

        # Export data
        st.subheader("📥 Export Data")
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download Table Class Recommendations (CSV)",
            data=csv,
            file_name="table_class_recommendations.csv",
            mime="text/csv",
        )

    except Exception as e:
        st.error(f"Error loading table class recommendations: {e}")
        import traceback

        st.code(traceback.format_exc())


def create_top_tables_chart(recommendations):
    """Create bar chart of top 10 tables by savings."""
    sorted_recs = sorted(
        recommendations, key=lambda x: x.monthly_savings_usd, reverse=True
    )[:10]

    tables = [rec.table_name[:30] for rec in sorted_recs]
    savings = [rec.monthly_savings_usd for rec in sorted_recs]

    fig = go.Figure(
        data=[
            go.Bar(
                y=tables,
                x=savings,
                orientation="h",
                marker=dict(color="#4ECDC4"),
                text=[f"${s:,.2f}" for s in savings],
                textposition="auto",
            )
        ]
    )

    fig.update_layout(
        height=400,
        margin=dict(l=20, r=20, t=20, b=20),
        xaxis_title="Monthly Savings ($)",
        yaxis_title="Table Name",
        showlegend=False,
    )

    return fig


def create_cost_breakdown_chart(recommendations):
    """Create stacked bar chart showing cost breakdown."""
    # Aggregate current vs projected costs
    current_storage = sum(rec.current_monthly_storage_cost_usd for rec in recommendations)
    current_throughput = sum(
        rec.current_monthly_throughput_cost_usd for rec in recommendations
    )
    projected_storage = sum(
        rec.projected_monthly_storage_cost_usd for rec in recommendations
    )
    projected_throughput = sum(
        rec.projected_monthly_throughput_cost_usd for rec in recommendations
    )

    fig = go.Figure(
        data=[
            go.Bar(
                name="Storage",
                x=["Current", "Projected"],
                y=[current_storage, projected_storage],
                marker=dict(color="#FF6B6B"),
            ),
            go.Bar(
                name="Throughput",
                x=["Current", "Projected"],
                y=[current_throughput, projected_throughput],
                marker=dict(color="#4ECDC4"),
            ),
        ]
    )

    fig.update_layout(
        barmode="stack",
        height=400,
        margin=dict(l=20, r=20, t=20, b=20),
        yaxis_title="Monthly Cost ($)",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
    )

    return fig


def create_recommendations_dataframe(recommendations):
    """Convert recommendations to pandas DataFrame for export."""
    data = []
    for rec in recommendations:
        data.append(
            {
                "Table Name": rec.table_name,
                "Region": rec.region,
                "Account ID": rec.account_id,
                "Current Class": rec.current_table_class,
                "Recommended Class": rec.recommended_table_class,
                "Current Monthly Cost": rec.current_monthly_total_cost_usd,
                "Current Storage Cost": rec.current_monthly_storage_cost_usd,
                "Current Throughput Cost": rec.current_monthly_throughput_cost_usd,
                "Projected Monthly Cost": rec.projected_monthly_total_cost_usd,
                "Projected Storage Cost": rec.projected_monthly_storage_cost_usd,
                "Projected Throughput Cost": rec.projected_monthly_throughput_cost_usd,
                "Monthly Savings": rec.monthly_savings_usd,
                "Annual Savings": rec.annual_savings_usd,
                "Savings %": rec.savings_percentage,
                "Avg Table Size (GB)": rec.avg_table_size_gb,
                "Storage-to-Throughput Ratio": rec.storage_to_throughput_ratio,
                "Breakeven Ratio": rec.breakeven_ratio,
                "Is Above Breakeven": rec.is_above_breakeven,
                "Confidence Score": rec.confidence_score,
                "Analysis Months": rec.analysis_months,
                "Recommendation Reason": rec.recommendation_reason,
                "Created At": rec.created_at,
                "Status": rec.status,
            }
        )
    return pd.DataFrame(data)
