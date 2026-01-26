"""
Utilization Analysis page - Over/under-provisioning recommendations.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from dynamodb_optima.gui.models import RecommendationFilter
from dynamodb_optima.gui.database import get_utilization_recommendations


def render_utilization_analysis(connection, filters: RecommendationFilter):
    """Render utilization analysis page."""
    st.title("📊 Utilization Analysis")
    st.markdown(
        "Recommendations for optimizing provisioned capacity based on usage patterns"
    )

    try:
        # Get recommendations
        recommendations = get_utilization_recommendations(connection, filters)

        if not recommendations:
            st.info(
                "No utilization recommendations found matching the current filters.\n\n"
                "Run `dynamodb-optima analyze-utilization` to generate recommendations."
            )
            return

        # Calculate summary metrics
        total_savings = sum(rec.monthly_savings_usd for rec in recommendations)
        total_annual = sum(rec.annual_savings_usd for rec in recommendations)
        avg_read_util = (
            sum(rec.avg_read_utilization for rec in recommendations)
            / len(recommendations)
            if recommendations
            else 0
        )
        avg_write_util = (
            sum(rec.avg_write_utilization for rec in recommendations)
            / len(recommendations)
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
            st.metric(
                label="📖 Avg Read Util",
                value=f"{avg_read_util:.1f}%",
                help="Average read capacity utilization",
            )

        with col4:
            st.metric(
                label="✍️ Avg Write Util",
                value=f"{avg_write_util:.1f}%",
                help="Average write capacity utilization",
            )

        st.markdown("---")

        # Visualization
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Top 10 Resources by Savings")
            fig = create_top_resources_chart(recommendations)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Resource Type Distribution")
            fig = create_resource_type_chart(recommendations)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Group recommendations by table
        st.subheader("📝 Recommendations by Table")

        tables = {}
        for rec in recommendations:
            if rec.table_name not in tables:
                tables[rec.table_name] = []
            tables[rec.table_name].append(rec)

        # Sort tables by total savings
        sorted_tables = sorted(
            tables.items(),
            key=lambda x: sum(r.monthly_savings_usd for r in x[1]),
            reverse=True,
        )

        for table_name, table_recs in sorted_tables:
            total_table_savings = sum(r.monthly_savings_usd for r in table_recs)

            with st.expander(
                f"**{table_name}** - {len(table_recs)} recommendation(s) - Save ${total_table_savings:,.2f}/month"
            ):
                for rec in table_recs:
                    st.markdown(f"### {rec.resource_type.upper()}: {rec.resource_name}")

                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown("#### Current Provisioning")
                        st.write(f"**Region:** {rec.region}")
                        st.write(f"**RCU:** {rec.current_provisioned_rcu:,}")
                        st.write(f"**WCU:** {rec.current_provisioned_wcu:,}")
                        st.write(
                            f"**Monthly Cost:** ${rec.current_monthly_cost_usd:,.2f}"
                        )
                        st.write(
                            f"**Avg Read Utilization:** {rec.avg_read_utilization:.1f}%"
                        )
                        st.write(
                            f"**Avg Write Utilization:** {rec.avg_write_utilization:.1f}%"
                        )

                    with col2:
                        st.markdown("#### Recommended Provisioning")
                        st.write(f"**RCU:** {rec.recommended_provisioned_rcu:,}")
                        st.write(f"**WCU:** {rec.recommended_provisioned_wcu:,}")
                        st.write(
                            f"**Projected Cost:** ${rec.projected_monthly_cost_usd:,.2f}"
                        )
                        st.write(
                            f"**Monthly Savings:** ${rec.monthly_savings_usd:,.2f} ({rec.savings_percentage:.1f}%)"
                        )
                        st.write(f"**Confidence:** {rec.confidence_score:.0f}%")
                        st.write(f"**Risk Level:** {rec.risk_level.upper()}")

                    # Utilization visualization
                    fig = create_utilization_comparison_chart(rec)
                    st.plotly_chart(fig, use_container_width=True, key=f"util_chart_{rec.resource_name}_{rec.region}")

                    st.markdown("**💡 Recommendation Reason:**")
                    st.info(rec.recommendation_reason)

                    st.markdown("---")

        st.markdown("---")

        # Export data
        st.subheader("📥 Export Data")
        df = create_recommendations_dataframe(recommendations)
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download Utilization Recommendations (CSV)",
            data=csv,
            file_name="utilization_recommendations.csv",
            mime="text/csv",
        )

    except Exception as e:
        st.error(f"Error loading utilization recommendations: {e}")
        import traceback

        st.code(traceback.format_exc())


def create_top_resources_chart(recommendations):
    """Create bar chart of top 10 resources by savings."""
    sorted_recs = sorted(
        recommendations, key=lambda x: x.monthly_savings_usd, reverse=True
    )[:10]

    labels = [
        f"{rec.table_name[:20]}:{rec.resource_name[:10]}" for rec in sorted_recs
    ]
    savings = [rec.monthly_savings_usd for rec in sorted_recs]

    fig = go.Figure(
        data=[
            go.Bar(
                y=labels,
                x=savings,
                orientation="h",
                marker=dict(color="#45B7D1"),
                text=[f"${s:,.2f}" for s in savings],
                textposition="auto",
            )
        ]
    )

    fig.update_layout(
        height=400,
        margin=dict(l=20, r=20, t=20, b=20),
        xaxis_title="Monthly Savings ($)",
        yaxis_title="Resource",
        showlegend=False,
    )

    return fig


def create_resource_type_chart(recommendations):
    """Create pie chart showing distribution by resource type."""
    table_count = sum(1 for r in recommendations if r.resource_type.strip().upper() == "TABLE")
    gsi_count = sum(1 for r in recommendations if r.resource_type.strip().upper() == "GSI")

    fig = go.Figure(
        data=[
            go.Pie(
                labels=["Tables", "GSIs"],
                values=[table_count, gsi_count],
                hole=0.3,
                marker=dict(colors=["#45B7D1", "#4ECDC4"]),
            )
        ]
    )

    fig.update_layout(
        height=400,
        margin=dict(l=20, r=20, t=20, b=20),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
    )

    return fig


def create_utilization_comparison_chart(rec):
    """Create comparison chart for current vs recommended capacity and utilization."""
    fig = go.Figure()

    # Current vs Recommended capacity
    fig.add_trace(
        go.Bar(
            name="Current RCU",
            x=["Read Capacity"],
            y=[rec.current_provisioned_rcu],
            marker=dict(color="#FF6B6B"),
        )
    )

    fig.add_trace(
        go.Bar(
            name="Recommended RCU",
            x=["Read Capacity"],
            y=[rec.recommended_provisioned_rcu],
            marker=dict(color="#4ECDC4"),
        )
    )

    fig.add_trace(
        go.Bar(
            name="Current WCU",
            x=["Write Capacity"],
            y=[rec.current_provisioned_wcu],
            marker=dict(color="#FF6B6B"),
        )
    )

    fig.add_trace(
        go.Bar(
            name="Recommended WCU",
            x=["Write Capacity"],
            y=[rec.recommended_provisioned_wcu],
            marker=dict(color="#4ECDC4"),
        )
    )

    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=40, b=20),
        title="Current vs Recommended Capacity",
        yaxis_title="Capacity Units",
        barmode="group",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
    )

    return fig


def create_recommendations_dataframe(recommendations):
    """Convert recommendations to pandas DataFrame for export."""
    data = []
    for rec in recommendations:
        data.append(
            {
                "Table Name": rec.table_name,
                "Resource Type": rec.resource_type,
                "Resource Name": rec.resource_name,
                "Region": rec.region,
                "Account ID": rec.account_id,
                "Current RCU": rec.current_provisioned_rcu,
                "Current WCU": rec.current_provisioned_wcu,
                "Recommended RCU": rec.recommended_provisioned_rcu,
                "Recommended WCU": rec.recommended_provisioned_wcu,
                "Current Monthly Cost": rec.current_monthly_cost_usd,
                "Projected Monthly Cost": rec.projected_monthly_cost_usd,
                "Monthly Savings": rec.monthly_savings_usd,
                "Annual Savings": rec.annual_savings_usd,
                "Savings %": rec.savings_percentage,
                "Avg Read Utilization %": rec.avg_read_utilization,
                "Avg Write Utilization %": rec.avg_write_utilization,
                "Max Read Utilization %": rec.max_read_utilization,
                "Max Write Utilization %": rec.max_write_utilization,
                "Confidence Score": rec.confidence_score,
                "Risk Level": rec.risk_level,
                "Analysis Days": rec.analysis_days,
                "Recommendation Reason": rec.recommendation_reason,
                "Created At": rec.created_at,
                "Status": rec.status,
            }
        )
    return pd.DataFrame(data)
