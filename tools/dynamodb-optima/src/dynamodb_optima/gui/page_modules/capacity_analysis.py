"""
Capacity Mode Analysis page - ON_DEMAND vs PROVISIONED recommendations.
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


def render_capacity_analysis(connection, filters: RecommendationFilter):
    """Render capacity mode analysis page."""
    st.title("⚡ Capacity Mode Analysis")
    st.markdown(
        "Recommendations for switching between ON_DEMAND and PROVISIONED billing modes"
    )

    try:
        # Get recommendations from all analyzers for deduplication
        capacity_recs = get_capacity_recommendations(connection, filters)
        table_class_recs = get_table_class_recommendations(connection, filters)
        utilization_recs = get_utilization_recommendations(connection, filters)
        
        # Deduplicate: when same savings, prioritize Capacity > Utilization > Table Class
        capacity_recs, _, _ = deduplicate_recommendations(
            capacity_recs, table_class_recs, utilization_recs
        )
        
        # Use only capacity recommendations for this page
        recommendations = capacity_recs

        if not recommendations:
            st.info(
                "No capacity mode recommendations found matching the current filters.\n\n"
                "Run `dynamodb-optima analyze-capacity` to generate recommendations."
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
            to_on_demand = sum(
                1 for r in recommendations if r.recommended_billing_mode == "PAY_PER_REQUEST"
            )
            to_provisioned = len(recommendations) - to_on_demand
            st.metric(
                label="🔄 Mode Changes",
                value=f"{to_on_demand} → On-Demand",
                delta=f"{to_provisioned} → Provisioned",
            )

        st.markdown("---")

        # Visualization
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Top 10 Tables by Savings")
            fig = create_top_tables_chart(recommendations)
            st.plotly_chart(fig, width='stretch')

        with col2:
            st.subheader("Recommendation Distribution")
            fig = create_mode_distribution_chart(recommendations)
            st.plotly_chart(fig, width='stretch')

        st.markdown("---")

        # Detailed recommendations
        st.subheader("📝 Detailed Recommendations")

        # Group by table
        for rec in recommendations:
            with st.expander(
                f"**{rec.table_name}** ({rec.region}) [Account: {rec.account_id}] - Save ${rec.monthly_savings_usd:,.2f}/month"
            ):
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("### Current Configuration")
                    st.write(f"**Billing Mode:** {rec.current_billing_mode}")
                    st.write(f"**Monthly Cost:** ${rec.current_monthly_cost_usd:,.2f}")
                    if rec.avg_provisioned_rcu is not None:
                        st.write(f"**Avg RCU:** {rec.avg_provisioned_rcu}")
                        st.write(f"**Avg WCU:** {rec.avg_provisioned_wcu}")
                    if rec.avg_read_utilization is not None:
                        st.write(
                            f"**Read Utilization:** {rec.avg_read_utilization:.1f}%"
                        )
                        st.write(
                            f"**Write Utilization:** {rec.avg_write_utilization:.1f}%"
                        )

                with col2:
                    st.markdown("### Recommended Configuration")
                    st.write(f"**Billing Mode:** {rec.recommended_billing_mode}")
                    st.write(
                        f"**Projected Monthly Cost:** ${rec.projected_monthly_cost_usd:,.2f}"
                    )
                    st.write(
                        f"**Monthly Savings:** ${rec.monthly_savings_usd:,.2f} ({rec.savings_percentage:.1f}%)"
                    )
                    st.write(f"**Annual Savings:** ${rec.annual_savings_usd:,.2f}")
                    st.write(f"**Confidence:** {rec.confidence_score:.0f}%")
                    st.write(f"**Risk Level:** {rec.risk_level.upper()}")

                st.markdown("---")
                st.markdown("**💡 Recommendation Reason:**")
                st.info(rec.recommendation_reason)

                st.markdown("**📅 Analysis Details:**")
                st.write(f"Analysis Period: {rec.analysis_days} days")
                st.write(f"Account: {rec.account_id}")
                st.write(f"Generated: {rec.created_at.strftime('%Y-%m-%d %H:%M')}")

        st.markdown("---")

        # Export data
        st.subheader("📥 Export Data")
        df = create_recommendations_dataframe(recommendations)
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download Capacity Recommendations (CSV)",
            data=csv,
            file_name="capacity_mode_recommendations.csv",
            mime="text/csv",
        )

    except Exception as e:
        st.error(f"Error loading capacity recommendations: {e}")
        import traceback

        st.code(traceback.format_exc())


def create_top_tables_chart(recommendations):
    """Create bar chart of top 10 tables by savings."""
    # Sort by savings and take top 10, then reverse for display (highest at top)
    sorted_recs = sorted(
        recommendations, key=lambda x: x.monthly_savings_usd, reverse=True
    )[:10]
    
    # Reverse the list so highest savings appears at the top of the chart
    sorted_recs = list(reversed(sorted_recs))

    tables = [rec.table_name[:30] for rec in sorted_recs]  # Truncate long names
    savings = [rec.monthly_savings_usd for rec in sorted_recs]

    fig = go.Figure(
        data=[
            go.Bar(
                y=tables,
                x=savings,
                orientation="h",
                marker=dict(color="#FF6B6B"),
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


def create_mode_distribution_chart(recommendations):
    """Create pie chart showing distribution of mode changes."""
    # Database stores as PAY_PER_REQUEST not ON_DEMAND
    to_on_demand = sum(
        1 for r in recommendations if r.recommended_billing_mode == "PAY_PER_REQUEST"
    )
    to_provisioned = sum(
        1 for r in recommendations if r.recommended_billing_mode == "PROVISIONED"
    )

    fig = go.Figure(
        data=[
            go.Pie(
                labels=["To ON_DEMAND", "To PROVISIONED"],
                values=[to_on_demand, to_provisioned],
                hole=0.3,
                marker=dict(colors=["#4ECDC4", "#FF6B6B"]),
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


def create_recommendations_dataframe(recommendations):
    """Convert recommendations to pandas DataFrame for export."""
    data = []
    for rec in recommendations:
        data.append(
            {
                "Table Name": rec.table_name,
                "Region": rec.region,
                "Account ID": rec.account_id,
                "Current Mode": rec.current_billing_mode,
                "Recommended Mode": rec.recommended_billing_mode,
                "Current Monthly Cost": rec.current_monthly_cost_usd,
                "Projected Monthly Cost": rec.projected_monthly_cost_usd,
                "Monthly Savings": rec.monthly_savings_usd,
                "Annual Savings": rec.annual_savings_usd,
                "Savings %": rec.savings_percentage,
                "Confidence Score": rec.confidence_score,
                "Risk Level": rec.risk_level,
                "Analysis Days": rec.analysis_days,
                "Avg RCU": rec.avg_provisioned_rcu,
                "Avg WCU": rec.avg_provisioned_wcu,
                "Read Utilization %": rec.avg_read_utilization,
                "Write Utilization %": rec.avg_write_utilization,
                "Recommendation Reason": rec.recommendation_reason,
                "Created At": rec.created_at,
                "Status": rec.status,
            }
        )
    return pd.DataFrame(data)
