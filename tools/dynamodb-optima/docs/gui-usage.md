# GUI Usage

← [Documentation Index](README.md) | [Main README](../README.md)

---

The Streamlit dashboard provides an interactive interface for exploring DynamoDB cost optimization recommendations across multiple analysis types.

## Getting Started

### Prerequisites

Before launching the GUI, ensure you have run the analysis commands:

```bash
# 1. Discover tables
dynamodb-optima discover --regions us-east-1,us-west-2

# 2. Collect metrics
dynamodb-optima collect --days 14

# 3. Run analyses
dynamodb-optima analyze-capacity
dynamodb-optima analyze-table-class  # requires CUR data
dynamodb-optima analyze-utilization
```

### Launch GUI

```bash
# Default port (8501)
dynamodb-optima gui

# Custom port
dynamodb-optima gui --port 8501

# Open browser to http://localhost:8501
```

### Recommendation Deduplication

**Important:** The GUI automatically deduplicates recommendations when multiple analyzers make the same recommendation for the same table with identical savings (when dollar amounts match exactly).

**Priority System:**
1. **Capacity Mode** (highest priority) - Billing model changes
2. **Utilization** - Optimization within current billing model  
3. **Table Class** (lowest priority) - Storage optimization

**Example:** If both Capacity Mode and Utilization analyzers recommend changes saving $100.00/month for the same table, only the Capacity Mode recommendation is shown.

**Applied:** Deduplication is consistent across all views (Dashboard, analysis pages, CSV exports) to prevent double-counting savings.

**Implementation:** See [`src/dynamodb_optima/gui/database.py`](../src/dynamodb_optima/gui/database.py) - `deduplicate_recommendations()` function

---

## Dashboard Page

The main dashboard provides a high-level overview of all cost optimization opportunities.

### Summary Metrics (4 Cards)

- **💰 Total Monthly Savings** - Sum of all recommendations with annual projection
- **📋 Total Recommendations** - Count with "need action" delta
- **🗂️ Tables Analyzed** - Number of unique tables with recommendations
- **✅ Optimization Rate** - Percentage of tables already optimized

### Visualizations

**Savings by Category (Pie Chart):**
- Distribution of monthly savings across Capacity Mode, Table Class, and Utilization
- Hover for exact amounts

**Recommendations by Type (Bar Chart):**
- Count of recommendations per analysis type
- Shows where most opportunities exist

### Top Savings Opportunities

Table showing top 10 tables by monthly savings across all recommendation types:

| Type | Account | Table | Region | Current | Recommended | Monthly $ | Annual $ |
|------|---------|-------|--------|---------|-------------|-----------|----------|
| Capacity Mode | 123456 | prod-users | us-east-1 | PROVISIONED | ON_DEMAND | $150.00 | $1,800.00 |

**Features:**
- Sorted by monthly savings (highest first)
- Includes all recommendation types (after deduplication)
- CSV export button: "📥 Download All Recommendations (CSV)"

---

## Filters (Sidebar)

### Available Filters

**Minimum Monthly Savings ($)**
- Numeric input, default: $0
- Shows only recommendations above this threshold
- Example: Set to $100 to see only high-impact opportunities

**Region**
- Dropdown: "All Regions" or specific region
- Options: All available regions from discovered tables
- Example: us-east-1, us-west-2, eu-west-1

**Account**
- Dropdown: "All Accounts" or specific account ID
- Useful for multi-account Organizations deployments
- Shows account IDs from discovered tables

**Table Name (regex)**
- Text input with **RE2 regex support**
- Case-sensitive by default
- Empty = show all tables

**Regex Examples:**
- `^prod-` - Tables starting with "prod-"
- `test` - Tables containing "test" anywhere
- `^(dev|staging)-` - Tables starting with "dev-" or "staging-"
- `(?i)PROD` - Case-insensitive match for "prod"
- `^myapp-[0-9]+$` - Tables like myapp-1, myapp-123

> **Invalid Pattern Error:** If you enter an invalid regex, the GUI shows an error with RE2 syntax examples and a link to [RE2 documentation](https://github.com/google/re2/wiki/Syntax).

**Status**
- Dropdown: All Statuses, pending, accepted, rejected, implemented
- Track recommendation lifecycle
- Default: All Statuses

### Refresh

- **Last refreshed timestamp** shown at bottom of sidebar
- **🔄 Refresh Data** button to reload from database

---

## Analysis Pages

Each analysis type has a dedicated page with detailed recommendations.

### Page Structure (All Pages)

**Summary Metrics (4 Cards)**
- Specific to each analysis type
- Shows total savings, recommendation count, averages, and distribution

**Visualizations (2 Charts)**
- **Left:** Top 10 tables by savings (horizontal bar chart)
- **Right:** Recommendation distribution (pie chart or relevant metric)

**Detailed Recommendations (Expandable)**

Each table has an expandable section showing:

**Header:** `Table Name (Region) [Account: ID] - Save $X/month`

**Expanded Content:**
- **Left Column:** Current configuration, costs, utilization
- **Right Column:** Recommended configuration, projected costs, savings, confidence, risk level
- **Recommendation Reason:** Detailed explanation of why the change is recommended
- **Analysis Details:** Period analyzed, account ID, timestamp

**CSV Export**
- Download button for current page's recommendations
- Includes all detailed fields in tabular format

### Capacity Mode Analysis Page

**Focus:** Switching between ON_DEMAND and PROVISIONED billing modes

**Metrics:**
- Monthly savings total
- Number of recommendations
- Average savings percentage
- Mode changes breakdown (X → On-Demand, Y → Provisioned)

**Charts:**
- Top 10 tables by monthly savings
- Mode distribution (to On-Demand vs to Provisioned)

**Expandable Details Include:**
- Current/projected monthly costs
- Average RCU/WCU (if Provisioned)
- Read/write utilization percentages
- Autoscaling simulation results

### Table Class Analysis Page

**Focus:** Switching between Standard and Standard-IA table classes

**Metrics:**
- Monthly savings total
- Number of recommendations
- Average savings percentage
- Storage-to-throughput ratio statistics

**Charts:**
- Top 10 tables by monthly savings
- Distribution of recommendations

**Expandable Details Include:**
- Current/projected monthly costs broken down by storage and throughput
- Storage-to-throughput ratio
- Breakeven analysis (threshold: 2.67:1)
- Reserved capacity indicator

### Utilization Analysis Page

**Focus:** Identifying over-provisioned capacity (Provisioned mode tables)

**Metrics:**
- Monthly savings total
- Number of recommendations
- Average utilization percentage
- Resource type breakdown (tables vs GSIs)

**Charts:**
- Top 10 tables by monthly savings
- Utilization distribution

**Expandable Details Include:**
- Current provisioned capacity (RCU/WCU)
- Recommended capacity (typically 80% of peak observed)
- Average, max, and p99 utilization percentages
- Resource type (table vs GSI)

---

## Navigation

Use the sidebar radio buttons to switch between pages:

1. **Dashboard** - Summary overview
2. **Capacity Mode Analysis** - ON_DEMAND vs PROVISIONED
3. **Table Class Analysis** - Standard vs Standard-IA
4. **Utilization Analysis** - Over-provisioning detection

All filters in the sidebar apply globally across all pages.

---

## Tips

- **Start with Dashboard** to see overall savings potential
- **Use filters** to focus on specific regions, accounts, or high-impact opportunities
- **Regex filtering** is powerful for analyzing tables by naming convention
- **Export CSVs** for reporting or sharing with teams
- **Check confidence scores** and risk levels before implementing recommendations
- **Deduplication** ensures clean data - each table appears once per savings amount

---

## Troubleshooting

**No data showing:**
- Ensure discovery and collection commands have been run
- Check that analysis commands completed successfully
- Verify database file exists: `data/dynamodb_optima.db` (or custom `--project-root`)

**Invalid regex error:**
- Check RE2 syntax (not PCRE/Python regex)
- Test pattern at [regex101.com](https://regex101.com) (select "Golang" flavor for RE2)
- See examples in filter help text

**Performance issues:**
- Large datasets (>1000 tables) may take a few seconds to load
- If there's no recommendations, the dashboard webpage will spin for minutes before displaying no recommendations
- Use filters to reduce displayed data
- Consider analyzing fewer days of metrics

---

## Related Documentation

- [Command Reference](command-reference.md) - CLI commands for discovery and analysis
- [Architecture](architecture.md) - Database schema and system design
- [Analysis Deep Dive](analysis-deep-dive.md) - How each analysis type works
