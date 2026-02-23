
# Analysis Deep Dive

← [Documentation Index](README.md) | [Main README](../README.md)

---

## Capacity Mode Analysis

**Objective:** Determine if On-Demand or Provisioned with autoscaling is more cost-effective.

**Process:**
1. **Collect** 14 days of `ConsumedReadCapacityUnits` and `ConsumedWriteCapacityUnits` metrics
2. **Simulate** autoscaling behavior:
   - Target utilization: 70%
   - Scale-out: When >70% for 2 consecutive minutes
   - Scale-in: When <50% for 15 consecutive minutes
   - Min capacity: 1, Max capacity: 40,000
3. **Calculate** costs:
   - On-Demand: $1.25/million write requests, $0.25/million read requests
   - Provisioned: $0.00065/hour per WCU, $0.00013/hour per RCU
   - Includes free tier: 25 WCU, 25 RCU if eligible
4. **Recommend** mode with lower monthly cost

**Best For:**
- Tables with unpredictable traffic patterns → On-Demand
- Tables with steady, predictable traffic → Provisioned
- Tables with high utilization (>70%) → Provisioned

**Example Savings:**
```
Table: prod-users-table
Current: Provisioned (500 RCU, 500 WCU) → $280/month
Recommended: On-Demand → $95/month
Monthly Savings: $185 (66% reduction)
Reason: Highly variable traffic, low average utilization
```

### Table Class Analysis

**Objective:** Determine if Standard-IA table class provides cost savings.

**Process:**
1. **Collect** CUR data with resource IDs (requires `INCLUDE_RESOURCES`)
2. **Calculate** storage-to-throughput cost ratio:
   ```
   Ratio = Monthly Storage Cost / Monthly Throughput Cost
   ```
3. **Compare** to breakeven ratio (2.67:1):
   - Standard-IA saves 50% on storage
   - Standard-IA costs 50% more on throughput
   - Breakeven when storage is 2.67x throughput costs
4. **Recommend** Standard-IA if ratio > 2.67

**Best For:**
- Tables with high storage, low throughput
- Infrequently accessed data (archives, logs)
- Tables with read-heavy workloads

**Example Savings:**
```
Table: prod-audit-logs
Current: Standard → $450/month
  Storage: $400 (10 TB)
  Throughput: $50
Ratio: 8.0:1 (well above 2.67 breakeven)

Recommended: Standard-IA → $300/month
  Storage: $200 (50% savings)
  Throughput: $75 (50% increase)
Monthly Savings: $150 (33% reduction)
```

### Utilization Analysis

**Objective:** Identify over-provisioned capacity in Provisioned mode tables.

**Process:**
1. **Analyze** average utilization over 7-14 days
2. **Identify** resources with <30% average utilization
3. **Recommend** capacity = 1.25x peak observed consumption (80% target)
4. **Calculate** savings from reduced capacity

**Best For:**
- Right-sizing after traffic decrease
- Cleaning up over-provisioned capacity
- Post-migration optimization

**Example Savings:**
```
Table: staging-test-data
Current: Provisioned (1000 RCU, 1000 WCU) → $560/month
Average Utilization: 15% read, 8% write

Recommended: Provisioned (200 RCU, 150 WCU) → $100/month
Monthly Savings: $460 (82% reduction)
Reason: Significantly over-provisioned for actual load
```
