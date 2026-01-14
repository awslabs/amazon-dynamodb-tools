# DAX Cluster Calculator

A command-line tool for sizing AWS DynamoDB Accelerator (DAX) clusters. Automates the manual calculations from the [AWS DAX cluster sizing guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.cluster-sizing.html).

## Overview

The DAX Cluster Calculator helps you determine the appropriate DAX cluster configuration based on your workload characteristics. It calculates:

- **Normalized RPS**: Total units of work required by your DAX cluster.
- **Target RPS**: Actual capacity needed accounting for utilization targets and node failure tolerance
- **Node Type Recommendations**: Optimal instance types and cluster sizes for your workload
- **Monthly Cost Estimates**: Calculates the monthly cost for each recommended configuration
- **Memory Validation**: Ensures your dataset fits in the recommended node type's memory

## Features

- Interactive command-line interface with input validation
- Automatic calculation of Normalized RPS and Target RPS
- Smart node type recommendations sorted by cost-efficiency
- Monthly cost estimates based on AWS us-east-1 pricing
- Memory capacity validation for dataset sizing
- Support for all DAX node types (dax.r5.large through dax.r5.24xlarge)
- Comprehensive test suite with property-based testing

## Installation

### Option 1: Using uv (Recommended)

This project uses `uv` for fast Python package management. If you don't have `uv` installed, follow the [installation guide](https://docs.astral.sh/uv/getting-started/installation/).

```bash
# Clone the repository
git clone https://github.com/awslabs/amazon-dynamodb-tools.git
cd dax_calculator

# Create and activate virtual environment
uv venv
source .venv/bin/activate  # On macOS/Linux
# or
.venv\Scripts\activate  # On Windows

# Install dependencies
uv pip install hypothesis pytest
```

### Option 2: Using pip

```bash
# Clone the repository
git clone https://github.com/awslabs/amazon-dynamodb-tools.git
cd dax_calculator

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On macOS/Linux
# or
.venv\Scripts\activate  # On Windows

# Install dependencies
pip install -r requirements.txt
```

## Usage

Run the calculator:

```bash
uv run python dax_calculator.py
```

You'll be prompted to enter:

1. **Cache hit reads per second**: Number of read requests that find data in cache
2. **Cache miss reads per second**: Number of read requests that require fetching from DynamoDB
3. **Writes per second**: Number of write operations
4. **Item size in KB**: Average size of items (fractional values are rounded up)
5. **Number of nodes**: Cluster size (1, 3, 5, or 11 nodes)
6. **Target utilization percentage**: Desired cluster utilization (1-100%)
7. **Dataset size in GB** (optional): Total size of your dataset for memory validation

### Example Session

```bash
=== DAX Cluster Calculator ===
Please provide the following information about your workload:

Cache hit reads per second: 50000
Cache miss reads per second: 1000
Writes per second: 100
Item size in KB: 2
Number of nodes in cluster: 3
Target utilization percentage (1-100): 80
Dataset size in GB (press Enter to skip): 10

============================================================
Input Summary
============================================================
Cache Hit Reads/sec:      50,000
Cache Miss Reads/sec:     1,000
Writes/sec:               100
Item Size:                2.0 KB
Node Count:               3
Target Utilization:       80.0%
Dataset Size:             10.0 GB

============================================================
Calculation Details
============================================================
Normalized RPS:           135,000

Multipliers:
  Utilization Multiplier: 1.25
  Node Loss Multiplier:   1.50
  Selected Multiplier:    1.50 (Node Loss Tolerance)

Target RPS:               202,500

============================================================
Node Type Recommendations
============================================================

For 3 node(s), the following node types meet your requirements:
(Target RPS: 202,500)

1. dax.r5.large
   Memory:        16 GB
   Capacity:      225,000 RPS
   Monthly Cost:  $369.36

2. dax.r5.xlarge
   Memory:        32 GB
   Capacity:      450,000 RPS
   Monthly Cost:  $738.72

...
```

## Testing

The project includes comprehensive test coverage with both unit tests and property-based tests.

### Run All Tests

```bash
pytest test_unit.py test_properties.py -v
```

### Test Structure

- **Unit Tests** (`test_unit.py`): 31 tests covering specific examples, edge cases, and error conditions
- **Property-Based Tests** (`test_properties.py`): 12 tests validating universal correctness properties using Hypothesis

### Test Coverage

- AWS documentation examples
- Edge cases (single node, zero values, boundary conditions)
- Error conditions (negative inputs, invalid ranges)
- Node recommendations and sorting
- Memory validation
- End-to-end calculation flows

## Project Structure

```
dax_calculator/
├── dax_calculator.py      # Main application
├── test_unit.py           # Unit tests
├── test_properties.py     # Property-based tests
├── README.md              # This file
└── .gitignore             # Git ignore rules
```

## How It Works

### Calculation Formulas

**Normalized RPS:**
```
(ReadRPS_CacheHit × Size) + (ReadRPS_CacheMiss × Size × 10) + (WriteRPS × 25 × Size × NodeCount)
```

**Multipliers:**
- Utilization Multiplier: `100 / TargetUtilization`
- Node Loss Multiplier: `NodeCount / (NodeCount - 1)`

**Target RPS:**
```
Normalized RPS × MAX(Utilization Multiplier, Node Loss Multiplier)
```

### Node Type Selection

The calculator compares your Target RPS against the capacity table for all DAX node types and recommends:

1. All node types that meet or exceed your Target RPS
2. Sorted by memory size (smallest/most cost-effective first)
3. With memory validation if dataset size is provided

## Supported Node Types

| Node Type | Memory | Hourly Cost | Capacity (1/3/5/11 nodes) |
|-----------|--------|-------------|---------------------------|
| dax.r5.large | 16 GB | $0.171 | 75K / 225K / 375K / 825K |
| dax.r5.xlarge | 32 GB | $0.342 | 150K / 450K / 750K / 1.65M |
| dax.r5.2xlarge | 64 GB | $0.684 | 300K / 900K / 1.5M / 3.3M |
| dax.r5.4xlarge | 128 GB | $1.368 | 600K / 1.8M / 3M / 6.6M |
| dax.r5.8xlarge | 256 GB | $2.736 | 1M / 3M / 5M / 11M |
| dax.r5.12xlarge | 384 GB | $4.104 | 1M / 3M / 5M / 11M |
| dax.r5.16xlarge | 512 GB | $5.472 | 1M / 3M / 5M / 11M |
| dax.r5.24xlarge | 768 GB | $8.208 | 1M / 3M / 5M / 11M |

*Pricing based on AWS us-east-1 region. Costs may vary by region.*

## References

- [AWS DAX Cluster Sizing Guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.cluster-sizing.html)
- [AWS DAX Documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.html)
