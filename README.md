# Amazon DynamoDB Tools

An open-source collection of tools and utilities for Amazon DynamoDB. This repository contains tools that help with cost-optimization, capacity planning, bulk data operations, and performance tuning.

## What's New ✨

Introducing **DynamoDB Optima**—a unified cost optimization and capacity analysis platform that consolidates three previously separate tools into one comprehensive solution. With multi-account discovery, CloudWatch metrics collection, capacity mode optimization, and an integrated Streamlit dashboard, Optima provides enterprise-grade insights for your DynamoDB infrastructure.

[Get started with DynamoDB Optima](tools/dynamodb-optima/README.md)

## Repository Structure

This repository is organized into four main sections to help you quickly find the right tool:

| Directory | Purpose |
|-----------|----------|
| [tools/](tools/) | Tools for DynamoDB operations |
| [snippets/](snippets/) | Lightweight, single-purpose utilities |
| [beta/](beta/) | Experimental tools and educational content |
| [archived/](archived/) | Deprecated tools kept for reference |

## Tools 🛠️

### DynamoDB Optima

[View Documentation](tools/dynamodb-optima/README.md)

A cost optimization and capacity analysis platform for multi-account AWS environments.

**Capabilities:**
- Multi-account discovery via AWS Organizations
- CloudWatch metrics collection with incremental updates
- Cost and Usage Report (CUR) analysis
- Capacity mode optimization (On-Demand vs Provisioned)
- Table class optimization (Standard vs Standard-IA)
- Autoscaling simulation and recommendations
- Interactive Streamlit dashboard for visualization

### Bulk Executor

[View Documentation](tools/bulk_executor/README.md)

A serverless solution for executing bulk operations on DynamoDB tables using AWS Glue.

**Use Cases:**
- Data migrations between tables or accounts
- Batch updates and transformations
- Parallel processing with automatic retry logic
- Schema evolution and data backfills

### DAX Calculator

[View Documentation](tools/dax_calculator/README.md)

Calculate DAX cluster sizing and estimate costs based on your workload requirements.

**Features:**
- Cluster size recommendations based on throughput
- Cost estimation and TCO comparison
- Performance impact analysis
- Right-sizing guidance

### Item Size Calculator

[View Documentation](snippets/item_size_calculator/README.md)

An NPM package for calculating DynamoDB item sizes in bytes.

**Features:**
- Calculate RCU/WCU consumption
- Validate items against the 400KB limit
- Support for both DDB-JSON and native JSON formats
- TypeScript support


## Migration Guide 📦

If you're currently using one of our archived tools, we've consolidated functionality into DynamoDB Optima for a better experience:

| Legacy Tool | New Command | Archive Date |
|-------------|-------------|-------------|
| metrics-collector | `dynamodb-optima collect` | February 2026 |
| capacity-mode-evaluator | `dynamodb-optima analyze-capacity` | February 2026 |
| table_class_optimizer | `dynamodb-optima analyze-table-class` | February 2026 |

[View complete migration guide](archived/README.md)

## Contributing 🤝

We welcome and encourage contributions from the community. Whether you're fixing bugs, improving documentation, or proposing new features, your input helps make these tools better for everyone.

**Ways to contribute:**
- Report bugs and request features via [GitHub Issues](https://github.com/awslabs/amazon-dynamodb-tools/issues)
- Submit pull requests with improvements
- Improve documentation and examples
- Share your use cases and success stories

Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct, development process, and how to submit pull requests.

## Community and Support 💬

- **Issues**: Report bugs or request features via [GitHub Issues](https://github.com/awslabs/amazon-dynamodb-tools/issues)
- **Discussions**: Join conversations about DynamoDB best practices
- **Documentation**: Each tool includes comprehensive documentation and examples

For AWS support inquiries, please use official AWS support channels.

## Security 🔒

See [CONTRIBUTING](CONTRIBUTING.md) for more information.

## License 📄

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

## Disclaimer

These tools are provided as-is for use at your own discretion and risk. While we test and verify functionality, we recommend reviewing the code and testing thoroughly in non-production environments before deploying to production. Each tool has been developed independently—please review individual requirements and documentation carefully.

---

Built with ❤️ by the AWS community. Star this repository if you find it useful!
