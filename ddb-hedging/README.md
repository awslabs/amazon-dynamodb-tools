# DynamoDB Request Hedging Demo

This repository demonstrates how to implement request hedging with Amazon DynamoDB to reduce tail latency in Java Spring Boot applications. 

## Overview

Request hedging is a technique where multiple identical requests are sent to DynamoDB with a configurable delay between them. The first response received is used while other in-flight requests are canceled. This helps mitigate tail latency issues in distributed systems.

## Prerequisites

- Java 21 or higher
- Gradle
- AWS Account with DynamoDB access
- AWS credentials configured

## Building the Project

```bash
./gradlew clean build
```


## Request Handler Implementations

The project provides two different request handler implementations for DynamoDB hedging, each optimized for different use cases.

### CrtHedgingRequestHandler

The CrtHedgingRequestHandler leverages the AWS Common Runtime (CRT) HTTP client, specifically designed for optimal performance with AWS services.

**Key Features:**
- Built on AWS CRT for enhanced performance with DynamoDB
- Native integration with AWS SDK metrics
- Efficient connection pooling and management
- Optimized for AWS service communication

**When to use:**
- AWS-heavy workloads
- When minimal latency is critical
- Production environments requiring AWS service optimization
- When detailed AWS-specific metrics are needed


## Configuration

The application uses Spring Boot configuration. Add the following properties to your `application.properties`:

```properties

# Client Selection
aws.dynamodb.use-crt-client=true  # Use CRT client (true) or Netty client (false)

# Hedging Parameters
ddb.hedging.request.delay=10  # Delay in ms between hedge requests
ddb.hedging.number=1  # Number of hedge requests to send Max up to 5 requests supported.

```

# Configuration Best Practices

## Hedge Delay Tuning

- **Initial Setup**
    - Start with average latency as hedge delay
    - Monitor duplicate request rate

- **Optimization**
    - Track p99 latency improvements
    - Adjust delay based on metrics
    - Balance latency vs resource usage

## Monitoring and Metrics

- **Key Metrics**
    - Request latency percentiles
    - Hedge request frequency
    - Success/failure rates
    - Connection pool statistics

- **Monitoring Focus**
    - Track connection utilization
    - Monitor request duplication
    - Measure latency improvements
