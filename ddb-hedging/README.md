# DynamoDB Request Hedging Demo

This demonstrates how to implement request hedging with Amazon DynamoDB to reduce tail latency in AWS SDK for Java 2.x.

## Overview

Request hedging is a technique where multiple identical requests are sent to DynamoDB with a configurable delay between them. The first response received is used while other in-flight requests are canceled. This helps mitigate tail latency issues in distributed systems.

## Prerequisites

- Java 21 or higher
- Gradle
- AWS Account with DynamoDB access
- AWS credentials configured
- TODO: 
  - DynamoDB table name 
  - and PK, SK requirements?

## Building the Project

```bash
./gradlew clean build
```

Todo:
* How to run the project
* Sample request:
  * URIs
  * http://localhost:8080/api/ddbDemo/readData
  * 



TODO: Following are the most classes in this repo are the hedging Request Handlers
## Request Handler Implementations

The project provides two different request handler implementations for DynamoDB hedging, each optimized for different use cases.

**Key Features:**
- Implements inbound message handling for DynamoDB responses
- Supports request hedging with configurable delays
- configurable to send upto 5 staggered requests  

### CrtHedgingRequestHandler

The CrtHedgingRequestHandler leverages the AWS Common Runtime (CRT) HTTP client, specifically designed for optimal performance with AWS services.

**When to use:**
- DynamoDB Clients using AwsCrtAsyncHttpClient

### NettyHedgingRequestHandler

The NettyHedgingRequestHandler is a Netty-based implementation.

**When to use:**
- DynamoDB Clients using NettyNioAsyncHttpClient



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
