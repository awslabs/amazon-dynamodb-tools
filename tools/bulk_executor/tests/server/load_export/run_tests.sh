#!/bin/bash
DIR="$(cd "$(dirname "$0")" && pwd)"
AWS_ACCESS_KEY_ID=fake AWS_SECRET_ACCESS_KEY=fake AWS_DEFAULT_REGION=us-east-1 python3 "$DIR/test_runner.py" "$@"
