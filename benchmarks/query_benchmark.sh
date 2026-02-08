#!/bin/bash
set -e
ghz --insecure --config query_benchmark.json --output=query_benchmark_result.json
echo "Query benchmark complete -> query_benchmark_result.json"
