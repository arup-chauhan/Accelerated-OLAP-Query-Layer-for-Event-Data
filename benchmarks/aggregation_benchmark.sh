#!/bin/bash
set -e
ghz --insecure --config aggregation_benchmark.json --output=aggregation_benchmark_result.json
echo "Aggregation benchmark complete -> aggregation_benchmark_result.json"
