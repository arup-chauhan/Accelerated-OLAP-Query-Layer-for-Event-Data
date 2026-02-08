#!/bin/bash
set -e
ghz --insecure --config bulk_activity_benchmark.json --output=bulk_activity_benchmark_result.json
echo "Bulk Activity benchmark complete -> bulk_activity_benchmark_result.json"
