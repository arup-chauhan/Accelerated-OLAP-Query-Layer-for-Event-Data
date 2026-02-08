#!/bin/bash
set -e
ghz --insecure --config activity_benchmark.json --output=activity_benchmark_result.json
echo "Activity benchmark complete -> activity_benchmark_result.json"
