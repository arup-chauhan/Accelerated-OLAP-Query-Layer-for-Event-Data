#!/bin/bash
set -e
ghz --insecure --config ingestion_benchmark.json --output=ingestion_benchmark_result.json
echo "Ingestion benchmark complete -> ingestion_benchmark_result.json"
