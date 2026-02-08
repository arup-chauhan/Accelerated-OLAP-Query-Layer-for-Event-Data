#!/bin/sh
mkdir -p target
envsubst < monitoring/prometheus.yml > target/prometheus.yml
envsubst < monitoring/grafana_datasource.yml > target/grafana_datasource.yml
