#!/bin/bash

kubectl -n grader-clickhouse port-forward svc/clickhouse 9000:9000
