#!/bin/bash

kubectl -n grader-clickhouse port-forward svc/clickhouse --address 0.0.0.0 9000:9000
