#!/bin/bash

aws dynamodb create-table \
  --cli-input-json file://schema.json \
  --endpoint-url http://localhost:8000