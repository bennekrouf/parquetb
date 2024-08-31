grpcurl -d @ \
    -plaintext \
    -import-path ./proto-definitions \
    -proto parquetb.proto \
    localhost:50056 parquetb.ParquetbService/StreamLogs <<EOM
{
  "tenant_name": "gibro",
  "item_id": "item123",
  "status": "active",
  "qty": 10.5,
  "metadata": {
    "key2": "value2"
  }
}

EOM
