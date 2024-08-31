grpcurl -d @ \
    -plaintext \
    -import-path ./proto-definitions \
    -proto log.proto \
    localhost:50056 log.LogService/StreamLogs <<EOM
{
  "datetime": "2024-08-30T12:00:00Z",
  "tenant_name": "example_tenant",
  "item_id": "item123",
  "status": "active",
  "qty": 10.5,
  "metadata": {
    "key2": "value2"
  }
}

EOM
