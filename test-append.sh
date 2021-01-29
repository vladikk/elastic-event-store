curl --header "Content-Type: application/json" \
     --request POST \
     --data '{ "metadata": { "timestamp": "1233214", "issuedBy": "2342", "commandId": "2342342" }, "payload": [ { "eventType": "init", "data": 1 }, { "eventType": "sell", "data": 10 }, { "eventType": "buy", "data": 5 } ]  }' \
     https://r26tvo7a9d.execute-api.us-east-1.amazonaws.com/Prod/commit\?streamId=123\&changesetId=123


     