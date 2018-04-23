#!/bin/sh

docker run -d --name couchbase_test -p 8091-8094:8091-8094 -p 11210:11210 couchbase:5.1.0

sleep 20

curl -X POST http://127.0.0.1:8091/pools/default -d memoryQuota=300 -d indexMemoryQuota=300 -d flushEnabled=1
curl http://127.0.0.1:8091/node/controller/setupServices -d services=kv%2Cn1ql%2Cindex
curl http://127.0.0.1:8091/settings/web -d port=8091 -d username=Administrator -d password=Administrator
curl -u Administrator:Administrator -X POST http://127.0.0.1:8091/settings/indexes -d 'storageMode=memory_optimized'
curl -u Administrator:Administrator -X POST http://127.0.0.1:8091/pools/default/buckets -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' --data 'name=default&bucketType=membase&autoCompactionDefined=false&evictionPolicy=valueOnly&threadsNumber=3&replicaNumber=1&replicaIndex=0&conflictResolutionType=seqno&ramQuotaMB=256&flushEnabled=1'

