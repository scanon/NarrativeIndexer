#!/bin/sh

ES=http://localhost:9200/cinarrative

curl -X DELETE $ES 
curl -X PUT ${ES}/
curl -X PUT ${ES}/_mapping/data -d "$(cat specs/data.json)"
curl -X PUT ${ES}/_mapping/access -d "$(cat specs/access.json)"

