# this file illustrates different requests to crux built in http server

# status
curl http://localhost:8081

# submit tx
curl -X POST http://localhost:8081/tx-log \
  -H 'Content-Type: application/edn' \
  --data-binary \
  '[[:crux.tx/put
     {:crux.db/id #crux/id :id/jeff
      :testing "works"}]
    [:crux.tx/put
     {:crux.db/id :id/jane
      :skill :singing}]]'

# query
curl -X POST http://localhost:8081/query \
  -H 'Content-Type: application/edn' \
  --data-binary \
  '{:query {:find [e] :where [[e :skill _]] :full-results? true}}'

curl -X POST http://localhost:8081/query \
  -H 'Content-Type: application/edn' \
  --data-binary \
  '{:query {:find [e] :where [[e :crux.db/id _]] :full-results? true}}'

# entity
curl -X POST http://localhost:8081/entity \
  -H 'Content-Type: application/edn' \
  --data-binary '{:eid :id/jane}'

# stats for indexed attrs
curl http://localhost:8081/attribute-stats

# for more examples visit https://juxt.pro/crux/docs/rest.html#_using_the_http_api
