
curl -v localhost:8080/query \
    -H 'Content-Type: application/edn' \
    -H "Origin: http://example.com" \
    --data-binary \
    "{:query
        [:find e
         :where
         [e :crux.db/id]
         [(contains? ids e)]
         :args {}
         :limit 10]}"

# breaks, to test exception reporting
curl -v localhost:8080/query \
    -H 'Content-Type: application/edn' \
    -H "Origin: http://example.com" \
    --data-binary \
    "{:query
        [:find e
         :where
         [e :crux.db/id]
         [(contains? ids e)]
         :args {}
         :limit 10]}"


curl localhost:8080/query \
    -H 'Content-Type: application/edn' \
    --data-binary \
    '{:query
       {:find [e]
        :where
        [[e :crux.db/id]
         [(contains? ids  e)]]
        :args [{ids #{:ids/tech-ticker-2}}]
        :limit 2}}'

