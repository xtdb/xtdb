echo ">> Status"
# tag::xtdb-status[]
curl -X GET \
     -H "Accept: application/transit+json" \
     http://localhost:3000/status
# end::xtdb-status[]
echo

echo ">> Transaction"
last_tx=$(
# tag::xtdb-tx[]
curl -X POST \
     -H "Content-Type: application/transit+json" \
     -H "Accept: application/transit+json" \
     -d '{"~:tx-ops": [
           ["~#xtdb.tx/put", {
             "~:table-name": "~:users",
             "~:doc": {
               "~:xt/id": 1,
               "~:first-name": "John",
               "~:last-name": "Doe"
             }
            }]
         ]}' \
     http://localhost:3000/tx 2>/dev/null
# end::xtdb-tx[]
)
echo "$last_tx"

echo ">> Post Transaction Status"
curl -X GET \
     -H "Accept: application/transit+json" \
     http://localhost:3000/status
echo

echo ">> XTQL Query"
# tag::xtdb-xtql[]
curl -X POST \
     -H "Content-Type: application/transit+json" \
     -H "Accept: application/transit+json" \
     -d '{"~:query": ["~#list",["~$from","~:users",["~$first-name", "~$last-name"]]],
          "~:after-tx": '"$last_tx"'}' \
     http://localhost:3000/query
# end::xtdb-xtql[]
echo

echo ">> SQL Query"
# tag::xtdb-sql[]
curl -X POST \
     -H "Content-Type: application/transit+json" \
     -H "Accept: application/transit+json" \
     -d '{"~:query": "SELECT u.first_name, u.last_name FROM users AS u",
          "~:after-tx": '"$last_tx"'}' \
     http://localhost:3000/query
# end::xtdb-sql[]
