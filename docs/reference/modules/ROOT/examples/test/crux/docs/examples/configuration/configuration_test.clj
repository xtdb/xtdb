(ns crux.docs.examples.configuration.configuration-test
    (:require [clojure.test :as t]
      [crux.fixtures :as fix :refer [*api*]]))

;; tag::import[]
(require '[crux.api :as crux]
         '[clojure.java.io :as io])
;; end::import[]

(t/deftest explicit
  (with-open [node
              ;; tag::from-explicit[]
              (crux/start-node {
                                ;; Configuration Map
                                })
              ;; end::from-explicit[]
              ]
   (t/is true)))

(t/deftest from-file
  (with-open [node
              ;; tag::from-file[]
              (crux/start-node (io/file "resources/config.json"))
              ;; end::from-file[]
              ]
    (t/is true)))

(t/deftest from-resource
  (with-open [node
              ;; tag::from-resource[]
              (crux/start-node (io/resource "config.json"))
              ;; end::from-resource[]
              ]
    (t/is true)))

(t/deftest http-server
  (with-open [node
              ;; tag::http-server[]
              (crux/start-node {:crux.http-server/server {:port 3000}})
              ;; end::http-server[]
              ]
    (t/is true)))

(comment "Not testing this one as it requires real info!"
(t/deftest override-module
  (with-open [node
              ;; tag::override-module[]
              (crux/start-node {:crux/document-store {:crux/module 'crux.s3/->document-store
                                                      :bucket "my-bucket"
                                                      :prefix "my-prefix"}})
              ;; end::override-module[]
              ]
    (t/is true))))

(t/deftest nested-modules
  (with-open [node
              ;; tag::nested-modules-0[]
              (crux/start-node {:crux/tx-log {:kv-store {:crux/module 'crux.rocksdb/->kv-store
                                                         :db-dir (io/file "/tmp/txs")}}
                                ;; end::nested-modules-0[]
                                })]
                                (comment [( ("This obviously won't run so putting in a comment"
                                ;; tag::nested-modules-1[]
                                :crux/document-store { }
                                :crux/index-store { }
                                ;; end::nested-modules-1[]
                                         ) {
                                ;; tag::nested-modules-2[]
                                })
              ;; end::nested-modules-2[]
              ])
    (t/is true)))

(t/deftest sharing-modules
  (with-open [node
              ;; tag::sharing-modules[]
              (crux/start-node {:my-rocksdb {:crux/module 'crux.rocksdb/->kv-store
                                             :db-dir (io/file "/tmp/rocksdb")}
                                :crux/tx-log {:kv-store :my-rocksdb}
                                :crux/document-store {:kv-store :my-rocksdb}})
              ;; end::sharing-modules[]
              ]
    (t/is true)))