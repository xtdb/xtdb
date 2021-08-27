(ns xtdb.docs.examples.configuration.configuration-test
  (:require [clojure.test :as t]
            [xtdb.fixtures :as fix :refer [*api*]]))

;; tag::import[]
(require '[xtdb.api :as xt]
         '[clojure.java.io :as io])
;; end::import[]

(t/deftest explicit
  (with-open [node
              ;; tag::from-explicit[]
              (xt/start-node {
                              ;; Configuration Map
                              })
              ;; end::from-explicit[]
              ]
    (t/is true)))

(t/deftest from-file
  (with-open [node
              ;; tag::from-file[]
              (xt/start-node (io/file "resources/config.json"))
              ;; end::from-file[]
              ]
    (t/is true)))

(t/deftest from-resource
  (with-open [node
              ;; tag::from-resource[]
              (xt/start-node (io/resource "config.json"))
              ;; end::from-resource[]
              ]
    (t/is true)))

(t/deftest http-server
  (with-open [node
              ;; tag::http-server[]
              (xt/start-node {:xtdb.http-server/server {:port 3000}})
              ;; end::http-server[]
              ]
    (t/is true)))

(comment "Not testing this one as it requires real info!"
         (t/deftest override-module
           (with-open [node
                       ;; tag::override-module[]
                       (xt/start-node {:xtdb/document-store {:xtdb/module 'xtdb.s3/->document-store
                                                           :bucket "my-bucket"
                                                           :prefix "my-prefix"}})
                       ;; end::override-module[]
                       ]
             (t/is true))))

(t/deftest nested-modules
  (with-open [node
              ;; tag::nested-modules-0[]
              (xt/start-node {:xtdb/tx-log {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                                                     :db-dir (io/file "/tmp/txs")}}
                              ;; end::nested-modules-0[]
                              })]
    (comment [( ("This obviously won't run so putting in a comment"
                 ;; tag::nested-modules-1[]
                 :xtdb/document-store { }
                 :xtdb/index-store { }
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
              (xt/start-node {:my-rocksdb {:xtdb/module 'xtdb.rocksdb/->kv-store
                                           :db-dir (io/file "/tmp/rocksdb")}
                              :xtdb/tx-log {:kv-store :my-rocksdb}
                              :xtdb/document-store {:kv-store :my-rocksdb}})
              ;; end::sharing-modules[]
              ]
    (t/is true)))
