(ns xtdb.cli-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.cli :as cli]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.node :as xtn])
  (:import (xtdb.indexer.live_index LiveIndex)))

(def xtdb-cli-edn
  (io/resource "xtdb/cli-test.edn"))

(def xtdb-cli-edn-env
  (io/resource "xtdb/cli-test-env.edn"))

(def xtdb-cli-yaml
  (io/resource "xtdb/cli-test.yaml"))

(def xtdb-cli-yaml-multi-dot
  (io/resource "xtdb/cli-test.multi-dot.yaml"))

(def xtdb-cli-edn-multi-dot
  (io/resource "xtdb/cli-test.multi-dot.edn"))

(defn with-file-override [files f]
  (with-redefs [io/file (some-fn files io/file)]
    (f)))

(defn with-resource-override [resources f]
  (let [og io/resource]
    (with-redefs [io/resource (fn [& args]
                                (apply (some-fn resources og) args))]
      (f))))

(defn parse-args [args cli-spec]
  (let [[tag arg] (cli/parse-args args cli-spec)]
    (case tag
      :success arg
      (throw (err/fault ::failed-parsing-args "Failed to parse CLI args"
                        {:result [tag arg]})))))

(t/deftest no-config
  (t/is (= {} (parse-args [] cli/node-cli-spec))
        "if no config present via file, returns an empty map")

  (t/is (= {} (cli/file->node-opts nil))
        "if no file provided, returns an empty map as node opts"))

(t/deftest test-config
  (t/is (= {:file (io/as-file xtdb-cli-edn)}
           (parse-args ["-f" (str (io/as-file xtdb-cli-edn))] cli/node-cli-spec))
        "uses CLI supplied EDN file")

  (t/testing "uses xtdb.edn if present"
    (with-file-override {"xtdb.edn" (io/as-file xtdb-cli-edn)}
      (fn []
        (t/is (= {::foo {:bar {}}} (cli/file->node-opts nil))
              "uses xtdb.edn if present")))))

(t/deftest test-env-loading
  (with-redefs [cli/read-env-var (fn [env-name]
                                   (when (= (str env-name) "TEST_ENV")
                                     "hello world"))]
    (t/testing
      (t/is (= {::foo "hello world"}
               (cli/file->node-opts (io/as-file xtdb-cli-edn-env)))
            "EDN config - #env reader tag fetches from env"))))

(defn- ->node-opts [cli-args]
  (cli/file->node-opts (:file (parse-args cli-args cli/node-cli-spec))))

;; Expect YAML config file to just return the file - this will get passed to
;; start-node and subsequently decoded by the Kotlin API
(t/deftest test-config-yaml
  (t/testing "Explicitly provided YAML file will output specified file-location"
    (t/is (= (io/file xtdb-cli-yaml)
             (->node-opts ["-f" (str (io/as-file xtdb-cli-yaml))]))))

  (t/testing "returns file location of xtdb.yaml if present"
    (with-file-override {"xtdb.yaml" (io/as-file xtdb-cli-yaml)}
      (fn []
        (t/is (= (io/file (io/resource "xtdb/cli-test.yaml"))
                 (->node-opts []))))))

  (t/testing "also returns file location of xtdb.yaml if on the classpath"
    (with-resource-override {"xtdb.yaml" (io/as-file xtdb-cli-yaml)}
      (fn []
        (t/is (= (io/file (io/resource "xtdb/cli-test.yaml"))
                 (->node-opts []))))))

  (t/testing "node opts passed to start-node passes through yaml file and starts node"
    (with-open [node (xtn/start-node (->node-opts ["-f" (str (io/as-file xtdb-cli-yaml))]))]
      (let [^LiveIndex live-idx (.getLiveIndex (db/primary-db node))]
        (t/is (= 65 (.log-limit live-idx))
              "using provided config"))
      (xt/submit-tx node [[:put-docs :docs {:xt/id :foo}]])
      (t/is (= [{:e :foo}] (xt/q node '(from :docs [{:xt/id e}])))))))

(t/deftest test-multi-dot-yaml
  (t/testing "YAML with multiple dots in the filename"
    (t/is (= (io/file xtdb-cli-yaml-multi-dot)
             (->node-opts ["-f" (str (io/as-file xtdb-cli-yaml-multi-dot))]))))

  (t/testing "EDN with multiple dots in the filename"
    (t/is (= {:xtdb.cli-test/foo {:bar {}}}
             (->node-opts ["-f" (str (io/as-file xtdb-cli-edn-multi-dot))]))))

  (t/testing "YAML with multiple dots in the filename starts node"
    (with-open [node (xtn/start-node (->node-opts ["-f" (str (io/as-file xtdb-cli-yaml-multi-dot))]))]
      (let [^LiveIndex live-idx (.getLiveIndex (db/primary-db node))]
        (t/is (= 65 (.log-limit live-idx))
              "using provided config"))
      (xt/submit-tx node [[:put-docs :docs {:xt/id :foo}]])
      (t/is (= [{:e :foo}] (xt/q node '(from :docs [{:xt/id e}])))))))
