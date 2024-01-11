(ns xtdb.cli-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.cli :as cli]
            [xtdb.config :as config]
            [clojure.data.json :as json]))

(def xtdb-cli-edn
  (io/resource "xtdb/cli-test.edn"))

(def xtdb-cli-json
  (io/resource "xtdb/cli-test.json"))

(defn with-file-override [files f]
  (with-redefs [io/file (some-fn files io/file)]
    (f)))

(defn with-resource-override [resources f]
  (let [og io/resource]
    (with-redefs [io/resource (fn [& args]
                                (apply (some-fn resources og) args))]
      (f))))

(defmethod ig/init-key ::foo [_ opts] opts)

(t/deftest test-config
  (letfn [(->system [cli-args]
            (-> (::cli/node-opts (cli/parse-args cli-args))
                ig/prep
                ig/init
                (->> (into {}))))]
    (t/testing "uses CLI supplied EDN file"
      (t/is (= {::foo {:bar {}}}
               (->system ["-f" (str (io/as-file xtdb-cli-edn))]))))

    (t/testing "uses xtdb.edn if present"
      (with-file-override {"xtdb.edn" (io/as-file xtdb-cli-edn)}
        (fn []
          (t/is (= {::foo {:bar {}}}
                   (->system []))))))

    (t/testing "uses CLI supplied JSON file"
      (t/testing "uses CLI supplied EDN file"
        (t/is (= {::foo {:baz {}}}
                 (->system ["-f" (str (io/as-file xtdb-cli-json))])))))

    (t/testing "uses xtdb.json if present"
      (with-file-override {"xtdb.json" (io/as-file xtdb-cli-json)}
        (fn []
          (t/is (= {::foo {:baz {}}}
                   (->system []))))))

    (t/testing "looks for xtdb.edn on classpath, prefers to xtdb.json"
      (with-resource-override {"xtdb.json" (io/as-file xtdb-cli-json)
                               "xtdb.edn" (io/as-file xtdb-cli-edn)}
        (fn []
          (t/is (= {::foo {:bar {}}}
                   (->system []))))))

    (t/testing "does also look for xtdb.json on the classpath"
      (with-resource-override {"xtdb.json" (io/as-file xtdb-cli-json)}
        (fn []
          (t/is (= {::foo {:baz {}}}
                   (->system []))))))
    
    (t/testing "uses config passed in via --edn"
      (t/is (= {::foo 1}
               (->system ["--edn" "{:xtdb.cli-test/foo 1}"]))))
    
    (t/testing "uses config passed in via --json"
      (t/is (= {::foo 1}
               (->system ["--json" "{\"xtdb.cli-test/foo\": 1}"]))))

    (t/testing "if file config passed in, prefers to edn"
      (fn []
        (= {::foo {:baz {}}}
           (->system ["-f" (str (io/as-file xtdb-cli-edn))
                      "--edn" "{:xtdb.cli-test/foo 1}"]))))
    
    (t/testing "if xtdb.edn present, uses this in place to --edn"
      (with-file-override {"xtdb.edn" (io/as-file xtdb-cli-edn)}
        (fn []
          (= {::foo {:baz {}}}
             (->system ["--edn" "{:xtdb.cli-test/foo 1}"])))))))

(t/deftest test-env-loading
  (with-redefs [config/read-env-var (fn [env-name]
                                      (when (= (str env-name) "TEST_ENV") "hello world"))]
    (letfn [(->system [cli-args]
              (-> (::cli/node-opts (cli/parse-args cli-args))
                  ig/prep
                  ig/init
                  (->> (into {}))))]

      (t/testing "EDN config - #env reader tag fetches from env"
        (t/is (= {::foo "hello world"}
                 (->system ["--edn" "{:xtdb.cli-test/foo #env TEST_ENV}"]))))

      (t/testing "JSON config - env object fetched from env"
        (t/is (= {::foo "hello world"}
                 (->system ["--json" "{\"xtdb.cli-test/foo\": {\"@env\": \"TEST_ENV\"}}"])))))))

(defmethod ig/init-key ::bar [_ opts] opts)

(t/deftest test-ref-handling
  (letfn [(->system [cli-args]
            (-> (::cli/node-opts (cli/parse-args cli-args))
                ig/prep
                ig/init
                (->> (into {}))))]
    
    (t/testing "EDN config - #ig/ref reader tag correctly includes ref"
      (t/is (= {::foo {:baz 1}
                ::bar {:foo {:baz 1}}}
               (->system ["--edn" "{:xtdb.cli-test/foo {:baz 1}
                                    :xtdb.cli-test/bar {:foo #ig/ref :xtdb.cli-test/foo}}"]))))
    
    (t/testing "JSON config - ref object correctly includes ref"
      (t/is (= {::foo {:baz 1}
                ::bar {:foo {:baz 1}}}
               (->system ["--json" "{\"xtdb.cli-test/foo\": {\"baz\": 1},
                                     \"xtdb.cli-test/bar\": {\"foo\": {\"@ref\": \"xtdb.cli-test/foo\"}}}"]))))))