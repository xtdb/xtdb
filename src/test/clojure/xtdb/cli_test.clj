(ns xtdb.cli-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.cli :as cli]))

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

(t/deftest test-config-merging
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
                   (->system []))))))))
