(ns crux.cli-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [crux.cli :as cli]
            [crux.io :as cio]
            [crux.system :as sys]))

(def xtdb-cli-edn
  (io/resource "crux/xtdb-cli.edn"))

(def xtdb-cli-json
  (io/resource "crux/xtdb-cli.json"))

(defn with-file-override [files f]
  (with-redefs [io/file (some-fn files io/file)]
    (f)))

(defn with-resource-override [resources f]
  (with-redefs [io/resource (some-fn resources io/resource)]
    (f)))

(defn- ->foo [opts] opts)

(t/deftest test-config-merging
  (letfn [(->system [cli-args]
            (-> (::cli/node-opts (cli/parse-args cli-args))
                (sys/prep-system)
                (sys/start-system)
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

(defn- string-array ^"[Ljava.lang.String;" [& strs]
  (into-array String strs))

(t/deftest test-cli-can-start
  (let [opts {:xtdb.http-server/server {:port (cio/free-port)}}

        process (.. (ProcessBuilder. (string-array
                                      "timeout" "30s"
                                      "lein" "with-profiles" "+cli-e2e-test" "run" "-m" "crux.main"
                                      "--edn" (pr-str opts)))
                    (redirectErrorStream true)
                    (directory (-> xtdb-cli-edn
                                   (io/as-file)
                                   (.getParentFile)
                                   (.getParentFile)
                                   (.getParentFile)))
                    start)]
    (try
      (with-open [out (io/reader (.getInputStream process))]
        (t/is (->> (line-seq out)
                   (map #(doto % println))
                   (filter #(str/includes? % "crux.cli - Node started"))
                   first)))
      (finally
        (.destroy process)))))
