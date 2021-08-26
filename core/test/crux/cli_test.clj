(ns crux.cli-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [crux.cli :as cli]
            [crux.io :as cio]
            [crux.system :as sys]))

(def crux-cli-edn
  (io/resource "crux/crux-cli.edn"))

(def crux-cli-json
  (io/resource "crux/crux-cli.json"))

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
               (->system ["-f" (str (io/as-file crux-cli-edn))]))))

    (t/testing "uses crux.edn if present"
      (with-file-override {"crux.edn" (io/as-file crux-cli-edn)}
        (fn []
          (t/is (= {::foo {:bar {}}}
                   (->system []))))))

    (t/testing "uses CLI supplied JSON file"
      (t/testing "uses CLI supplied EDN file"
        (t/is (= {::foo {:baz {}}}
                 (->system ["-f" (str (io/as-file crux-cli-json))])))))

    (t/testing "uses crux.json if present"
      (with-file-override {"crux.json" (io/as-file crux-cli-json)}
        (fn []
          (t/is (= {::foo {:baz {}}}
                   (->system []))))))

    (t/testing "looks for crux.edn on classpath, prefers to crux.json"
      (with-resource-override {"crux.json" (io/as-file crux-cli-json)
                               "crux.edn" (io/as-file crux-cli-edn)}
        (fn []
          (t/is (= {::foo {:bar {}}}
                   (->system []))))))

    (t/testing "does also look for crux.json on the classpath"
      (with-resource-override {"crux.json" (io/as-file crux-cli-json)}
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
                    (directory (-> crux-cli-edn
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
