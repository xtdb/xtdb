(ns crux.cli-test
  (:require [crux.cli :as cli]
            [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.fixtures :as f]
            [crux.io :as cio]
            [clojure.string :as str]))

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

(t/deftest test-config-merging
  (t/testing "uses CLI supplied EDN file"
    (t/is (= :bar (-> (cli/parse-args ["-f" (str (io/as-file crux-cli-edn))])
                      (get-in [::cli/node-opts 0 ::foo])))))

  (t/testing "uses crux.edn if present"
    (with-file-override {"crux.edn" (io/as-file crux-cli-edn)}
      (fn []
        (t/is (= :bar (-> (cli/parse-args [])
                          (get-in [::cli/node-opts 0 ::foo])))))))

  (t/testing "uses CLI supplied JSON file"
    (t/testing "uses CLI supplied EDN file"
      (t/is (= "baz" (-> (cli/parse-args ["-f" (str (io/as-file crux-cli-json))])
                         (get-in [::cli/node-opts 0 "crux.cli-test/foo"]))))))

  (t/testing "uses crux.json if present"
    (with-file-override {"crux.json" (io/as-file crux-cli-json)}
      (fn []
        (t/is (= "baz" (-> (cli/parse-args [])
                           (get-in [::cli/node-opts 0 "crux.cli-test/foo"])))))))

  (t/testing "looks for crux.edn on classpath, prefers to crux.json"
    (with-resource-override {"crux.json" (io/as-file crux-cli-json)
                             "crux.edn" (io/as-file crux-cli-edn)}
      (fn []
        (t/is (= :bar (-> (cli/parse-args [])
                          (get-in [::cli/node-opts 0 ::foo])))))))

  (t/testing "does also look for crux.json on the classpath"
    (with-resource-override {"crux.json" (io/as-file crux-cli-json)}
      (fn []
        (t/is (= "baz" (-> (cli/parse-args [])
                           (get-in [::cli/node-opts 0 "crux.cli-test/foo"]))))))))

(defn- string-array ^"[Ljava.lang.String;" [& strs]
  (into-array String strs))

(t/deftest test-cli-can-start
  (let [opts {:crux.http-server/server {:port (cio/free-port)}}

        process (.. (ProcessBuilder. (string-array
                                      "timeout" "30s"
                                      "lein" "with-profiles" "+cli-e2e-test" "run" "crux.cli"
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
