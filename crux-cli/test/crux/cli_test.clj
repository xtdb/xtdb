(ns crux.cli-test
  (:require [crux.cli :as cli]
            [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.fixtures :as f]
            [crux.io :as cio]
            [clojure.string :as str]))

(def crux-cli-edn
  (io/resource "crux/crux-cli.edn"))

(def crux-cli-props
  (io/resource "crux/crux-cli.properties"))

(defn with-file-override [files f]
  (with-redefs [io/file (some-fn files io/file)]
    (f)))

(defn with-resource-override [resources f]
  (with-redefs [io/resource (some-fn resources io/resource)]
    (f)))

(t/deftest test-config-merging
  (t/testing "uses CLI supplied EDN file"
    (t/is (= :bar (-> (cli/parse-args ["-e" (str (io/as-file crux-cli-edn))])
                      (get-in [::cli/node-opts ::foo])))))

  (t/testing "uses crux.edn if present"
    (with-file-override {"crux.edn" (io/as-file crux-cli-edn)}
      (fn []
        (t/is (= :bar (-> (cli/parse-args [])
                          (get-in [::cli/node-opts ::foo])))))))

  (t/testing "uses CLI supplied props file"
    (t/testing "uses CLI supplied EDN file"
      (t/is (= "baz" (-> (cli/parse-args ["-p" (str (io/as-file crux-cli-props))])
                         (get-in [::cli/node-opts ::foo]))))))

  (t/testing "uses crux.properties if present"
    (with-file-override {"crux.properties" (io/as-file crux-cli-props)}
      (fn []
        (t/is (= "baz" (-> (cli/parse-args [])
                           (get-in [::cli/node-opts ::foo])))))))

  (t/testing "looks for crux.edn on classpath, prefers to crux.properties"
    (with-resource-override {"crux.properties" (io/as-file crux-cli-props)
                             "crux.edn" (io/as-file crux-cli-edn)}
      (fn []
        (t/is (= :bar (-> (cli/parse-args [])
                          (get-in [::cli/node-opts ::foo])))))))

  (t/testing "does also look for crux.properties on the classpath"
    (with-resource-override {"crux.properties" (io/as-file crux-cli-props)}
      (fn []
        (t/is (= "baz" (-> (cli/parse-args [])
                           (get-in [::cli/node-opts ::foo]))))))))

(defn- string-array ^"[Ljava.lang.String;" [& strs]
  (into-array String strs))

(t/deftest test-cli-can-start
  (f/with-tmp-dir "cli" [cli-dir]
    (let [opts {:crux.node/topology '[crux.standalone/topology crux.http-server/module]
                :crux.standalone/event-log-dir (str (io/file cli-dir "event-log"))
                :crux.http-server/port (cio/free-port)
                :crux.node/kv-store 'crux.kv.memdb/kv
                :crux.kv/db-dir (str (io/file cli-dir "db-dir"))}

          process (.. (ProcessBuilder. (string-array
                                        "timeout" "30s"
                                        "lein" "run" "crux.cli"
                                        "-x" (pr-str opts)))
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
                     (filter #(str/includes? % "org.eclipse.jetty.server.Server - Started"))
                     first)))
        (finally
          (.destroy process))))))
