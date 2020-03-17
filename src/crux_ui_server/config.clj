(ns crux-ui-server.config
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn])
  (:import (java.io FileNotFoundException PushbackReader IOException File)))

(def ^:private defaults
  {:console/frontend-port      5000
   :console/embed-crux         false
   :console/routes-prefix      "/console"
   :console/crux-node-url-base "localhost:8080/crux"
   :console/hide-features      #{:features/attribute-history}
   :console/crux-http-port     8080})

(defn load-edn
  "Load edn from an io/reader source (filename or io/resource)."
  [source]
  (try
    (with-open [r (io/reader source)]
      (edn/read (PushbackReader. r)))
    (catch IOException e
      (printf "Couldn't open '%s': %s\n" source (.getMessage e)))
    (catch RuntimeException e
      (printf "Error parsing edn file '%s': %s\n" source (.getMessage e)))))

(def ^:private default-conf-name "crux-console-conf.edn")

(defn- try-load-conf [^String user-conf-filename]
  (if (and user-conf-filename
           (not (.exists (io/as-file user-conf-filename))))
    (throw (FileNotFoundException. (str "file " user-conf-filename " not found"))))
  (let [conf-file-name (or user-conf-filename default-conf-name)
        file ^File (io/as-file conf-file-name)]
    (when (.exists file)
      (println "loading conf from " user-conf-filename)
      (load-edn file))))

(defn- fix-key [[k v]]
  [(cond-> k
           (and (string? k) (.startsWith k "--"))
           (subs 2))
   v])

(defn- parse-args [args-map]
  (let [w-norm-keys (into {} (map fix-key args-map))
        w-kw-keys (into {} (map (fn [[k v]] [(keyword "console" k) v]) w-norm-keys))
        w-parsed (into {} (map (fn [[k v]] [k (cond-> v (string? v) read-string)]) w-kw-keys))]
    w-parsed))

(assert
  (= {:console/frontend-port  5000,
      :console/embed-crux     false,
      :console/crux-http-port 8080}
     (parse-args
       {"--frontend-port"  "5000"
        "--embed-crux"     "false"
        "--crux-http-port" "8080"})))

(defn calc-conf [args]
  (let [conf-filename (get args "--conf-file")
        args          (dissoc args "--conf-file")]
    (merge defaults
           (try-load-conf conf-filename)
           (parse-args args))))

