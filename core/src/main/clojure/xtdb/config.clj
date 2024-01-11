(ns xtdb.config
  (:require [clj-yaml.core :as yaml]
            [clojure.data.json :as json]
            [clojure.walk :as walk]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.util :as util])
  (:import java.io.File))

(defn read-env-var [env-var]
  (System/getenv (str env-var)))

(defn read-ig-ref [ref]
  (ig/ref (keyword ref)))

(defn edn-read-string [edn-string]
  (ig/read-string {:readers {'env read-env-var}}
                  edn-string))

(defn json-read-string [json-string]
  (walk/postwalk
   (fn [item]
     (let [env-key (keyword "@env")
           ref-key (keyword "@ref")]
       (cond
         (env-key item) (read-env-var (env-key item))
         (ref-key item) (read-ig-ref (ref-key item))
         :else item)))
   (json/read-str json-string :key-fn keyword)))

(defn yaml-read-string [yaml-string] 
  (yaml/parse-string yaml-string
                     :keywords true
                     :unknown-tag-fn (fn [{:keys [tag value]}]
                                       (cond
                                         (= "!Env" tag) (read-env-var value)
                                         (= "!Ref" tag) (read-ig-ref value)
                                         :else value))))

(defn- read-opts-from-file [^File f]
  (let [file-extension (util/file-extension f)]
    (cond
      (= file-extension "edn") (edn-read-string (slurp f))
      (= file-extension "json") (json-read-string (slurp f))
      (= file-extension "yaml") (yaml-read-string (slurp f)) 
      :else (throw (err/illegal-arg :unsupported-options-type
                                    {::err/message (format "Unsupported type for options file: '%s'" file-extension)})))))

(defn file->config-opts
  [^File f]
  (if (.exists f)
    (read-opts-from-file f)
    (throw (err/illegal-arg :opts-file-not-found
                            {::err/message (format "File not found: '%s'" (.getName f))}))))