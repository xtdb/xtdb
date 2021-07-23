(ns ^:no-doc crux.system
  (:refer-clojure :exclude [ref])
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pp]
            [juxt.clojars-mirrors.dependency.v1v0v0.com.stuartsierra.dependency :as dep]
            [crux.io :as cio]
            [crux.error :as err])
  (:import [java.io Closeable File Writer]
           java.net.URL
           [java.nio.file Path Paths]
           java.time.Duration
           java.net.URI
           java.util.concurrent.TimeUnit
           java.util.Map))

(defprotocol OptsSource
  (load-opts [src]))

(alter-meta! #'load-opts assoc :private true)

(defn- read-opts [src file-name]
  (cond
    (str/ends-with? file-name ".json") (json/read-str (slurp src))
    (str/ends-with? file-name ".edn") (edn/read-string (slurp src))
    :else (throw (err/illegal-arg :unsupported-options-type
                                  {::err/message (format "Unsupported options type: '%s'" file-name)}))))

(extend-protocol OptsSource
  Map
  (load-opts [src] src)

  File
  (load-opts [src] (read-opts src (.getName src)))

  URL
  (load-opts [src] (read-opts src (.getFile src)))

  nil
  (load-opts [_] nil))

(defn- spread-opts [opts m]
  (->> m
       (into {} (map (juxt (comp keyword key) val)))
       (reduce-kv (fn [acc k v]
                    (-> acc (update k (fnil conj []) v)))
                  opts)))

(declare ->ModuleRef)

(defn- with-default-module [opts-seq k]
  (cond-> (vec opts-seq)
    (qualified-keyword? k) (conj (->ModuleRef (symbol (namespace k) (str "->" (name k))) {}))))

(defprotocol Dep
  (prepare-dep [dep k-path opts-seq]))

(alter-meta! #'prepare-dep assoc :private true)

(defn parse-opts [opts args]
  (->> args
       (into {}
             (map (fn [[k {:keys [spec required? default]}]]
                    (let [v (first (get opts k [default]))]
                      (when (and required? (nil? v))
                        (throw (err/illegal-arg :arg-required
                                                {::err/message (format "Arg %s required" (pr-str k))})))

                      (let [conformed-v (some-> v (cond->> spec (s/conform spec)))]
                        (if (s/invalid? conformed-v)
                          (throw (err/illegal-arg :arg-invalid
                                                  {::err/message (format "Arg %s = %s invalid: %s" (pr-str k) v (s/explain-str spec v))}))
                          [k conformed-v]))))))))

(defrecord Module [start-fn before deps args]
  Dep
  (prepare-dep [this k-path opts]
    {:module {:start-fn start-fn
              :before before
              :refs (->> (keys deps)
                         (into {}
                               (map (fn [k]
                                      [k (conj k-path k)]))))
              :opts (merge (->> (apply dissoc opts (concat (keys args) (keys deps)))
                                (into {} (map (juxt key (comp first val)))))
                           (try
                             (parse-opts opts args)
                             (catch IllegalArgumentException e
                               (throw (err/illegal-arg :error-parsing-opts
                                                       {::err/message (format "Error parsing opts for %s" (pr-str k-path))}
                                                       e)))))}
     :deps (->> (keys deps)
                (into {} (map (fn [k]
                                [(conj k-path k)
                                 (-> (conj (get opts k)
                                           (get deps k))
                                     (with-default-module k))]))))}))

(defrecord ModuleRef [sym opts]
  Dep
  (prepare-dep [this k-path opts]
    (let [opts (-> opts (spread-opts (:opts this)))
          module-var (or (try
                           (requiring-resolve (:sym this))
                           (catch Exception e
                             (throw (err/illegal-arg :error-locating-module
                                                     {::err/message "Error locating module"
                                                      :module sym}
                                                     e))))
                         (throw (ex-info "Error locating module" {:module sym})))
          {::keys [deps args before]} (meta module-var)]
      (prepare-dep (->Module (deref module-var) before deps args) k-path opts))))

(defrecord Ref [k]
  Dep
  (prepare-dep [this k-path opts]
    {:module {:start-fn :dep
              :refs {:dep [k]}
              :ref [k]}}))

(defn- opts-reducer [k-path]
  (fn f [opts el]
    (let [module (or (:crux/module el) (get el "crux/module"))]
      (cond
        module (-> opts
                   (f (dissoc (into {} el) :crux/module "crux/module"))
                   (f (cond-> module (string? module) symbol)))
        (qualified-symbol? el) (reduced (prepare-dep (->ModuleRef el {}) k-path opts))
        (keyword? el) (reduced (prepare-dep (->Ref el) k-path opts))
        (string? el) (reduced (prepare-dep (->Ref (keyword el)) k-path opts))
        (satisfies? Dep el) (reduced (prepare-dep el k-path opts))
        (fn? el) (reduced (prepare-dep (->Module el (::before (meta el)) (::deps (meta el)) (::args (meta el))) k-path opts))
        (instance? Map el) (spread-opts opts el)
        (nil? el) opts
        :else (throw (err/illegal-arg :unexpected-config-option
                                      {::err/message (format "Unexpected config option %s" (pr-str el))}))))))

(defn prep-system
  ([opts] (prep-system opts nil))

  ([opts prep-ks]
   (let [root-opts (->> (cond-> opts (not (vector? opts)) vector)
                        (map load-opts)
                        reverse
                        (reduce spread-opts {})
                        (into {} (map (juxt (comp vector keyword key) val))))]
     (loop [opts (cond-> root-opts
                   (some? prep-ks) (select-keys (map vector prep-ks)))
            res {}]
       (if-let [[k-path dep-seq] (first opts)]
         (let [{:keys [module deps]} (-> dep-seq
                                         (with-default-module (last k-path))
                                         (->> (reduce (opts-reducer k-path) {})))]
           (when-not module
             (throw (err/illegal-arg :missing-module
                                     {::err/message (str "Missing module at " (pr-str k-path))})))

           (recur (merge (dissoc opts k-path)
                         deps
                         (when-let [ref (:ref module)]
                           (when (not (contains? res ref))
                             {ref (get root-opts ref [])})))
                  (cond-> res
                    module (assoc k-path module))))

         res)))))

(defrecord StartedSystem []
  Closeable
  (close [this]
    ((::close-fn (meta this)))))

(defmethod print-method StartedSystem [it ^Writer w]
  (.write w (format "#<System %s>" (set (keys it)))))

(defmethod pp/simple-dispatch StartedSystem [it]
  (print-method it *out*))

(defn ->dep-graph [prepped-system]
  (reduce-kv (fn [g k {:keys [before refs]}]
               (as-> g g
                 (dep/depend g ::system k)
                 (reduce (fn [g b]
                           (dep/depend g b k))
                         g
                         before)
                 (reduce (fn [g r]
                           (dep/depend g k r))
                         g
                         (vals refs))))
             (dep/graph)
             prepped-system))

(defn start-system ^java.io.Closeable [prepped-system]
  (letfn [(ref? [k]
            (boolean (get-in prepped-system [k :ref])))]
    (let [start-order (->> (->dep-graph prepped-system)
                           dep/topo-sort
                           (remove #{::system}))
          system (reduce (fn [system k]
                           (let [{:keys [start-fn refs opts]} (get prepped-system k)]
                             (try
                               (when (Thread/interrupted)
                                 (throw (InterruptedException.)))

                               (log/debug "Starting" k)
                               (-> system
                                   (assoc k (start-fn (-> (reduce-kv (fn [acc k r]
                                                                       (assoc acc k (get system r)))
                                                                     (or opts {})
                                                                     refs)
                                                          (vary-meta assoc ::module-key k)))))
                               (catch Throwable e
                                 (->> (reverse (take-while (complement #{k}) start-order))
                                      (remove ref?)
                                      (run! (comp cio/try-close system)))
                                 (throw (ex-info "Error starting system" {:k k} e))))))
                         {}
                         start-order)]
      (-> (map->StartedSystem (->> system
                                   (into {} (keep (fn [[k v]]
                                                    (when (= 1 (count k))
                                                      [(first k) v]))))))
          (vary-meta assoc ::close-fn (fn []
                                        (->> (reverse start-order)
                                             (remove ref?)
                                             (run! (comp cio/try-close system)))))))))

;;;; specs/conformers

(s/def ::boolean
  (s/and (s/conformer (fn [x] (cond (boolean? x) x, (string? x) (Boolean/parseBoolean x), :else x)))
         boolean?))

(s/def ::string string?)

(s/def ::keyword
  (s/and (s/conformer (fn [x] (cond (keyword? x) x, (string? x) (keyword x), :else x)))
         keyword?))

(s/def ::path
  (s/and (s/conformer (fn [fp]
                        (cond
                          (instance? Path fp) fp
                          (instance? File fp) (.toPath ^File fp)
                          (uri? fp) (Paths/get ^URI fp)
                          (string? fp) (let [uri (URI. fp)]
                                         (if (.getScheme uri)
                                           (Paths/get uri)
                                           (Paths/get fp (make-array String 0)))))))
         #(instance? Path %)))

(s/def ::int
  (s/and (s/conformer (fn [x] (cond (int? x) x, (string? x) (Integer/parseInt x), :else x)))
         int?))

(s/def ::nat-int (s/and ::int nat-int?))
(s/def ::pos-int (s/and ::int pos-int?))

(s/def ::double
  (s/and (s/conformer (fn [x] (cond (float? x) x, (string? x) (Double/parseDouble x), :else x)))
         float?))

(s/def ::pos-double (s/and ::double pos?))

(s/def ::string-map (s/map-of string? string?))
(s/def ::string-list (s/coll-of string?))

(s/def ::duration
  (s/and (s/conformer (fn [d]
                        (cond
                          (instance? Duration d) d
                          (nat-int? d) (Duration/ofMillis d)
                          (string? d) (Duration/parse d))))
         #(instance? Duration %)))

(s/def ::time-unit
  (s/and (s/conformer (fn [t]
                        (cond
                          (instance? TimeUnit t) t
                          (string? t) (TimeUnit/valueOf (str/upper-case t)))))
         #(instance? TimeUnit %)))
