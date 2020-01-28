(ns crux.topology
  (:require [clojure.spec.alpha :as s]
            [crux.api :as api]
            [com.stuartsierra.dependency :as dep]
            [crux.io :as cio])
  (:import (java.io Closeable)))

(s/def ::resolvable-id
  (fn [id]
    (and (or (string? id) (keyword? id) (symbol? id))
         (namespace (symbol id)))))

(defn- resolve-id [id]
  (s/assert ::resolvable-id id)
  (-> (or (-> id symbol requiring-resolve)
          (throw (IllegalArgumentException. (format "Can't resolve symbol: '%s'" id))))
      var-get))

(s/def ::start-fn ifn?)
(s/def ::deps (s/coll-of keyword?))

(s/def ::args
  (s/map-of keyword?
            (s/keys :req [:crux.config/type]
                    :req-un [:crux.config/doc]
                    :opt-un [:crux.config/default
                             :crux.config/required?])))

(s/def ::component
  (s/and (s/or :component-id ::resolvable-id, :component map?)
         (s/conformer (fn [[c-or-id s]]
                        (cond-> s (= :component-id c-or-id) resolve-id)))
         (s/keys :req-un [::start-fn]
                 :opt-un [::deps ::args])))

(s/def ::module
  (s/and (s/or :module-id ::resolvable-id, :module map?)
         (s/conformer (fn [[m-or-id s]]
                        (cond-> s (= :module-id m-or-id) resolve-id)))))

(s/def ::resolved-topology (s/map-of keyword? ::component))

(defn options->topology [{:keys [crux.node/topology] :as options}]
  (when-not topology
    (throw (IllegalArgumentException. "Please specify :crux.node/topology")))

  (let [topology (-> topology
                     (cond-> (not (vector? topology)) vector)
                     (->> (map #(s/conform ::module %))
                          (apply merge)))]
    (->> (merge topology (select-keys options (keys topology)))
         (s/conform ::resolved-topology))))

(defn- start-order [system]
  (let [g (reduce-kv (fn [g k c]
                       (let [c (s/conform ::component c)]
                         (reduce (fn [g d] (dep/depend g k d)) g (:deps c))))
                     (dep/graph)
                     system)
        dep-order (dep/topo-sort g)
        dep-order (->> (keys system)
                       (remove #(contains? (set dep-order) %))
                       (into dep-order))]
    dep-order))

(defn parse-opts [args options]
  (into {}
        (for [[k {:keys [crux.config/type default required?]}] args]
          (let [[validate-fn parse-fn] (s/conform :crux.config/type type)
                v (some-> (get options k) parse-fn)
                v (if (nil? v) default v)]

            (when (and required? (not v))
              (throw (IllegalArgumentException. (format "Arg %s required" k))))

            (when (and v (not (validate-fn v)))
              (throw (IllegalArgumentException. (format "Arg %s invalid" k))))

            [k v]))))

(defn start-component [c started options]
  (s/assert ::component c)
  (let [{:keys [start-fn deps spec args]} (s/conform ::component c)
        deps (select-keys started deps)
        options (merge options (parse-opts args options))]
    (start-fn deps options)))

(defn- close-topology [started-order]
  (->> (reverse started-order)
       (filter #(instance? Closeable %))
       (run! cio/try-close)))

(defn start-topology [options]
  (let [options (into {} options)
        topology (options->topology options)]
    (s/assert ::resolved-topology topology)
    (let [started-order (atom [])
          started (atom {})
          started-modules (try
                            (into {}
                                  (for [k (start-order topology)]
                                    (let [c (or (get topology k)
                                                (throw (IllegalArgumentException. (str "Could not find component " k))))
                                          c (start-component c @started options)]
                                      (swap! started-order conj c)
                                      (swap! started assoc k c)
                                      [k c])))
                            (catch Throwable t
                              (close-topology @started-order)
                              (throw t)))]
      [started-modules (fn []
                         (close-topology @started-order))])))
