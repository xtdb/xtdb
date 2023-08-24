(ns xtdb.xray
  "Utility for interactive development when working with opaque mutable object graphs."
  (:require [clojure.reflect :as reflect])
  (:import (java.lang.reflect InaccessibleObjectException)))

(def ^:private state
  (atom {:oids {}, :objs {}, :next-id 0, :hide #{}}))

(deftype IDBox [x]
  Object
  (equals [_ o] (and (instance? IDBox o) (identical? x (.-x ^IDBox o))))
  (hashCode [_] (System/identityHashCode x)))

(defn id [o] (get-in @state [:oids (->IDBox o)]))

(defn obj [oid] (get-in @state [:objs oid]))

(defn remember [o]
  (->> (fn [{:keys [oids objs next-id] :as st}]
         (let [id-box (->IDBox o)]
           (if (contains? oids id-box)
             st
             (assoc st :oids (assoc oids id-box next-id)
                       :objs (assoc objs next-id o)
                       :next-id (inc next-id)))))
       (swap! state)
       ((fn [st] (get-in st [:oids (->IDBox o)])))))

(defn clear
  "Clears any remembered objects, does not clear the id ctr or hide instructions, use (reset) for that."
  []
  (swap! state assoc :oids {}, :objs {})
  nil)

(defn reset
  "Resets the registry to its initial state, removing remembered objects, resetting the id counter and hide/show instructions."
  []
  (swap! state assoc :oids {}, :objs {}, :next-id 0, :hide #{})
  nil)

(defn toggle-vis [t-or-f & things]
  (doseq [o things
          :let [entry (cond
                        (class? o) o

                        (and (number? o) (obj o))
                        (->IDBox (obj o))

                        :else (->IDBox o))]]
    (swap! state update :hide (if t-or-f disj conj) entry))
  nil)

(def show
  "Toggle the visibility of provided classes or object instances on (if previously hidden with hide)"
  (partial toggle-vis true))

(defn show-all []
  (swap! state assoc :hide #{})
  nil)

(def hide
  "Toggle visibility of provided classes or object instances off, use (show) to toggle back on or (show-all) to remove all hide instructions."
  (partial toggle-vis false))

(defn- hide? [o]
  (or (contains? (:hide @state) (->IDBox o)) (contains? (:hide @state) (class o))))

(defn xray
  "Walks an object with reflection and returns its field state recursively, objects are saved to a registry on walk for later addressing with (obj id).

  Usage: (xray (let [foo 42] (fn [] foo))) => {:_ref (xtdb.xray/obj 0), :_t xtdb.xray$eval36931$fn__36932, :foo 42}\n

  See also:
  - (clear), (reset) to clear registry state and enable memory to be freed.
  - (hide), (show) to opt instances or classes out of xray'ing

  ---

  Transformation Rules:

  Objects will be returned as maps of their public and private field state
  with a couple of extra entries:

  :_ref a clojure form that when evaluated will return the instance of the object from the registry
  :_t the java Class of the object

  Map rules:

  - All maps are returned sorted if all keys are keywords
  - Clojure maps do not get a :_ref or :_t
  - Both java.util.Map gets converted to a clojure map and has a _ref and a _t keyword added
    for the original (probably mutable) map

  Other rules:

  - Primitives are returned as is
  - Primitive arrays are returned as is
  - hidden objects are returned as is
  - cycles terminate the walk, you get a truncated map with none of the field state
  - seqables (incl object arrays of any component type) are walked lazily, a (lazy) Seq is returned.
  - Whenever an object is transformed, a :xray/obj key can be found on the returned metadata holding the original object"
  [o]
  (let [see (fn [seen o] (conj seen (->IDBox o)))

        map-init (fn [o & kvs] (if (every? keyword? (keys o)) (apply sorted-map kvs) (apply hash-map kvs)))

        walk
        (fn walk [seen o]
          (cond
            (nil? o) o
            (boolean? o) o
            (string? o) o
            (symbol? o) o
            (keyword? o) o
            (number? o) o
            (hide? o) o
            (map? o)
            (-> (into (map-init o) (update-vals o (partial walk (see seen o))))
                (with-meta (assoc (meta o) :xray/obj o)))

            (instance? java.util.Map o)
            (let [oid (remember o)]
              (-> (into (map-init o :_ref (list `obj oid), :_t (class o))
                        (for [[k v] o]
                          [k (walk (see seen o) v)]))
                  (with-meta (assoc (meta o) :xray/obj o))))

            (and (.isArray (class o))
                 (.isPrimitive (.getComponentType (class o)))) o

            (seqable? o) (with-meta (map (partial walk (see seen o)) o) {:xray/obj o})
            :else
            (let [oid (remember o)]
              (if (contains? seen (->IDBox o))
                (-> (sorted-map :_ref (list `obj oid), :_t (class o), :_cycle true)
                    (with-meta {:xray/obj o}))
                (let [{:keys [members]} (reflect/reflect o)]
                  (into (sorted-map :_ref (list `obj oid), :_t (class o))
                        (for [member members
                              :when (and (instance? clojure.reflect.Field member)
                                         (not (:static (:flags member)))
                                         (not= '__meta (:name member)))]
                          [(keyword (name (:name member)))
                           (try
                             (some-> (.getDeclaredField (class o) (str (:name member)))
                                     (doto (.setAccessible true))
                                     (.get o)
                                     (->> (walk (see seen o))))
                             (catch InaccessibleObjectException _ 'inaccessible)
                             (catch Throwable e
                               (with-meta
                                 (sorted-map :_ref (list `obj (remember e))
                                             :_t (class e)
                                             :_ex (.getMessage e)
                                             :_obj o)
                                 {:xray/ex e
                                  :xray/obj o})))])))))))]
    (walk #{} o)))

(comment

  (clear)

  (reset)

  (xray {:foo 42})

  (xray (Object.))

  (xray (let [foo 42] (fn [] foo)))

  )
