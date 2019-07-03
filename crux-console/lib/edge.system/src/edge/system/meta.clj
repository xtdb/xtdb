(ns edge.system.meta)

(defmulti useful-info (fn [k config state]
                        (if (vector? k)
                          (first k)
                          k)))

(defmethod useful-info :default [_ _ _] nil)

(defn useful-infos
  [config state]
  (filter some?
          (for [k (keys state)]
            (useful-info k (get config k) (get state k)))))
