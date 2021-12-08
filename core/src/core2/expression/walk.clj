(ns core2.expression.walk
  (:import clojure.lang.MapEntry))

(defmulti direct-child-exprs
  (fn [{:keys [op] :as expr}]
    op)
  :default ::default)

(defmethod direct-child-exprs ::default [_])

(defmethod direct-child-exprs :if [e] (map e [:pred :then :else]))
(defmethod direct-child-exprs :if-some [e] (map e [:expr :then :else]))
(defmethod direct-child-exprs :let [e] (map e [:expr :body]))
(defmethod direct-child-exprs :call [e] (:args e))
(defmethod direct-child-exprs :struct [e] (vals (:entries e)))
(defmethod direct-child-exprs :dot-const-field [e] (map e [:struct-expr]))
(defmethod direct-child-exprs :dot [e] (map e [:struct-expr :field-expr]))
(defmethod direct-child-exprs :list [e] (:elements e))
(defmethod direct-child-exprs :nth-const-idx [e] (map e [:coll-expr]))
(defmethod direct-child-exprs :nth [e] (map e (map e [:coll-expr :idx-expr])))

(defn expr-seq [expr]
  (lazy-seq
   (cons expr (mapcat expr-seq (direct-child-exprs expr)))))

(defmulti walk-expr
  (fn [inner outer {:keys [op] :as expr}]
    op)
  :default ::default)

(defmethod walk-expr ::default [_inner outer expr]
  (outer expr))

(defmethod walk-expr :if [inner outer {:keys [pred then else]}]
  (outer {:op :if, :pred (inner pred), :then (inner then), :else (inner else)}))

(defmethod walk-expr :if-some [inner outer {:keys [local expr then else]}]
  (outer {:op :if-some, :local local, :expr (inner expr)
          :then (inner then), :else (inner else)}))

(defmethod walk-expr :let [inner outer {:keys [local expr body]}]
  (outer {:op :let, :local local, :expr (inner expr), :body (inner body)}))

(defmethod walk-expr :call [inner outer {expr-f :f, :keys [args]}]
  (outer {:op :call, :f expr-f, :args (mapv inner args)}))

(defmethod walk-expr :struct [inner outer {:keys [entries]}]
  (outer {:op :struct
          :entries (->> (for [[k expr] entries]
                          (MapEntry/create k (inner expr)))
                        (into {}))}))

(defmethod walk-expr :dot-const-field [inner outer {:keys [struct-expr field]}]
  (outer {:op :dot-const-field, :struct-expr (inner struct-expr), :field field}))

(defmethod walk-expr :dot [inner outer {:keys [struct-expr field-expr]}]
  (outer {:op :dot, :struct-expr (inner struct-expr), :field-expr (inner field-expr)}))

(defmethod walk-expr :list [inner outer {:keys [elements]}]
  (outer {:op :list, :elements (mapv inner elements)}))

(defmethod walk-expr :nth-const-idx [inner outer {:keys [coll-expr idx]}]
  (outer {:op :nth-const-idx, :coll-expr (inner coll-expr), :idx idx}))

(defmethod walk-expr :nth [inner outer {:keys [coll-expr idx-expr]}]
  (outer {:op :nth, :coll-expr (inner coll-expr), :idx-expr (inner idx-expr)}))

;; from clojure.walk
(defn postwalk-expr [f expr]
  (walk-expr (partial postwalk-expr f) f expr))

(defn prewalk-expr [f expr]
  (walk-expr (partial prewalk-expr f) identity (f expr)))
