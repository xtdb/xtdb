(ns xtdb.xtql.json
  (:require [xtdb.error :as err])
  (:import [java.time Duration LocalDate LocalDateTime ZonedDateTime]
           (xtdb.query Expr Expr$Bool Expr$Call Expr$Double Expr$LogicVar Expr$Long Expr$Obj)))

(defprotocol Unparse
  (unparse [this]))

(declare parse-expr)

(defn- parse-literal [{v "@value", t "@type" :as l}]
  (letfn [(bad-literal [l]
            (throw (err/illegal-arg :xtql/malformed-literal {:literal l})))

          (try-parse [v f]
            (try
              (f v)
              (catch Exception e
                (throw (err/illegal-arg :xtql/malformed-literal {:literal l, :error (.getMessage e)})))))]
    (cond
      (nil? v) (Expr/val nil)

      (nil? t) (cond
                 (int? v) (Expr/val (long v))
                 (double? v) (Expr/val (double v))
                 (boolean? v) (if v Expr/TRUE Expr/FALSE)
                 (map? v) (Expr/val (into {} (map (juxt key (comp parse-expr val))) v))
                 (vector? v) (Expr/val (mapv parse-expr v))
                 (string? v) (Expr/val v)
                 :else (bad-literal l))

      :else (if (= "xt:set" t)
              (if-not (vector? v)
                (bad-literal l)

                (Expr/val (into #{} (map parse-expr) v)))

              (if-not (string? v)
                (bad-literal l)

                (case t
                  "xt:keyword" (Expr/val (keyword v))
                  "xt:date" (Expr/val (try-parse v #(LocalDate/parse %)))
                  "xt:duration" (Expr/val (try-parse v #(Duration/parse %)))
                  "xt:timestamp" (Expr/val (try-parse v #(LocalDateTime/parse %)))
                  "xt:timestamptz" (Expr/val (try-parse v #(ZonedDateTime/parse %)))
                  (throw (err/illegal-arg :xtql/unknown-type {:value v, :type t}))))))))

(defn parse-expr [expr]
  (letfn [(bad-expr [expr]
            (throw (err/illegal-arg :xtql/malformed-expr {:expr expr})))]
    (cond
      (string? expr) (Expr/lVar expr)
      (vector? expr) (parse-literal {"@value" expr})

      (map? expr) (cond
                    (contains? expr "@value") (parse-literal expr)

                    (= 1 (count expr)) (let [[f args] (first expr)]
                                         (when-not (vector? args)
                                           (bad-expr expr))
                                         (Expr/call f (mapv parse-expr args)))

                    :else (bad-expr expr))

      :else (bad-expr expr))))

(extend-protocol Unparse
  Expr$LogicVar (unparse [lv] (.lv lv))
  Expr$Bool (unparse [b] {"@value" (.bool b)})
  Expr$Long (unparse [l] {"@value" (.lng l)})
  Expr$Double (unparse [d] {"@value" (.dbl d)})

  Expr$Call (unparse [c] {(.f c) (mapv unparse (.args c))})

  Expr$Obj
  (unparse [obj]
    (let [obj (.obj obj)]
      (cond
        (nil? obj) nil
        (vector? obj) (mapv unparse obj)
        (string? obj) {"@value" obj}
        (keyword? obj) {"@value" (str (symbol obj)), "@type" "xt:keyword"}
        (set? obj) {"@value" (mapv unparse obj), "@type" "xt:set"}
        (instance? LocalDate obj) {"@value" (str obj), "@type" "xt:date"}
        (instance? Duration obj) {"@value" (str obj), "@type" "xt:duration"}
        (instance? LocalDateTime obj) {"@value" (str obj), "@type" "xt:timestamp"}
        (instance? ZonedDateTime obj) {"@value" (str obj), "@type" "xt:timestamptz"}
        :else (throw (UnsupportedOperationException. (format "obj: %s" (pr-str obj))))))))
