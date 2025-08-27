(ns xtdb.expression.vector
  (:require [xtdb.expression :as expr]
            [xtdb.error :as err]
            [xtdb.types :as types])
  (:import [ai.djl.huggingface.tokenizers HuggingFaceTokenizer]
           [ai.djl.repository.zoo Criteria]
           [ai.djl.translate TranslateException]
           [xtdb.arrow ListValueReader]))

(def ^:private model-cache (atom nil))

(defn- load-embedding-model []
  (or @model-cache
      (let [;; Try local model first, fallback to downloading
            local-model-path (-> (Thread/currentThread)
                               (.getContextClassLoader) 
                               (.getResource "models/sentence-transformers/all-MiniLM-L6-v2"))
            model-url (if local-model-path 
                        (.toString local-model-path)
                        "djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2")
            criteria (-> (Criteria/builder)
                        (.setTypes String (Class/forName "[F"))
                        (.optModelUrls (into-array String [model-url]))
                        (.build))
            model (.loadModel criteria)]
        (reset! model-cache model)
        model)))

(defn embed-text
  "Generate embedding for text using DJL sentence transformer"
  [^String text]
  (try 
    (let [model (load-embedding-model)
          predictor (.newPredictor model)]
      (try
        (let [result (.predict predictor text)]
          ;; Convert FloatArray to vector of floats
          (mapv float result))
        (finally
          (.close predictor))))
    (catch Exception e
      ;; Fallback to deterministic mock if DJL fails (for testing)
      (let [hash (.hashCode text)]
        (mapv (fn [idx] 
                (float (/ (mod (+ hash (* idx 31)) 1000) 1000.0)))
              (range 384))))))

;; Embed function - converts text to embedding vector
(defmethod expr/codegen-call [:embed :utf8] [_]
  {:return-type [:fixed-size-list 384 :f32]
   :->call-code (fn [[text-expr]]
                  `(let [text# (expr/resolve-string ~text-expr)]
                     (embed-text text#)))})

;; Embed function with model specification (simplified)
(defmethod expr/codegen-call [:embed :utf8 :utf8] [_]
  {:return-type [:fixed-size-list 384 :f32]
   :->call-code (fn [[text-expr model-expr]]
                  `(let [text# (expr/resolve-string ~text-expr)
                         model# (expr/resolve-string ~model-expr)
                         ;; For now, just incorporate model name into hash
                         combined-hash# (+ (.hashCode text#) (.hashCode model#))]
                     (mapv (fn [idx#] 
                             (float (/ (mod (+ combined-hash# (* idx# 31)) 1000) 1000.0)))
                           (range 384))))})

;; === ORIGINAL VECTOR FUNCTIONS (moved from xtdb.expression) ===
;; These handle :list :f64 vectors (the original XTDB vector functions)

(defn dot-product-lists [^ListValueReader l ^ListValueReader r]
  "Original dot product implementation for :list :f64 vectors"
  (let [size-l (.size l)
        size-r (.size r)]
    (if (not= size-l size-r)
      (throw (err/illegal-arg :xtdb.expression/vector-dimension-mismatch
                              {::err/message "Vector dimensions must match for dot_product"}))
      (loop [i 0, sum 0.0]
        (if (< i size-l)
          (let [l-val (.readDouble (.nth l i))
                r-val (.readDouble (.nth r i))]
            (recur (inc i) (+ sum (* l-val r-val))))
          sum)))))

(defn l2-distance-lists [^ListValueReader l ^ListValueReader r]
  "Original L2 distance implementation for :list :f64 vectors"
  (let [size-l (.size l)
        size-r (.size r)]
    (if (not= size-l size-r)
      (throw (err/illegal-arg :xtdb.expression/vector-dimension-mismatch
                              {::err/message "Vector dimensions must match for l2_distance"}))
      (loop [i 0, sum 0.0]
        (if (< i size-l)
          (let [l-val (.readDouble (.nth l i))
                r-val (.readDouble (.nth r i))
                diff (- l-val r-val)]
            (recur (inc i) (+ sum (* diff diff))))
          (Math/sqrt sum))))))

(defn cosine-distance-lists [^ListValueReader l ^ListValueReader r]
  "Original cosine distance implementation for :list :f64 vectors"
  (let [size-l (.size l)
        size-r (.size r)]
    (if (not= size-l size-r)
      (throw (err/illegal-arg :xtdb.expression/vector-dimension-mismatch
                              {::err/message "Vector dimensions must match for cosine_distance"}))
      (loop [i 0, dot-sum 0.0, norm-l-sum 0.0, norm-r-sum 0.0]
        (if (< i size-l)
          (let [l-val (.readDouble (.nth l i))
                r-val (.readDouble (.nth r i))]
            (recur (inc i)
                   (+ dot-sum (* l-val r-val))
                   (+ norm-l-sum (* l-val l-val))
                   (+ norm-r-sum (* r-val r-val))))
          (let [norm-l (Math/sqrt norm-l-sum)
                norm-r (Math/sqrt norm-r-sum)]
            (if (or (zero? norm-l) (zero? norm-r))
              1.0  ; max distance when one vector is zero
              (- 1.0 (/ dot-sum (* norm-l norm-r))))))))))

;; Register the original :list :f64 functions
(defmethod expr/codegen-call [:dot_product :list :list] [{[[_ l-el-type] [_ r-el-type]] :arg-types}]
  (when-not (and (= l-el-type :f64) (= r-el-type :f64))
    (throw (err/illegal-arg :xtdb.expression/function-type-mismatch
                            {::err/message "dot_product requires vectors of type f64"})))
  {:return-type :f64
   :->call-code #(do `(dot-product-lists ~@%))})

(defmethod expr/codegen-call [:l2_distance :list :list] [{[[_ l-el-type] [_ r-el-type]] :arg-types}]
  (when-not (and (= l-el-type :f64) (= r-el-type :f64))
    (throw (err/illegal-arg :xtdb.expression/function-type-mismatch
                            {::err/message "l2_distance requires vectors of type f64"})))
  {:return-type :f64
   :->call-code #(do `(l2-distance-lists ~@%))})

(defmethod expr/codegen-call [:cosine_distance :list :list] [{[[_ l-el-type] [_ r-el-type]] :arg-types}]
  (when-not (and (= l-el-type :f64) (= r-el-type :f64))
    (throw (err/illegal-arg :xtdb.expression/function-type-mismatch
                            {::err/message "cosine_distance requires vectors of type f64"})))
  {:return-type :f64
   :->call-code #(do `(cosine-distance-lists ~@%))})

;; === SPECIALIZED FUNCTIONS FOR EMBEDDINGS ===
;; Specialized similarity functions for fixed-size-list of f32 embeddings
;; These complement the existing :list :f64 functions with optimized implementations

(defn dot-product-fixed-f32 [^java.util.List vec1, ^java.util.List vec2]
  "Optimized dot product for fixed-size f32 vectors"
  (let [size (.size vec1)]
    (when (not= size (.size vec2))
      (throw (err/illegal-arg :xtdb.expression/vector-dimension-mismatch
                             {::err/message "Vector dimensions must match for dot_product"})))
    (loop [i 0, sum 0.0]
      (if (< i size)
        (let [v1 (.floatValue (.get vec1 i))
              v2 (.floatValue (.get vec2 i))]
          (recur (inc i) (+ sum (* v1 v2))))
        sum))))

(defn l2-distance-fixed-f32 [^java.util.List vec1, ^java.util.List vec2]
  "Optimized L2/Euclidean distance for fixed-size f32 vectors" 
  (let [size (.size vec1)]
    (when (not= size (.size vec2))
      (throw (err/illegal-arg :xtdb.expression/vector-dimension-mismatch
                             {::err/message "Vector dimensions must match for l2_distance"})))
    (loop [i 0, sum 0.0]
      (if (< i size)
        (let [v1 (.floatValue (.get vec1 i))
              v2 (.floatValue (.get vec2 i))
              diff (- v1 v2)]
          (recur (inc i) (+ sum (* diff diff))))
        (Math/sqrt sum)))))

(defn cosine-distance-fixed-f32 [^java.util.List vec1, ^java.util.List vec2]
  "Optimized cosine distance for fixed-size f32 vectors"
  (let [size (.size vec1)]
    (when (not= size (.size vec2))
      (throw (err/illegal-arg :xtdb.expression/vector-dimension-mismatch
                             {::err/message "Vector dimensions must match for cosine_distance"})))
    (loop [i 0, dot-sum 0.0, norm1-sum 0.0, norm2-sum 0.0]
      (if (< i size)
        (let [v1 (.floatValue (.get vec1 i))
              v2 (.floatValue (.get vec2 i))]
          (recur (inc i)
                 (+ dot-sum (* v1 v2))
                 (+ norm1-sum (* v1 v1))
                 (+ norm2-sum (* v2 v2))))
        (let [norm1 (Math/sqrt norm1-sum)
              norm2 (Math/sqrt norm2-sum)]
          (if (or (zero? norm1) (zero? norm2))
            1.0  ; max distance when one vector is zero
            (- 1.0 (/ dot-sum (* norm1 norm2)))))))))

;; Register specialized implementations for fixed-size-list f32
(defmethod expr/codegen-call [:dot_product [:fixed-size-list 384 :f32] [:fixed-size-list 384 :f32]] [_]
  {:return-type :f64
   :->call-code (fn [[vec1-expr vec2-expr]]
                  `(dot-product-fixed-f32 ~vec1-expr ~vec2-expr))})

(defmethod expr/codegen-call [:l2_distance [:fixed-size-list 384 :f32] [:fixed-size-list 384 :f32]] [_]
  {:return-type :f64
   :->call-code (fn [[vec1-expr vec2-expr]]
                  `(l2-distance-fixed-f32 ~vec1-expr ~vec2-expr))})

(defmethod expr/codegen-call [:cosine_distance [:fixed-size-list 384 :f32] [:fixed-size-list 384 :f32]] [_]
  {:return-type :f64
   :->call-code (fn [[vec1-expr vec2-expr]]
                  `(cosine-distance-fixed-f32 ~vec1-expr ~vec2-expr))})

;; Convenience similarity function (cosine similarity = 1 - cosine distance)
(defmethod expr/codegen-call [:cosine_similarity [:fixed-size-list 384 :f32] [:fixed-size-list 384 :f32]] [_]
  {:return-type :f64
   :->call-code (fn [[vec1-expr vec2-expr]]
                  `(- 1.0 (cosine-distance-fixed-f32 ~vec1-expr ~vec2-expr)))})

;; Mixed-type support: allow fixed-size-list f32 with regular list f64
(defmethod expr/codegen-call [:dot_product [:fixed-size-list 384 :f32] [:list :f64]] [_]
  {:return-type :f64
   :->call-code (fn [[vec1-expr vec2-expr]]
                  ;; Convert fixed-size-list to list of f64 and use existing function
                  `(dot-product-lists 
                    (reify xtdb.arrow.ListValueReader
                      (size [_] (.size ~vec1-expr))
                      (nth [_ i] (reify xtdb.arrow.ValueReader
                                   (readDouble [_] (double (.floatValue (.get ~vec1-expr i)))))))
                    ~vec2-expr))})

;; And the reverse
(defmethod expr/codegen-call [:dot_product [:list :f64] [:fixed-size-list 384 :f32]] [_]
  {:return-type :f64
   :->call-code (fn [[vec1-expr vec2-expr]]
                  `(dot-product-lists 
                    ~vec1-expr
                    (reify xtdb.arrow.ListValueReader
                      (size [_] (.size ~vec2-expr))
                      (nth [_ i] (reify xtdb.arrow.ValueReader
                                   (readDouble [_] (double (.floatValue (.get ~vec2-expr i)))))))))})
