(ns core2.expression.hash
  (:import core2.vector.IIndirectVector
           org.apache.arrow.memory.util.hash.MurmurHasher))

(def ^:private ^org.apache.arrow.memory.util.hash.ArrowBufHasher hasher
  (MurmurHasher.))

(definterface IVectorHasher
  (^int hashCode [^int idx]))

(defn ->hasher ^core2.expression.hash.IVectorHasher [^IIndirectVector ivec]
  (let [v (.getVector ivec)]
    (reify IVectorHasher
      (hashCode [_ idx]
        (.hashCode v (.getIndex ivec idx) hasher)))))
