(ns crux.moberg.types
  (:import java.util.Date))

(deftype CompactKVsAndKey [kvs key])

(deftype SentMessage [^Date time ^long id topic])
