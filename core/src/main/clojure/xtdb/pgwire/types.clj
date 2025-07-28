(ns xtdb.pgwire.types
  (:require [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.error :as err]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import [java.io ByteArrayInputStream]
           [java.net URI]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.time Duration Instant LocalDate LocalDateTime LocalTime OffsetDateTime Period ZoneId ZoneOffset ZonedDateTime]
           [java.time.format DateTimeFormatter DateTimeFormatterBuilder]
           (java.time.temporal ChronoField)
           [java.util Arrays List Map Set UUID]
           (org.apache.arrow.vector.types.pojo Field)
           [org.apache.commons.codec.binary Hex]
           xtdb.arrow.VectorReader
           (xtdb.error Anomaly)
           xtdb.JsonSerde
           (xtdb.pg.codec NumericBin)
           xtdb.table.TableRef
           [xtdb.time Interval Time]
           (xtdb.types ZonedDateTimeRange)))

(defn- json-clj
  "This function is temporary, the long term goal will be to walk arrow directly to generate the json (and later monomorphic
  results).

  Returns a clojure representation of the value, which - when printed as json will present itself as the desired json type string."
  [obj]

  ;; we do not extend any jackson/data.json/whatever because we want intentional printing of supported types and
  ;; text representation (e.g consistent scientific notation, dates and times, etc).

  ;; we can reduce the cost of this walk later by working directly on typed arrow vectors rather than dynamic clojure maps!
  ;; we lean on data.json for now for encoding, quote/escape, json compat floats etc

  (cond
    (nil? obj) nil
    (boolean? obj) obj
    (int? obj) obj
    (decimal? obj) obj

    ;; I am allowing string pass through for now but be aware data.json escapes unicode and that may not be
    ;; what we want at some point (e.g pass plain utf8 unless prompted as a param).
    (string? obj) obj

    ;; bigdec cast gets us consistent exponent notation E with a sign for doubles, floats and bigdecs.
    (float? obj) (bigdec obj)

    ;; java.time datetime-ish toString is already iso8601, we may want to tweak this later
    ;; as the string format often omits redundant components (like trailing seconds) which may make parsing harder
    ;; for clients, doesn't matter for now - json probably gonna die anyway
    (instance? LocalTime obj) (str obj)
    (instance? LocalDate obj) (str obj)
    (instance? LocalDateTime obj) (str obj)
    (instance? OffsetDateTime obj) (str obj)
    ;; print offset instead of zoneprefix  otherwise printed representation may change depending on client
    ;; we might later revisit this if json printing remains
    (instance? ZonedDateTime obj) (recur (.toOffsetDateTime ^ZonedDateTime obj))
    (instance? Instant obj) (recur (.atZone ^Instant obj #xt/zone "UTC"))
    (instance? Duration obj) (str obj)
    (instance? Period obj) (str obj)
    (instance? Interval obj) (str obj)

    ;; returned to handle big utf8 bufs, right now we do not handle this well, later we will be writing json with less
    ;; copies and we may encode the quoted json string straight out of the buffer
    (instance? org.apache.arrow.vector.util.Text obj) (str obj)

    (or (instance? List obj) (instance? Set obj))
    (mapv json-clj obj)

    ;; maps, cannot be created from SQL yet, but working playground requires them
    ;; we are not dealing with the possibility of non kw/string keys, xt shouldn't return maps like that right now.
    (instance? Map obj) (update-vals obj json-clj)

    (instance? Anomaly obj)
    (json-clj (-> (ex-data obj)
                  (assoc :message (ex-message obj))))

    (instance? clojure.lang.Keyword obj) (json-clj (str (symbol obj)))
    (instance? clojure.lang.Symbol obj) (json-clj (str (symbol obj)))
    (instance? UUID obj) (json-clj (str obj))
    (instance? ByteBuffer obj) (json-clj (str "0x" (Hex/encodeHexString (util/byte-buffer->byte-array obj))))
    (instance? URI obj) (json-clj (str obj))

    (instance? TableRef obj) (let [^TableRef table obj]
                               (json-clj {:db-name (.getDbName table)
                                          :schema-name (.getSchemaName table)
                                          :table-name (.getTableName table)}))

    :else
    (throw (err/unsupported ::unknown-type (format "Unexpected type encountered by pgwire (%s)" (class obj))))))


(defn- read-utf8 [^bytes barr] (String. barr StandardCharsets/UTF_8))

(defn utf8
  "Returns the utf8 byte-array for the given string"
  ^bytes [s]
  (.getBytes (str s) StandardCharsets/UTF_8))

(def json-bytes (comp utf8 json/json-str json-clj))

(defn write-json [_env ^VectorReader rdr idx]
  (json-bytes (.getObject rdr idx)))

(def iso-local-date-time-formatter-with-space
  (-> (DateTimeFormatterBuilder.)
      (.parseCaseInsensitive)
      (.append DateTimeFormatter/ISO_LOCAL_DATE)
      (.appendLiteral " ")
      (.appendValue ChronoField/HOUR_OF_DAY 2)
      (.appendLiteral ":")
      (.appendValue ChronoField/MINUTE_OF_HOUR 2)
      (.optionalStart)
      (.appendLiteral ":")
      (.appendValue ChronoField/SECOND_OF_MINUTE 2)
      (.optionalStart)
      (.appendFraction ChronoField/MICRO_OF_SECOND, 0, 6, true)
      (.parseStrict)
      (.toFormatter)))

(def iso-offset-date-time-formatter-with-space
  (-> (DateTimeFormatterBuilder.)
      (.parseCaseInsensitive)
      (.append iso-local-date-time-formatter-with-space)
      (.parseLenient)
      (.optionalStart)
      (.appendOffset "+HH:MM:ss", "+00:00")
      (.optionalEnd)
      (.parseStrict)
      (.toFormatter)))

(def ^LocalDate pg-epoch #xt/date "2000-01-01")

;;10957
(def ^{:tag 'int} unix-pg-epoch-diff-in-days (int (.toEpochDay pg-epoch)))

;; 946684800000000
(def ^{:tag 'long} unix-pg-epoch-diff-in-micros (* (.toEpochMilli (.toInstant (.atStartOfDay pg-epoch) ZoneOffset/UTC)) 1000))

(def ^:const ^long transit-oid 16384)

(def pg-ranges
  {:tzts-range
   {:rngtypid 3910
    :rngsubtype 1184
    :rngmultitypid 4534
    :rngcollation 0
    :rngsubopc 3127
    :rngcanonical ""
    :rngsubdiff "tstzrange_subdiff"}})

(defn calculate-buffer-size
  "list: <<byte-length,dimensions=1,data-offset=0,oid,elem-count=(lenght-of-list),l-bound=1?,data>>
   https://github.com/postgres/postgres/blob/master/src/include/utils/array.h
   except XT wire protocol adds the length of the payload as the first int32 - so we no longer need that
   but need to allocate extra 4 bytes
  "
  ^long [^long length ^long typlen]
  (+ 4 (* 4 4) (* typlen length) (* 4 length)))

(defn binary-list-header [^long length element-oid ^long typlen]
  (let [buffer-size (calculate-buffer-size length typlen)] ;; 4 integers describing array + [data - each prefixed by their length as int32]
    (doto
     (ByteBuffer/allocate buffer-size)
      (.putInt 1) ;; dimensions
      (.putInt 0) ;; data offset
      (.putInt element-oid)
      (.putInt length)
      ;; l-bound - MUST BE 1
      (.putInt 1))))

(defn binary-variable-size-array-header
  [^long element-count oid ^long length-of-elements]
  (doto
   (ByteBuffer/allocate (+ 20 (* 4 element-count) length-of-elements))
    (.putInt 1) ;; dimensions
    (.putInt 0) ;; data offset
    (.putInt oid)
    (.putInt element-count)
    ;; l-bounds - MUST BE 1
    (.putInt 1)))

(defn read-binary-int-array [ba]
  (let [^ByteBuffer bb (ByteBuffer/wrap ba)
        data-offset (long (.getInt bb 4))
        elem-oid (.getInt bb 8)
        length (.getInt bb 12)
        start (long (+ 24 data-offset)) ;; 24 - header = 20 bytes + length of element = 4 bytes
        [^long typlen get-elem] (case elem-oid
                                  23 [4 (fn [^ByteBuffer bb ^long start]
                                          (.getInt bb start))]
                                  20 [8 (fn [^ByteBuffer bb ^long start]
                                          (.getLong bb start))])]
    (loop [arr []
           start start
           idx 0]
      (if (< idx length)
        (recur (conj arr (get-elem bb start))
               (+ start 4 typlen)
               (inc idx))
        arr))))

(defn read-binary-text-array
  "This seems to not have length as the first int32 - that's what JDBC connection/createArrayOf does"
  [^bytes ba]
  (let [^ByteBuffer bb (ByteBuffer/wrap ba)
        data-offset (long (.getInt bb 4))
        length (.getInt bb 12)
        start (long (+ 20 data-offset))]
    (loop [elem 0
           start start
           arr []]
      (if (< elem length)
        (let [str-len (long (.getInt bb start))
              string (read-utf8 (Arrays/copyOfRange ba
                                                    (int (+ 4 start))
                                                    (int (+ 4 start str-len))))]
          (recur (inc elem)
                 (+ 4 start str-len)
                 (conj arr string)))
        arr))))

(defn get-unix-micros ^long [^VectorReader rdr idx]
  (let [[_ unit] (types/field->col-type (.getField rdr))]
    (cond-> (.getLong rdr idx)
      (= :nano unit) (quot 1000))))

(defn interval-rdr->iso-micro-interval-str-bytes ^bytes [^VectorReader rdr idx]
  (let [itvl (.getObject rdr idx)]
    (-> (cond
          (instance? Interval itvl)
          ;; Postgres only has month-day-micro intervals so we truncate the nanos
          (let [^Interval itvl itvl]
            (Interval. (.getMonths itvl)
                       (.getDays itvl)
                       (-> (.getNanos itvl) (quot 1000) (* 1000))))

          :else (throw (IllegalArgumentException. (format "Unsupported interval type: %s" itvl))))

        ;; we use the standard toString for encoding
        (utf8))))

(def pg-types
  (-> {
       ;;default oid is currently only used to describe a parameter without a known type
       ;;these are not supported in DML and for queries are defaulted to text by the backend
       :default {:typname "default"
                 :col-type :utf8
                 :oid 0
                 :read-text (fn [_env ba] (read-utf8 ba))
                 :read-binary (fn [_env ba] (read-utf8 ba))}

       :int8 (let [typlen 8]
               {:typname "int8"
                :col-type :i64
                :oid 20
                :typlen typlen
                :typcategory :numeric
                :typarray :_int8
                :typsend "int8send"
                :typreceive "int8recv"
                :read-binary (fn [_env ba] (-> ba ByteBuffer/wrap .getLong))
                :read-text (fn [_env ba] (-> ba read-utf8 Long/parseLong))
                :write-binary (fn [_env ^VectorReader rdr idx]
                                (let [bb (doto (ByteBuffer/allocate typlen)
                                           (.putLong (.getLong rdr idx)))]
                                  (.array bb)))
                :write-text (fn [_env ^VectorReader rdr idx]
                              (utf8 (.getLong rdr idx)))})
       :int4 (let [typlen 4]
               {:typname "int4"
                :col-type :i32
                :oid 23
                :typlen typlen
                :typcategory :numeric
                :typarray :_int4
                :typsend "int4send"
                :typreceive "int4recv"
                :read-binary (fn [_env ba] (-> ba ByteBuffer/wrap .getInt))
                :read-text (fn [_env ba] (-> ba read-utf8 Integer/parseInt))
                :write-binary (fn [_env ^VectorReader rdr idx]
                                (let [bb (doto (ByteBuffer/allocate typlen)
                                           (.putInt (.getInt rdr idx)))]
                                  (.array bb)))
                :write-text (fn [_env ^VectorReader rdr idx]
                              (utf8 (.getInt rdr idx)))})
       :int2 (let [typlen 2]
               {:typname "int2"
                :col-type :i16
                :oid 21
                :typlen typlen
                :typcategory :numeric
                :typsend "int2send"
                :typreceive "int2recv"
                :read-binary (fn [_env ba] (-> ba ByteBuffer/wrap .getShort))
                :read-text (fn [_env ba] (-> ba read-utf8 Short/parseShort))
                :write-binary (fn [_env ^VectorReader rdr idx]
                                (let [bb (doto (ByteBuffer/allocate typlen)
                                           (.putShort (.getShort rdr idx)))]
                                  (.array bb)))
                :write-text (fn [_env ^VectorReader rdr idx]
                              (utf8 (.getShort rdr idx)))})
       ;;
       ;;Java boolean maps to both bit and bool, opting to support latter only for now
       #_#_:bit {:typname "bit"
                 :oid 1500
                 :typlen 1
                 :read-binary (fn [ba] (-> ba ByteBuffer/wrap .get))
                 :read-text (fn [ba] (-> ba read-utf8 Byte/parseByte))
                 :write-binary (fn [^VectorReader rdr idx]
                                 (when-not (.isNull rdr idx)
                                   (let [bb (doto (ByteBuffer/allocate 1)
                                              (.put (.getByte rdr idx)))]
                                     (.array bb))))
                 :write-text (fn [^VectorReader rdr idx]
                               (when-not (.isNull rdr idx)
                                 (utf8 (.getByte rdr idx))))}

       :float4 (let [typlen 4]
                 {:typname "float4"
                  :col-type :f32
                  :oid 700
                  :typlen typlen
                  :typcategory :numeric
                  :typsend "float4send"
                  :typreceive "float4recv"
                  :read-binary (fn [_env ba] (-> ba ByteBuffer/wrap .getFloat))
                  :read-text (fn [_env ba] (-> ba read-utf8 Float/parseFloat))
                  :write-binary (fn [_env ^VectorReader rdr idx]
                                  (let [bb (doto (ByteBuffer/allocate 4)
                                             (.putFloat (.getFloat rdr idx)))]
                                    (.array bb)))
                  :write-text (fn [_env ^VectorReader rdr idx]
                                (utf8 (.getFloat rdr idx)))})
       :float8 (let [typlen 8]
                 {:typname "float8"
                  :col-type :f64
                  :oid 701
                  :typlen typlen
                  :typcategory :numeric
                  :typsend "float8send"
                  :typreceive "float8recv"
                  :read-binary (fn [_env ba] (-> ba ByteBuffer/wrap .getDouble))
                  :read-text (fn [_env ba] (-> ba read-utf8 Double/parseDouble))
                  :write-binary (fn [_env ^VectorReader rdr idx]
                                  (let [bb (doto (ByteBuffer/allocate typlen)
                                             (.putDouble (.getDouble rdr idx)))]
                                    (.array bb)))
                  :write-text (fn [_env ^VectorReader rdr idx]
                                (utf8 (.getDouble rdr idx)))})

       :numeric {:typname "numeric"
                 :col-type :decimal
                 :oid 1700
                 :typlen -1
                 :typcategory :numeric
                 :typsend "numeric_send"
                 :typreceive "numeric_recv"
                 :read-binary (fn [_env ba] (NumericBin/decode (ByteBuffer/wrap ba)))
                 :read-text (fn [_env ba] (-> ba read-utf8 bigdec))
                 :write-binary (fn [_env ^VectorReader rdr idx]
                                 (.array (NumericBin/encode (.getObject rdr idx))))
                 :write-text (fn [_env ^VectorReader rdr idx]
                               (utf8 (.getObject rdr idx)))}

       :uuid (let [typlen 16]
               {:typname "uuid"
                :col-type :uuid
                :oid 2950
                :typlen typlen
                :typcategory :user-defined
                :typsend "uuid_send"
                :typreceive "uuid_recv"
                :typinput "uuid_in"
                :typoutput "uuid_out"
                :read-binary (fn [_env ba] (util/byte-buffer->uuid (ByteBuffer/wrap ba)))
                :read-text (fn [_env ba] (UUID/fromString (read-utf8 ba)))
                :write-binary (fn [_env ^VectorReader rdr idx]
                                (let [ba ^bytes (byte-array typlen)]
                                  (.get (.getBytes rdr idx) ba)
                                  ba))
                :write-text (fn [_env ^VectorReader rdr idx]
                              (utf8 (util/byte-buffer->uuid (.getBytes rdr idx))))})

       :timestamp (let [typlen 8]
                    {:typname "timestamp"
                     :col-type [:timestamp-local :micro]
                     :oid 1114
                     :typcategory :date
                     :typlen typlen
                     :typsend "timestamp_send"
                     :typreceive "timestamp_recv"
                     :read-binary (fn [_env ba] ;;not sent via pgjdbc, reverse engineered
                                    (let [micros (+ (-> ba ByteBuffer/wrap .getLong) unix-pg-epoch-diff-in-micros)]
                                      (LocalDateTime/ofInstant (time/micros->instant micros) (ZoneId/of "UTC"))))

                     :read-text (fn [_env ba]
                                  (let [text (read-utf8 ba)
                                        res (time/parse-sql-timestamp-literal text)]
                                    (if (instance? LocalDateTime res)
                                      res
                                      (throw (IllegalArgumentException. (format "Can not parse '%s' as timestamp" text))))))

                     :write-binary (fn [_env ^VectorReader rdr idx]
                                     (let [micros (- (get-unix-micros rdr idx) unix-pg-epoch-diff-in-micros)
                                           bb (doto (ByteBuffer/allocate typlen)
                                                (.putLong micros))]
                                       (.array bb)))

                     :write-text (fn [env ^VectorReader rdr idx]
                                   (let [^LocalDateTime ldt (.getObject rdr idx)]
                                     (utf8 (case (get-in env [:parameters "datestyle"])
                                             "iso8601" (str ldt)
                                             (.format ldt iso-offset-date-time-formatter-with-space)))))})

       :timestamptz (let [typlen 8]
                      {:typname "timestamptz"
                       :col-type [:timestamp-tz :micro "UTC"]
                       ;;not possible to know the zone ahead of time
                       ;;this UTC most likely to be correct, but will have to replan if thats not the case
                       ;;could try to emit expression for zone agnostic tstz
                       :oid 1184
                       :typcategory :date
                       :typlen typlen
                       :typsend "timestamptz_send"
                       :typreceive "timestamptz_recv"
                       :read-binary (fn [_env ba] ;; not sent via pgjdbc, reverse engineered
                                      ;;unsure how useful binary tstz inputs are and if therefore if any drivers actually send them, as AFAIK
                                      ;;the wire format for tstz contains no offset, unlike the text format. Therefore we have to
                                      ;;infer one, so opting for UTC
                                      (let [micros (+ (-> ba ByteBuffer/wrap .getLong) unix-pg-epoch-diff-in-micros)]
                                        (OffsetDateTime/ofInstant (time/micros->instant micros) (ZoneId/of "UTC"))))

                       :read-text (fn [_env ba]
                                    (let [text (read-utf8 ba)
                                          res (time/parse-sql-timestamp-literal text)]
                                      (if (or (instance? ZonedDateTime res) (instance? OffsetDateTime res))
                                        res
                                        (throw (IllegalArgumentException. (format "Can not parse '%s' as timestamptz" text))))))

                       :write-binary (fn [_env ^VectorReader rdr idx]
                                       ;;despite the wording in the docs, based off pgjdbc behaviour
                                       ;;and other sources, this appears to be in UTC rather than the session tz
                                       (when-let [^ZonedDateTime zdt (.getObject rdr idx)]
                                         ;; getObject on end-of-time returns nil but isNull is false
                                         (let [unix-micros (-> zdt
                                                               (.toInstant)
                                                               (time/instant->micros))
                                               micros (- unix-micros unix-pg-epoch-diff-in-micros)
                                               bb (doto (ByteBuffer/allocate typlen)
                                                    (.putLong micros))]
                                           (.array bb))))

                       :write-text (fn [env ^VectorReader rdr idx]
                                     (when-let [^ZonedDateTime zdt (.getObject rdr idx)]
                                       (utf8 (case (get-in env [:parameters "datestyle"])
                                               "iso8601" (str zdt)
                                               (-> ^ZonedDateTime zdt
                                                   (.format iso-offset-date-time-formatter-with-space))))))})

       :tstz-range {:typname "tstz-range"
                    :oid 3910
                    :typcategory :range
                    :typsend "range_send"
                    :typreceive "range_recv"
                    :read-binary (fn [_env ba]
                                   (letfn [(parse-datetime [s]
                                             (when s
                                               (let [ts (time/parse-sql-timestamp-literal s)]
                                                 (cond
                                                   (instance? ZonedDateTime ts) ts
                                                   (instance? OffsetDateTime ts) (.toZonedDateTime ^OffsetDateTime ts)
                                                   :else ::malformed-date-time))))]
                                     (when-let [[_ from to] (re-matches #"\[([^,]*),([^\)]*)\)" (read-utf8 ba))]
                                       (let [from (parse-datetime from)
                                             to (parse-datetime to)]
                                         (when-not (or (= ::malformed-date-time from)
                                                       (= ::malformed-date-time to))
                                           (ZonedDateTimeRange. from to))))))
                    :read-text (fn [_env ba]
                                 (letfn [(parse-datetime [s]
                                           (when s
                                             (let [ts (time/parse-sql-timestamp-literal s)]
                                               (cond
                                                 (instance? ZonedDateTime ts) ts
                                                 (instance? OffsetDateTime ts) (.toZonedDateTime ^OffsetDateTime ts)
                                                 :else ::malformed-date-time))))]
                                   (when-let [[_ from to] (re-matches #"\[([^,]*),([^\)]*)\)" (read-utf8 ba))]
                                     (let [from (parse-datetime from)
                                           to (parse-datetime to)]
                                       (when-not (or (= ::malformed-date-time from)
                                                     (= ::malformed-date-time to))
                                         (ZonedDateTimeRange. from to))))))

                    :write-text (fn [_env ^VectorReader rdr idx]
                                  (let [^ZonedDateTimeRange tstz-range (.getObject rdr idx)]
                                    (utf8
                                     (str "[" (some-> (.getFrom tstz-range) (.format iso-offset-date-time-formatter-with-space))
                                          "," (some-> (.getTo tstz-range) (.format iso-offset-date-time-formatter-with-space))
                                          ")"))))
                    :write-binary (fn [_env ^VectorReader rdr idx]
                                    (let [^ZonedDateTimeRange tstz-range (.getObject rdr idx)]
                                      (byte-array
                                       (.getBytes
                                        (str "[" (some-> (.getFrom tstz-range) (.format iso-offset-date-time-formatter-with-space))
                                             "," (some-> (.getTo tstz-range) (.format iso-offset-date-time-formatter-with-space))
                                             ")")))))}

       :date (let [typlen 4]
               {:typname "date"
                :col-type [:date :day]
                :oid 1082
                :typcategory :date
                :typlen typlen
                :typsend "date_send"
                :typreceive "date_recv"
                :read-binary (fn [_env ba] ;;not sent via pgjdbc, reverse engineered
                               (let [days (+
                                           (-> ba ByteBuffer/wrap .getInt)
                                           unix-pg-epoch-diff-in-days)]
                                 (LocalDate/ofEpochDay days)))
                :read-text (fn [_env ba] (LocalDate/parse (read-utf8 ba)))
                :write-binary (fn [_env ^VectorReader rdr idx]
                                (let [days (- (.getInt rdr idx) unix-pg-epoch-diff-in-days)
                                      bb (doto (ByteBuffer/allocate typlen)
                                           (.putInt days))]
                                  (.array bb)))
                :write-text (fn [_env ^VectorReader rdr idx]
                              (-> ^LocalDate (.getObject rdr idx)
                                  (.format DateTimeFormatter/ISO_LOCAL_DATE)
                                  (utf8)))})

       :time (let [typlen 8]
               {:typname "time"
                :col-type [:time :micro]
                :oid 1083
                :typcategory :date
                :typlen typlen
                :typsend "time_send"
                :typreceive "time_recv"
                :read-text (fn [_env ba] (LocalTime/parse (read-utf8 ba)))
                :write-text (fn [_env ^VectorReader rdr idx]
                              (-> ^LocalTime (.getObject rdr idx)
                                  (.format DateTimeFormatter/ISO_LOCAL_TIME)
                                  (utf8)))})

       :varchar {:typname "varchar"
                 :col-type :utf8
                 :oid 1043
                 :typcategory :string
                 :typsend "varcharsend"
                 :typreceive "varcharrecv"
                 :read-text (fn [_env ba] (read-utf8 ba))
                 :read-binary (fn [_env ba] (read-utf8 ba))
                 :write-text (fn [_env ^VectorReader rdr idx]
                               (let [bb (.getBytes rdr idx)
                                     ba ^bytes (byte-array (.remaining bb))]
                                 (.get bb ba)
                                 ba))
                 :write-binary (fn [_env ^VectorReader rdr idx]
                                 (let [bb (.getBytes rdr idx)
                                       ba ^bytes (byte-array (.remaining bb))]
                                   (.get bb ba)
                                   ba))}

       :keyword {:typname "keyword"
                 :col-type :keyword
                 :oid 11111
                 :typcategory :string
                 :typsend "keywordsend"
                 :typreceive "keywordrecv"
                 :read-text (fn [_env ba] (read-utf8 ba))
                 :read-binary (fn [_env ba] (read-utf8 ba))
                 :write-text (fn [_env ^VectorReader rdr idx]
                               (let [bb (.getBytes rdr idx)
                                     ba ^bytes (byte-array (.remaining bb))]
                                 (.get bb ba)
                                 ba))
                 :write-binary (fn [_env ^VectorReader rdr idx]
                                 (let [bb (.getBytes rdr idx)
                                       ba ^bytes (byte-array (.remaining bb))]
                                   (.get bb ba)
                                   ba))}

       :bytea {:typname "bytea"
               :col-type :varbinary
               :oid 17
               :typcategory :user-defined
               :typsend "byteasend"
               :typreceive "bytearecv"
               :read-text (fn [_env ba] ba)
               :read-binary (fn [_env ba] (ByteBuffer/wrap ba)
                              (throw (UnsupportedOperationException.)))
               :write-text (fn [_env ^VectorReader rdr idx]
                             (let [bb (.getBytes rdr idx)
                                   ba ^bytes (byte-array (.remaining bb))]
                               (.get bb ba)
                               ba))
               :write-binary (fn [_env ^VectorReader rdr idx]
                               (let [bb (.getBytes rdr idx)
                                     ba ^bytes (byte-array (.remaining bb))]
                                 (.get bb ba)
                                 ba))}

       ;;same as varchar which makes this technically lossy in roundtrip
       ;;text is arguably more correct for us than varchar
       :text {:typname "text"
              :col-type :utf8
              :oid 25
              :typcategory :string
              :typarray :_text
              :typsend "textsend"
              :typreceive "textrecv"
              :read-text (fn [_env ba] (read-utf8 ba))
              :read-binary (fn [_env ba] (read-utf8 ba))
              :write-text (fn [_env ^VectorReader rdr idx]
                            (let [bb (.getBytes rdr idx)
                                  ba ^bytes (byte-array (.remaining bb))]
                              (.get bb ba)
                              ba))
              :write-binary (fn [_env ^VectorReader rdr idx]
                              (let [bb (.getBytes rdr idx)
                                    ba ^bytes (byte-array (.remaining bb))]
                                (.get bb ba)
                                ba))}

       :regclass {:typname "regclass"
                  :col-type :regclass
                  :oid 2205
                  :typcategory :numeric
                  :typsend "regclasssend"
                  :typreceive "regclassrecv"
                  ;;skipping read impl, unsure if anything can/would send a regclass param
                  :write-text (fn [_env ^VectorReader rdr idx]
                                ;;postgres returns the table name rather than a string of the
                                ;;oid here, however regclass is usually not returned from queries
                                ;;could reimplement oid -> table name resolution here or in getObject
                                ;;if the user cannot adjust the query to cast to varchar/text
                                (utf8 (Integer/toUnsignedString (.getInt rdr idx))))
                  :write-binary (fn [_env ^VectorReader rdr idx]
                                  ;;postgres returns the table name rather than a string of the
                                  ;;oid here, however regclass is usually not returned from queries
                                  ;;could reimplement oid -> table name resolution here or in getObject
                                  ;;if the user cannot adjust the query to cast to varchar/text
                                  (byte-array
                                   (utf8 (Integer/toUnsignedString (.getInt rdr idx)))))}

       :regproc {:typname "regproc"
                 :col-type :regproc
                 :oid 24
                 :typcategory :numeric
                 :typsend "regprocsend"
                 :typreceive "regprocrecv"
                 :write-text (fn [_env ^VectorReader rdr idx]
                               (utf8 (Integer/toUnsignedString (.getInt rdr idx))))
                 :write-binary (fn [_env ^VectorReader rdr idx]
                                 (byte-array
                                  (utf8 (Integer/toUnsignedString (.getInt rdr idx)))))}

       :boolean {:typname "boolean"
                 :col-type :bool
                 :typlen 1
                 :oid 16
                 :typcategory :boolean
                 :typsend "boolsend"
                 :typreceive "boolrecv"
                 :read-binary (fn [_env ba]
                                (case (-> ba ByteBuffer/wrap .get)
                                  1 true
                                  0 false
                                  (throw (IllegalArgumentException. "Invalid binary boolean value"))))
                 :read-text (fn [_env ba]
                              ;; https://www.postgresql.org/docs/current/datatype-boolean.html
                              ;; TLDR: unique prefixes of any of the text values are valid
                              (case (-> ba
                                        (read-utf8)
                                        (str/trim)
                                        (str/lower-case))
                                ("tr" "yes" "tru" "true" "on" "ye" "t" "y" "1") true
                                ("f" "fa" "fal" "fals" "false" "no" "n" "off" "0") false
                                (throw (IllegalArgumentException. "Invalid boolean value"))))
                 :write-binary (fn [_env ^VectorReader rdr idx]
                                 (byte-array [(if (.getBoolean rdr idx) 1 0)]))
                 :write-text (fn [_env ^VectorReader rdr idx]
                               (utf8 (if (.getBoolean rdr idx) "t" "f")))}

       :interval {:typname "interval"
                  :col-type [:interval :month-day-micro]
                  :typlen 16
                  :oid 1186
                  :typcategory :timespan
                  :typsend "interval_send"
                  :typreceive "interval_recv"
                  :read-binary (fn [_env _ba]
                                 (throw (IllegalArgumentException. "Interval parameters currently unsupported")))
                  :read-text (fn [_env ba]
                               (try
                                 (Time/asInterval (read-utf8 ba))
                                 (catch NullPointerException _
                                   (throw (IllegalArgumentException. "Interval parameters currently unsupported")))))
                  :write-binary (fn [_env ^VectorReader rdr idx]
                                  (byte-array (interval-rdr->iso-micro-interval-str-bytes rdr idx)))
                  :write-text (fn [_env ^VectorReader rdr idx] (interval-rdr->iso-micro-interval-str-bytes rdr idx))}

       ;; json-write-text is essentially the default in send-query-result so no need to specify here
       :json {:typname "json"
              :oid 114
              :typcategory :user-defined
              :typsend "json_send"
              :typreceive "json_recv"
              :read-text (fn [_env ba]
                           (JsonSerde/decode (ByteArrayInputStream. ba)))
              :read-binary (fn [_env ba]
                             (JsonSerde/decode (ByteArrayInputStream. ba)))
              :write-binary (fn [_env ^VectorReader rdr idx]
                              (let [json (-> (.getObject rdr idx) (JsonSerde/encode))]
                                (.getBytes json)))}

       :jsonb {:typname "jsonb"
               :oid 3802
               :typcategory :user-defined
               :typsend "jsonb_send"
               :typreceive "jsonb_recv"
               :read-text (fn [_env ba]
                            (JsonSerde/decode (ByteArrayInputStream. ba)))
               :read-binary (fn [_env ba]
                              (JsonSerde/decode (ByteArrayInputStream. ba)))}

       :transit {:typname "transit"
                 :oid transit-oid
                 :typcategory :user-defined
                 :typsend "transit_send"
                 :typreceive "transit_recv"
                 :read-text (fn [_env ^bytes ba] (serde/read-transit ba :json))
                 :read-binary (fn [_env ^bytes ba] (serde/read-transit ba :json))
                 :write-text (fn [_env ^VectorReader rdr idx]
                               (serde/write-transit (.getObject rdr idx) :json))
                 :write-binary (fn [_env ^VectorReader rdr idx]
                                 (serde/write-transit (.getObject rdr idx) :json))}

       :_int4 (let [typlen 4
                    oid 1007
                    typelem 23]
                {:typname "_int4"
                 :typlen typlen
                 :oid oid
                 :typcategory :array
                 :typelem typelem
                 :typsend "array_send"
                 :typreceive "array_recv"
                 :typinput "array_in"
                 :typoutput "array_out"
                 :read-text (fn [_env arr]
                              (log/tracef "read-text_int4 %s" arr)
                              (let [elems (when-not (empty? arr)
                                            (let [sa (str/trim arr)]
                                              (-> sa
                                                  (subs 1 (dec (count sa)))
                                                  (str/split #","))))]
                                (mapv #(Integer/parseInt %) elems)))
                 :read-binary (fn [_env ba]
                                (read-binary-int-array ba))
                 :write-text (fn [_env ^VectorReader list-rdr idx]
                               (let [list (.getObject list-rdr idx)
                                     sb (StringBuilder. "{")]
                                 (doseq [elem list]
                                   (.append sb (or elem "NULL"))
                                   (.append sb ","))
                                 (if (seq list)
                                   (.setCharAt sb (dec (.length sb)) \})
                                   (.append sb "}"))
                                 (utf8 (.toString sb))))
                 :write-binary (fn [_env ^VectorReader list-rdr idx]
                                 (let [list (.getObject list-rdr idx)
                                       ^ByteBuffer bb (binary-list-header (count list) typelem typlen)]
                                   (doseq [elem list]
                                     ;; apparently each element has to be prefixed with its length
                                     ;; https://stackoverflow.com/questions/4016412/postgresqls-libpq-encoding-for-binary-transport-of-array-data
                                     ;; also the way vanilla postgres does it seems to confirm this
                                     (.putInt bb typlen)
                                     (.putInt bb elem))
                                   (.array bb)))})

       :_int8 (let [typlen 8
                    oid 1016
                    typelem 20]
                {:typname "_int8"
                 :typlen typlen
                 :oid oid
                 :typcategory :array
                 :typelem typelem
                 :typsend "array_send"
                 :typreceive "array_recv"
                 :typinput "array_in"
                 :typoutput "array_out"
                 :read-text (fn [_env arr]
                              (log/tracef "read-text_int8 %s" arr)
                              (let [elems (when-not (empty? arr)
                                            (let [sa (str/trim arr)]
                                              (-> sa
                                                  (subs 1 (dec (count sa)))
                                                  (str/split #","))))]
                                (mapv #(Long/parseLong %) elems)))
                 :read-binary (fn [_env ba]
                                (read-binary-int-array ba))
                 :write-text (fn [_env ^VectorReader list-rdr idx]
                               (let [list (.getObject list-rdr idx)
                                     sb (StringBuilder. "{")]
                                 (doseq [elem list]
                                   (.append sb (or elem "NULL"))
                                   (.append sb ","))
                                 (if (seq list)
                                   (.setCharAt sb (dec (.length sb)) \})
                                   (.append sb "}"))
                                 (utf8 (.toString sb))))
                 :write-binary (fn [_env ^VectorReader list-rdr idx]
                                 (let [list (.getObject list-rdr idx)
                                       ^ByteBuffer bb (binary-list-header (count list) typelem typlen)]
                                   (doseq [elem list]
                                     ;; apparently each element has to be prefixed with its length
                                     ;; https://stackoverflow.com/questions/4016412/postgresqls-libpq-encoding-for-binary-transport-of-array-data
                                     ;; also the way vanilla postgres does it seems to confirm this
                                     (.putInt bb typlen)
                                     (.putLong bb elem))
                                   (.array bb)))})
       :_text (let [oid 1009
                    typelem 25]
                {:typname "_text"
                 :oid oid
                 :typcategory :array
                 :typelem typelem
                 :typsend "array_send"
                 :typreceive "array_recv"
                 :typinput "array_in"
                 :typoutput "array_out"
                 :read-text (fn [_env arr]
                              (when-not (empty? arr)
                                (let [sa (str/trim arr)]
                                  (-> sa
                                      (subs 1 (dec (count sa)))
                                      (str/split #",")))))
                 :write-text (fn [_env ^VectorReader list-rdr idx]
                               (let [list (.getObject list-rdr idx)
                                     sb (StringBuilder. "{")]
                                 (doseq [elem list]
                                   (.append sb (or elem "NULL"))
                                   (.append sb ","))
                                 (if (seq list)
                                   (.setCharAt sb (dec (.length sb)) \})
                                   (.append sb "}"))
                                 (utf8 (.toString sb))))
                 :read-binary (fn [_env ba]
                                (read-binary-text-array ba))
                 :write-binary (fn [_env ^VectorReader list-rdr idx]
                                 (let [list (.getObject list-rdr idx)
                                       ^ByteBuffer bb (binary-variable-size-array-header
                                                       (count list)
                                                       typelem
                                                       (reduce + 0 (map count list)))]
                                   (doseq [^String elem list]
                                     ;; apparently each element has to be prefixed with its length
                                     ;; https://stackoverflow.com/questions/4016412/postgresqls-libpq-encoding-for-binary-transport-of-array-data
                                     ;; also the way vanilla postgres does it seems to confirm this
                                     (let [bytez (.getBytes elem)]
                                       (.putInt bb (count bytez))
                                       (.put bb bytez)))
                                   (.array bb)))})}

      (as-> types (assoc types :null (:text types)))))

(def pg-types-by-oid (into {} (map #(hash-map (:oid (val %)) (val %))) pg-types))

(def ^:private col-types->pg-types
  {:utf8 :text
   :i64 :int8
   :i32 :int4
   :i16 :int2
   #_#_:i8 :bit
   :f64 :float8
   :f32 :float4
   :decimal :numeric
   :uuid :uuid
   :bool :boolean
   :null :null
   :regclass :regclass
   :regproc :regproc
   :varbinary :bytea
   :keyword :keyword
   [:date :day] :date
   [:time :second] :time
   [:time :milli] :time
   [:time :micro] :time
   [:time :nano] :time
   [:timestamp-local :micro] :timestamp
   [:timestamp-tz :micro] :timestamptz
   [:timestamp-local :nano] :timestamp
   [:timestamp-tz :nano] :timestamptz
   :tstz-range :tstz-range
   [:interval :year-month] :interval
   [:interval :month-day-nano] :interval
   [:interval :month-day-micro] :interval
   [:list :i32] :_int4
   [:list :i64] :_int8
   [:list :utf8] :_text

   #_#_ ; FIXME not supported by pgjdbc until we sort #3683 and #3212
   :transit :transit})

(defn col-type->pg-type [col-type]
  (get col-types->pg-types
       (cond-> col-type
         ;; ignore TZ
         (= (types/col-type-head col-type) :timestamp-tz) (subvec 0 2)

         (= (types/col-type-head col-type) :decimal) types/col-type-head)

       :default))

(defn ->unified-col-type [col-types]
  (when (pos? (count col-types))
    (cond
      (= 1 (count col-types)) (first col-types)
      (set/subset? col-types #{:float4 :float8}) :float8
      (set/subset? col-types #{:int2 :int4 :int8}) :int8
      (set/subset? col-types #{:_int4 :_int8}) :_int8)))

(defn field->pg-col [^Field field]
   (let [field-name (.getName field)
         col-type (types/field->col-type field)
         col-types (-> (types/remove-nulls col-type)
                       types/flatten-union-types
                       (->> (into #{} (map col-type->pg-type))))]
     (log/tracef "field->pg-type %s, col-type %s, field-name %s" (pr-str col-types) col-type field-name)

     {:pg-type (or (->unified-col-type col-types) :default)
      :col-name field-name}))
