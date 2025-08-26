(ns xtdb.error
  (:require [clojure.string :as str]
            [clojure.test :as t]
            [cognitect.anomalies :as-alias anom]
            [cognitect.transit :as transit])
  (:import [java.io Writer]
           [java.nio.channels ClosedByInterruptException]
           [java.sql BatchUpdateException]
           [java.time.format DateTimeParseException]
           [java.util.regex Pattern]
           com.fasterxml.jackson.core.JsonParseException
           [xtdb.error Anomaly Busy Conflict Fault Forbidden Incorrect Interrupted NotFound Unavailable Unsupported]))

(defprotocol ToAnomaly
  (->anomaly [ex data]))

(defn incorrect
  (^xtdb.error.Incorrect [error-code msg] (incorrect error-code msg {}))
  (^xtdb.error.Incorrect [error-code msg data]
   (Incorrect. msg
               (-> data
                   (assoc ::anom/category ::anom/incorrect)
                   (cond-> error-code (assoc ::code error-code))
                   (dissoc ::cause))
               (::cause data))))

(defn ^:deprecated illegal-arg
  ;; replace with `incorrect`
  ([k] (illegal-arg k {}))

  ([k data]
   (illegal-arg k data nil))

  ([k {::keys [^String message] :as data} cause]
   (incorrect k (or message (format "Illegal argument: '%s'" (symbol k))) (-> data (assoc ::cause cause) (dissoc ::message)))))

(defn ^:deprecated runtime-err
  ;; replace with `incorrect`
  ([k] (runtime-err k {}))

  ([k data]
   (runtime-err k data nil))

  ([k {::keys [^String message] :as data} cause]
   (incorrect k (or message (format "Runtime error: '%s'" (symbol k))) (-> data (assoc ::cause cause) (dissoc ::message)))))

(defn conflict
  (^xtdb.error.Conflict [error-code msg] (conflict error-code msg {}))
  (^xtdb.error.Conflict [error-code msg data]
   (Conflict. msg
              (-> data
                  (assoc ::anom/category ::anom/conflict)
                  (cond-> error-code (assoc ::code error-code))
                  (dissoc ::cause))
              (::cause data))))

(defn unsupported
  (^xtdb.error.Unsupported [error-code msg] (unsupported error-code msg {}))
  (^xtdb.error.Unsupported [error-code msg data]
   (Unsupported. msg
                 (-> data
                     (assoc ::anom/category ::anom/unsupported)
                     (cond-> error-code (assoc ::code error-code))
                     (dissoc ::cause))
                 (::cause data))))

(defn todo
  (^xtdb.error.Unsupported [] (todo "TODO"))
  (^xtdb.error.Unsupported [msg] (unsupported ::todo msg)))

(defn interrupted
  (^xtdb.error.Interrupted [] (interrupted nil ""))
  (^xtdb.error.Interrupted [error-code msg] (interrupted error-code msg {}))
  (^xtdb.error.Interrupted [error-code msg data]
   (Interrupted. msg
                 (-> data
                     (assoc ::anom/category ::anom/interrupted)
                     (cond-> error-code (assoc ::code error-code))
                     (dissoc ::cause))
                 (::cause data))))

(defn fault
  (^xtdb.error.Fault [error-code msg] (fault error-code msg {}))
  (^xtdb.error.Fault [error-code msg data]
   (Fault. msg
           (-> data
               (assoc ::anom/category ::anom/fault)
               (cond-> error-code (assoc ::code error-code))
               (dissoc ::cause))
           (::cause data))))

(defn forbidden
  (^xtdb.error.Forbidden [error-code msg] (forbidden error-code msg {}))
  (^xtdb.error.Forbidden [error-code msg data]
   (Forbidden. msg
               (-> data
                   (assoc ::anom/category ::anom/forbidden)
                   (cond-> error-code (assoc ::code error-code))
                   (dissoc ::cause))
               (::cause data))))

(defn not-found
  (^xtdb.error.NotFound [error-code msg] (not-found error-code msg {}))
  (^xtdb.error.NotFound [error-code msg data]
   (NotFound. msg
              (-> data
                  (assoc ::anom/category ::anom/not-found)
                  (cond-> error-code (assoc ::code error-code))
                  (dissoc ::cause))
              (::cause data))))

(defn busy
  (^xtdb.error.Busy [error-code msg] (busy error-code msg {}))
  (^xtdb.error.Busy [error-code msg data]
   (Busy. msg
          (-> data
              (assoc ::anom/category ::anom/busy)
              (cond-> error-code (assoc ::code error-code))
              (dissoc ::cause))
          (::cause data))))

(defn unavailable
  (^xtdb.error.Unavailable [error-code msg] (unavailable error-code msg {}))
  (^xtdb.error.Unavailable [error-code msg data]
   (Unavailable. msg
                 (-> data
                     (assoc ::anom/category ::anom/unavailable)
                     (cond-> error-code (assoc ::code error-code))
                     (dissoc ::cause))
                 (::cause data))))

(defn read-error [[category error-code msg data]]
  ((case category
    :incorrect incorrect
    :conflict conflict
    :unsupported unsupported
    :fault fault
    :interrupted interrupted)
   error-code msg data))

(defn render-error [anomaly]
  (let [{:keys [::code ::anom/category] :as data} (ex-data anomaly)]
    [(keyword (name category)) code (ex-message anomaly) (dissoc data ::anom/category ::code)]))

(defmethod print-dup Anomaly [anomaly ^Writer w]
  (.write w (str " #xt/error " (render-error anomaly))))

(defmethod print-method Anomaly [anomaly w]
  (print-dup anomaly w))

(def transit-readers
  {"xt/error" (transit/read-handler read-error)})

(def transit-writers
  {Anomaly (transit/write-handler "xt/error" render-error)})

(defn- wrap-ctx [anom ctx]
  (if (empty? ctx)
    anom
    (let [msg (ex-message anom)
          data (into (ex-data anom) ctx)]
      (case (::anom/category data)
        ::anom/incorrect (Incorrect. msg data anom)
        ::anom/conflict (Conflict. msg data anom)
        ::anom/unsupported (Unsupported. msg data anom)
        ::anom/fault (Fault. msg data anom)
        ::anom/forbidden (Forbidden. msg data anom)
        ::anom/interrupted (Interrupted. msg data anom)
        ::anom/not-found (NotFound. msg data anom)
        ::anom/busy (Busy. msg data anom)
        ::anom/unavailable (Unavailable. msg data anom)))))

(extend-protocol ToAnomaly
  Anomaly
  (->anomaly [ex data] (wrap-ctx ex data))

  NumberFormatException
  (->anomaly [ex data] (incorrect ::number-format (ex-message ex) (into {::cause ex} data)))

  IllegalArgumentException
  (->anomaly [ex data] (incorrect ::illegal-arg (ex-message ex) (into {::cause ex} data)))

  DateTimeParseException
  (->anomaly [ex data]
    (incorrect ::date-time-parse (ex-message ex)
               (into {::cause ex, :date-time (.getParsedString ex)} data)))

  ClosedByInterruptException
  (->anomaly [ex data] (interrupted ::interrupted (or (ex-message ex) "Interrupted") (into {::cause ex} data)))

  InterruptedException
  (->anomaly [ex data] (interrupted ::interrupted (ex-message ex) (into {::cause ex} data)))

  UnsupportedOperationException
  (->anomaly [ex data] (unsupported ::unsupported (ex-message ex) (into {::cause ex} data)))

  JsonParseException
  (->anomaly [ex data] (incorrect ::json-parse (ex-message ex) (into {::cause ex} data)))

  BatchUpdateException
  (->anomaly [ex data] (->anomaly (ex-cause ex) data))

  Throwable
  (->anomaly [ex data] (fault ::unknown (ex-message ex) (into {::cause ex} data))))

(defmacro wrap-anomaly ^{:style/indent 1} [ctx & body]
  `(try
     ~@body
     (catch Throwable e#
       (throw (->anomaly e# ~ctx)))))

(defmethod t/assert-expr 'anomalous? [msg [_anomalous? [cat code expected-msg data] & body :as form]]
  ;; e.g. (t/is (anomalous? [:incorrect :xtdb/an-error-code "foo" {:bar 1}]
  ;;                        ...))
  ;; if any are set to nil, they are not checked
  ;;
  ;; expected-msg can be a string or a regex
  ;; - if it is a string, it is checked with `str/includes?`
  ;; - if it is a regex, it is checked with `re-find`

  (let [cat-sym (gensym 'cat)
        code-sym (gensym 'code)
        msg-sym (gensym 'msg)
        data-sym (gensym 'data)]
    `(let [msg# ~msg]
       (try
         ~@body
         (t/do-report {:type :fail, :message msg#, :expected '~form, :actual nil})

         (catch Anomaly e#
           (let [{~cat-sym ::anom/category, ~code-sym ::code, :as ~data-sym} (ex-data e#)
                 ~msg-sym (ex-message e#)]
             (when (and ~(or (nil? cat)
                             (t/assert-expr msg (list '= (keyword "cognitect.anomalies" (name cat)) cat-sym)))
                        ~(or (nil? code)
                             (t/assert-expr msg (list '= code code-sym)))
                        ~(or (nil? expected-msg)
                             (when (string? expected-msg)
                               (t/assert-expr msg `(str/includes? ~msg-sym ~expected-msg)))
                             (when (instance? Pattern expected-msg)
                               (t/assert-expr msg `(re-find ~expected-msg ~msg-sym))))
                        ~(or (nil? data)
                             (t/assert-expr msg `(= ~data (dissoc ~data-sym ::anom/category ::code)))))
               (t/do-report {:type :pass, :message ~msg,
                             :expected '~form, :actual e#})))
           e#)))))
