(ns xtdb.expression.uri
  (:require [xtdb.expression :as expr])
  (:import [java.net URI]))

(defmethod expr/read-value-code :uri [_ & args]
  (-> `(.readObject ~@args)
      (expr/with-tag URI)))

(defmethod expr/write-value-code :uri [_ & args]
  `(.writeObject ~@args))

(defmethod expr/codegen-cast [:utf8 :uri] [_]
  {:return-type #xt/type :uri
   :->call-code #(do `(URI/create (expr/resolve-string ~(first %))))})

(defmethod expr/codegen-cast [:uri :utf8] [_]
  {:return-type #xt/type :utf8
   :->call-code #(do `(expr/resolve-utf8-buf (str ~(first %))))})

(defn- codegen-extract [f-sym]
  {:return-type #xt/type [:? :utf8]
   :continue-call (fn [f [uri]]
                    (let [uri-part (gensym 'uri-part)]
                      `(if-let [~uri-part (~f-sym ~uri)]
                         ~(f #xt/type :utf8 `(expr/resolve-utf8-buf ~uri-part))
                         ~(f #xt/type :null nil))))})

(defn uri->scheme [^URI uri] (some-> (.getScheme uri) expr/resolve-utf8-buf))

(defmethod expr/codegen-call [:uri_scheme :uri] [_]
  (codegen-extract `uri->scheme))

(defn uri->user-info [^URI uri] (some-> (.getUserInfo uri) expr/resolve-utf8-buf))

(defmethod expr/codegen-call [:uri_user_info :uri] [_]
  (codegen-extract `uri->user-info))

(defn uri->host [^URI uri] (some-> (.getHost uri) expr/resolve-utf8-buf))

(defmethod expr/codegen-call [:uri_host :uri] [_]
  (codegen-extract `uri->host))

(defn uri->path [^URI uri] (some-> (.getPath uri) (expr/resolve-utf8-buf)))

(defmethod expr/codegen-call [:uri_path :uri] [_]
  (codegen-extract `uri->path))

(defmethod expr/codegen-call [:uri_port :uri] [_]
  {:return-type #xt/type [:? :i32]
   :continue-call (fn [f [uri]]
                    (let [uri-part (gensym 'uri-part)]
                      `(let [~uri-part (.getPort ~uri)]
                         (if (neg? ~uri-part)
                           ~(f #xt/type :null nil)
                           ~(f #xt/type :i32 uri-part)))))})

(defn uri->query [^URI uri] (some-> (.getQuery uri) expr/resolve-utf8-buf))

(defmethod expr/codegen-call [:uri_query :uri] [_]
  (codegen-extract `uri->query))

(defn uri->fragment [^URI uri] (some-> (.getFragment uri) expr/resolve-utf8-buf))

(defmethod expr/codegen-call [:uri_fragment :uri] [_]
  (codegen-extract `uri->fragment))
