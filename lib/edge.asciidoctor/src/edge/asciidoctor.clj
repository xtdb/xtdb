;; Copyright © 2016-2019, JUXT LTD.

(ns edge.asciidoctor
  (:require
   [aero.core :as aero]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.repl :refer [source source-fn]]
   [clojure.string :as string]
   [clojure.tools.logging :as log]
   [hiccup.core :as h]
   [integrant.core :as ig]
   [yada.yada :as yada]))

(defn- coerce-from-ruby-decorator
  "Attributes are given to Asciidoctorj macro callbacks as instances of
  the org.asciidoctor.internal.RubyAttributesMapDecorator class, which
  doesn't behave as a Clojure map. This function coerces to a Clojure
  map."
  [attrs]
  (into {} attrs))

(defn- document-attributes
  "Return the document's attributes from a Asciidoctorj node."
  [el]
  (.getAttributes (.getDocument el)))

;; Used to pass the yada ctx through to AsciiDoctor's JRuby environment
(def ^:private ctx-local
  (proxy [ThreadLocal clojure.lang.IDeref] []
    (initialValue [] :unset)
    (deref [] (.get this))))

(defn- bind-context [ctx]
  (.set ctx-local ctx)
  (proxy [java.io.Closeable] []
    (close [] (.set ctx-local nil))))

(defn engine []
  (let [engine (org.asciidoctor.Asciidoctor$Factory/create)
        reg (.javaExtensionRegistry engine)]

    (.docinfoProcessor
      reg
      (proxy [org.asciidoctor.extension.DocinfoProcessor] []
        (process [document]
          (slurp (io/resource "doc/docinfo/docinfo.html")))))

    (.postprocessor
      reg
      (proxy [org.asciidoctor.extension.Postprocessor] []
        (process [document output]
          (string/replace
            output "</html>"
            (str (slurp (io/resource "doc/docinfo/docinfo-footer.html"))
                 "</html>")))))

    (.includeProcessor
      reg
      (proxy [org.asciidoctor.extension.IncludeProcessor] []
        (handles [target] true)
        (process [parent reader target attributes]
          (condp re-matches target
            #"config:(.*):(.*)"
            :>> (fn [[_ k path]]
                  (.push_include
                    reader
                    (-> (io/resource "config.edn")
                        (aero/read-config {:profile (keyword k)})
                        (get (keyword path))
                        pr-str)
                    target target 1 attributes))

            #"src:(.*)"
            :>> (fn [[_ s]]
                  (.push_include
                    reader
                    (or (source-fn (symbol s)) "source not found")
                    target target 1 attributes))

            #"srcblk:(.*)"
            :>> (fn [[_ s]]
                  (.push_include
                    reader
                    (format
                      "[source,clojure]\n%s\n----\n%s\n----\n"
                      (if (contains? (set (vals (into {} attributes))) "title")
                        (let [{:keys [file line]} (meta (resolve (symbol s)))]
                          (format ".link:/sources/src/%s[%s] at line %s" file file line))
                        "")
                      (or (source-fn (symbol s)) "source not found"))
                    target target 1 attributes))

            #"resource:(.*)"
            :>> (fn [[_ path]]
                  (.push_include reader (if-let [res (io/resource path)]
                                          (slurp res)
                                          "resource not found")
                                 target target 1 attributes))

            ;; Else
            (if-let [f (io/resource (str "doc/sources/" target))]
              (.push_include reader (slurp f) target target 1 attributes)
              (log/warn "Failed to find" target))))))

    (.inlineMacro
      reg
      (proxy [org.asciidoctor.extension.InlineMacroProcessor] ["srcloc"]
        (getRegexp []
          (log/infof "Calling getRegexp!")
          )
        (process [parent target attributes]
          (let [{:keys [file line]} (some-> target symbol resolve meta)]
            (format "%s %s of link:/sources/src/%s[%s]"
                    (if
                        (contains? (set (vals (into {} attributes))) "titlecase")
                        "Line" "line")
                    line file file)))))

    (.inlineMacro
      reg
      (proxy [org.asciidoctor.extension.InlineMacroProcessor] ["bidi"]
        (getRegexp [])
        (process [parent target attributes]
          (let [attrs (into {} attributes)
                [_ typ id] (re-matches #"(.*):(.*)" target)
                path-info (get attrs "path-info")
                query-params (some-> (get attrs "query-params") read-string)
                uri-info (yada/uri-info
                           @ctx-local
                           (keyword id)
                           (merge {}
                                  (when query-params
                                    {:query-params query-params})
                                  (when path-info
                                    {:path-info path-info})))]
            (h/html [:a {:href
                         (case typ
                           "href" (:href uri-info)
                           "uri" (:uri uri-info))}
                     (get attrs "1")])))))
    engine))

(defn load-doc [ctx engine docname content]
  (assert ctx)
  (assert engine)
  (.set ctx-local ctx)
  (.load
    engine
    content
    (java.util.HashMap. ; Asciidoctor is in JRuby which takes HashMaps
      {"safe" org.asciidoctor.SafeMode/UNSAFE
       "header_footer" true
       "to_file" false
       "backend" "html5"
       "foo" "bar"
       "attributes"
       (java.util.HashMap.
         (merge
           (edn/read-string
             (slurp
               (io/resource "doc/asciidoctor/attributes.edn")))
           {"docname" docname
            }))})))

(defmethod ig/init-key :edge.asciidoctor/engine [_ config]
  (engine))
