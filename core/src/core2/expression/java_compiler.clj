(ns core2.expression.java-compiler
  (:import [java.io ByteArrayOutputStream StringWriter]
           [java.nio.charset StandardCharsets]
           [java.util LinkedHashMap Map]
           [java.util.function Function]
           [java.net URI]
           [javax.tools ForwardingJavaFileManager JavaCompiler JavaFileManager JavaFileObject$Kind
            SimpleJavaFileObject ToolProvider]
           [clojure.lang Compiler DynamicClassLoader]))

(defn- ->class-file-manager ^javax.tools.JavaFileManager [^JavaFileManager file-manager ^Map output-streams]
  (proxy [ForwardingJavaFileManager] [file-manager]
    (getJavaFileForOutput [location class-name kind sibling]
      (proxy [SimpleJavaFileObject]
          [(URI/create (str "string:////" (.replace ^String class-name "." "/") (.extension ^JavaFileObject$Kind kind)))
           kind]

        (openOutputStream []
          (.computeIfAbsent output-streams class-name (reify Function
                                                        (apply [_ _]
                                                          (ByteArrayOutputStream.)))))))))

(defn- ->java-source-file-object ^javax.tools.JavaFileObject [^String class-name ^String source]
  (proxy [SimpleJavaFileObject]
      [(URI/create (str "string:////" (.replace class-name "." "/") (.extension JavaFileObject$Kind/SOURCE)))
       JavaFileObject$Kind/SOURCE]

    (getCharContent [ignore-encoding-errors]
      source)))

(def ^:private ^JavaCompiler compiler (ToolProvider/getSystemJavaCompiler))
(def ^:private ^JavaFileManager standard-file-manager (.getStandardFileManager compiler nil nil StandardCharsets/UTF_8))

(defn compile-java ^Class [^String class-name ^String source]
  (let [output-streams (LinkedHashMap.)
        file-manager (->class-file-manager standard-file-manager output-streams)
        source-files [(->java-source-file-object class-name source)]
        err (StringWriter.)
        options ["-source" "11" "-target" "11"]
        task (.getTask compiler err file-manager nil options nil source-files)
        ^DynamicClassLoader loader @Compiler/LOADER]
    (when-not (.call task)
      (throw (IllegalArgumentException. (str err))))
    (reduce
     (fn [_ [class-name ^ByteArrayOutputStream output-stream]]
       (.defineClass loader class-name (.toByteArray output-stream) nil))
     nil
     output-streams)))

(comment
  (time
   (->> "
package user;

public class Main {
    class Foo {}
    public Main() {
      System.out.println(\"Hello World!\" + new Foo());
    }
}"
        (compile-java "user.Main")
        (.newInstance))))
