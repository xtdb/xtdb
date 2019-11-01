(ns crux.gen-topology-classes
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [crux.topology-info :as ti]))

(import (com.squareup.javapoet MethodSpec TypeSpec FieldSpec JavaFile
                               ParameterizedTypeName WildcardTypeName ClassName)
        javax.lang.model.element.Modifier
        java.util.Properties
        java.util.Map
        java.util.HashMap
        clojure.lang.Keyword)

(defn format-topology-key [key]
  (-> (name key)
      (string/replace "?" "")
      (string/replace "-" "_")
      (string/upper-case)))

(defn format-topology-key-method [key]
  (->> (name key)
       (#(string/replace % "?" ""))
       (#(string/split % #"-"))
       (map string/capitalize )
       (string/join "")
       (str "with")))

(defn make-generic [raw-type]
  (ParameterizedTypeName/get raw-type (into-array [Keyword, Object])))

(defn add-properties-field [class properties-name]
  (let [field (FieldSpec/builder (make-generic Map) properties-name
                                 (into-array ^Modifier [Modifier/PUBLIC]))]
    (.addField class (.build field))))

(defn add-constructor [class properties-name topology-name]
  (let [constructor (MethodSpec/constructorBuilder)]
    (.addModifiers constructor (into-array ^Modifier [Modifier/PUBLIC]))
    (.addStatement constructor (str properties-name " = new $T()") (into-array [(make-generic HashMap)]))
    (.addStatement constructor (str properties-name ".put(Keyword.intern($S),Keyword.intern($S))")
                   (into-array ["crux.node/topology", topology-name]))
    (.addMethod class (.build constructor))))

(defn val-type [val]
  (let [type (:crux.config/type val)]
    (cond
      (= type :crux.config/nat-int) Long
      (= type :crux.config/boolean) Boolean
      :else String)))

(defn build-key-field[[key value]]
  (let [field (FieldSpec/builder Keyword (format-topology-key key)
                                 (into-array ^Modifier [Modifier/PUBLIC Modifier/FINAL Modifier/STATIC]))]
    (.initializer field "Keyword.intern($S)" (into-array [(string/replace key ":" "")]))
    (.build field)))

(defn build-key-default-field[[key val]]
  (let [field (FieldSpec/builder (val-type val) (str (format-topology-key key) "_DEFAULT")
                                 (into-array ^Modifier [Modifier/PUBLIC Modifier/FINAL Modifier/STATIC]))]
    (cond
      (= (val-type val) String) (.initializer field "$S" (into-array [(:default val)]))
      (= (val-type val) Long) (.initializer field "$Ll" (into-array [(:default val)]))
      :else (.initializer field "$L" (into-array [(:default val)])))
    (.build field)))

(defn build-topology-key-setter [[key value] properties-name]
  (let [set-property (MethodSpec/methodBuilder (format-topology-key-method key))]
    (.addModifiers set-property (into-array ^Modifier [Modifier/PUBLIC]))
    (.addParameter set-property (val-type value) "val" (into-array Modifier []))
    (.addStatement set-property (str properties-name ".put($L, val)")
                   (into-array [(format-topology-key key)]))
    (.addStatement set-property "return this" (into-array Modifier []))
    (.returns set-property (ClassName/get "" "Builder" (into-array String [])))
    (.build set-property)))

(defn add-topology-key-code [class builder-class properties-name [_ value :as topology-val]]
  (do
    (.addField class (build-key-field topology-val))
    (when (some? (:default value))
      (.addField class (build-key-default-field topology-val)))
    (.addMethod builder-class (build-topology-key-setter topology-val properties-name))))

(defn add-builder-fn [class-name properties-name]
  (let [builder (MethodSpec/methodBuilder "build")]
    (.addModifiers builder (into-array ^Modifier [Modifier/PUBLIC]))
    (.addStatement builder "return $L" (into-array [properties-name]))
    (.returns builder (make-generic Map))
    (.addMethod class-name (.build builder))))

(defn build-java-class [class-name topology-info topology-name]
  (let [class (TypeSpec/classBuilder class-name)
        builder-class (TypeSpec/classBuilder "Builder")
        properties-name (str class-name "Properties")]
    (.addModifiers class (into-array ^Modifier [Modifier/PUBLIC]))
    (.addModifiers builder-class (into-array ^Modifier [Modifier/PUBLIC Modifier/STATIC]))
    (add-properties-field builder-class properties-name)
    (add-constructor builder-class properties-name topology-name)
    (doall (map #(add-topology-key-code class builder-class properties-name %) (seq topology-info)))
    (add-builder-fn builder-class properties-name)
    (.addType class (.build builder-class))
    (.build class)))

(defn build-java-file [class-name topology-info topology-name]
  (let [javafile (build-java-class class-name topology-info topology-name)
        output (io/file class-name)]
    (-> (JavaFile/builder "crux.api" javafile)
        (.build)
        (.writeTo output))))

(defn gen-topology-file [class-name topology]
  (let [topology-info (ti/get-topology-info topology)
        full-topology-info (merge topology-info crux.node/base-topology crux.kv/options)]
    (build-java-file class-name full-topology-info (str topology))))
