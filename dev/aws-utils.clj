(ns aws-utils
  (:require [clojure.string :as s]))

(defn ->userdata [sh]
  (str "\"UserData\": {\n"
       "\"Fn::Base64\": {\n"
       "\"Fn::Join\": [\n"
       "\"\\n\",\n"
       "[\n"
       "\""
       (apply
        str
        (-> sh
            (s/replace "\n\n" "\n")
            (s/replace "\\" "\\\\")
            (s/replace "\"" "\\\"")
            (s/replace "\n" "\",\n\"")
            drop-last
            drop-last
            drop-last))
       "\n"
       "]\n"
       "]\n"
       "}\n"
       "}\n"))
