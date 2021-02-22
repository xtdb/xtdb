(ns crux.http-server.https-server-test
  (:require  [clojure.test :as t]
             [crux.api :as api]
             [crux.io :as cio]
             [crux.fixtures :as fix :refer [*opts* *api*]]
             [clojure.java.io :as io]
             [certifiable.main :as cert]
             [clj-http.client :as http]))

(def ^:dynamic *api-url*)
(defn- get-request [path]
  (prn (str *api-url* path))
  (http/get (str *api-url* path)
            {:accept "application/edn"
             :redirect-strategy :none}))

(defn with-https-opts [f]
  (fix/with-tmp-dirs #{cert-dir}
    (let [certificate-path (str cert-dir "/test-cert.jks")
          server-port (cio/free-port)
          ssl-port (cio/free-port)]
      (cert/create-dev-certificate-jks {:keystore-path certificate-path})
      (binding [*api-url* (str "https://localhost:" ssl-port)
                *opts* {:crux.http-server/server {:port server-port
                                                  :ssl-opts {:ssl-port ssl-port
                                                             :keystore certificate-path
                                                             :key-password "password"
                                                             :keystore-type "jks"}}}]
        (f)))))

(t/use-fixtures :each with-https-opts fix/with-node)

(t/deftest can-access-http-server
  (let [{:keys [status]} (get-request "/_crux/status")]
    (t/is (= 200 status) "can request status of the server"))
  (fix/submit+await-tx *api* [[:crux.tx/put {:crux.db/id :ivan}]]))
