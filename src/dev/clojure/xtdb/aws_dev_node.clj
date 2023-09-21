(ns xtdb.aws-dev-node
  "Tools for provisioning and working with AWS hosted XTDB nodes for development and testing in AWS"
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import (clojure.lang ExceptionInfo)
           (java.io Closeable StringWriter)
           (java.time Duration)
           (java.util List)
           (xtdb.s3 S3Configurator)))

;; pre-reqs:
;; aws cli v2
;; valid juxt aws credentials
;; a profile set up using the juxt-admin role called 'xtdb-dev-node'

(defn- log [& msg]
  (send log/*logging-agent* (fn [_] (apply println msg))))

(set! *warn-on-reflection* false)

(def ^:private ^:dynamic *sh-ret* :out)

(defn- sh [& args]
  (let [args (mapcat (fn ! [a] (if (coll? a) (mapcat ! a) [a])) args)
        command-str (str/join " " args)
        _ (log "  $" command-str)
        {:keys [exit, err, out] :as m} (apply sh/sh args)]
    (case *sh-ret*
      :out
      (do
        (when-not (= 0 exit)
          (throw (ex-info (or (not-empty out) "Command failed")
                          {:command command-str
                           :exit exit
                           :out out
                           :err err})))
        out)
      :map m)))

(defn aws
  "Calls the aws cli, e.g (aws \"s3\" \"ls\" \"s3://my-bucket\") returns the json or throws on error.
  Check ex-info for stderr of any exception."
  [& args]
  (let [cmd-args (concat args ["--output" "json"
                               "--region" "eu-west-1"
                               "--profile" "xtdb-dev-node"])
        out (apply sh "aws" cmd-args)]
    (try (json/read-str out) (catch Throwable _ out))))

(defn- stack-output-map [stack-json]
  (reduce #(assoc %1 (%2 "OutputKey") (%2 "OutputValue")) {} (get stack-json "Outputs")))

(defn- dev-box-cfn-json [stack-name opts]
  {"StackName" stack-name
   "TemplateBody" (slurp (io/resource "aws-dev-node.yml"))
   "Parameters" (vec (for [[k v] (merge {"NodeId" stack-name} (:parameters opts))
                           :when v]
                       {"ParameterKey" k
                        "ParameterValue" v}))
   "OnFailure" "DELETE"
   "Tags" [{"Key" "XTDBDevNode", "Value" "dev-box"}]})

(defn- cfn-create [input-json]
  (aws "cloudformation" "create-stack"
       "--capabilities" "CAPABILITY_NAMED_IAM"
       "--cli-input-json" (json/write-str input-json)))

(defn- create-dev-box-stack [stack-name opts]
  (cfn-create (dev-box-cfn-json stack-name opts)))

(defn- cfn-delete [stack-name]
  (aws "cloudformation" "delete-stack" "--stack-name" stack-name))

(defn cfn-describe [stack-name]
  (try
    (-> (aws "cloudformation" "describe-stacks" "--stack-name" stack-name)
        (get "Stacks")
        first)
    (catch ExceptionInfo e
      (let [{:keys [exit]} (ex-data e)]
        ;; not exists
        (when-not (= 254 exit)
          (throw e))))))

(defn- ssm-get-private-key [key-pair-id]
  (let [key-name (str "/ec2/keypair/" key-pair-id)
        param (when key-pair-id (aws "ssm" "get-parameter" "--name" key-name "--with-decryption"))]
    (get-in param ["Parameter" "Value"])))

(defn- ssh-remote-info [stack]
  (let [{public-dns-name "PublicDnsName"
         key-pair-id "KeyPairId"}
        (stack-output-map stack)
        key-file (io/file (System/getProperty "java.io.tmpdir") (str key-pair-id ".pem"))]
    (when-not (.exists key-file)
      (spit key-file (ssm-get-private-key key-pair-id))
      (sh "chmod" "400" (.getAbsolutePath key-file)))
    {:user "ec2-user"
     :host public-dns-name
     :key-file key-file}))

(defn ssh-cmd [{:keys [user, host, key-file]}]
  ["ssh"
   "-oStrictHostKeyChecking=no"
   "-i" (.getAbsolutePath (io/file key-file))
   (str user "@" host)])

(defn ssh [ec2 cmd & cmd-args]
  (apply sh (concat (ssh-cmd ec2) [cmd] cmd-args)))

(defn wait-for-ssh [dev-box]
  (let [ssh-wait-duration (Duration/ofMinutes 2)
        ssh-wait-until (+ (System/currentTimeMillis) (.toMillis ssh-wait-duration))]
    (trampoline
      (fn try-ssh []
        (try
          (binding [*out* (StringWriter.)] (ssh dev-box "echo" "ping"))
          (catch ExceptionInfo e
            (let [{:keys [err]} (ex-data e)]
              (if (and (string? err)
                       (str/includes? err "Connection refused")
                       (< (System/currentTimeMillis) ssh-wait-until))
                (do (Thread/sleep 1000)
                    try-ssh)
                (throw e)))))))))

(defn ssh-repl-fwd-cmd [{:keys [user, host, key-file, repl-local-port, repl-remote-port]}]
  ["ssh"
   "-oStrictHostKeyChecking=no"
   "-i" (.getAbsolutePath (io/file key-file))
   "-NL"
   (format "%s:localhost:%s" repl-local-port repl-remote-port)
   (str user "@" host)])

(defn ssh-repl-fwd ^Process [dev-box]
  (-> (doto (ProcessBuilder. ^List (ssh-repl-fwd-cmd dev-box))
        (.inheritIO))
      (.start)))

(def ^:redef repls [])

(defn kill-java [dev-box] (binding [*sh-ret* :map] (ssh dev-box "pkill" "java")))

(defrecord ReplProcess [dev-box ^Process fwd-process]
  Closeable
  (close [_]
    (.destroy fwd-process)
    (kill-java dev-box)))

(defn repl [dev-box]
  (log "Starting repl, output will be inherited - you should see the stdout of the remote gradle process")
  (let [{:keys [repl-local-port]} dev-box
        _
        (->
          (ProcessBuilder.
            (->> (concat
                   (ssh-cmd dev-box)
                   ["cd" "xtdb" ";"]
                   ["./gradlew" "clojureRepl" "--bind" "localhost" "--port" (str repl-local-port)])
                 ^List (vec)))
          (doto (.inheritIO))
          (.start))

        fwd-proc
        (ssh-repl-fwd dev-box)]

    (log (format "Forwarded localhost:%s to the ec2 box, connect to it as a nREPL." repl-local-port))

    (let [ret (->ReplProcess dev-box fwd-proc)]
      (alter-var-root #'repls conj ret)
      ret)))

(defn wait-for-stack [stack-name]
  (let [wait-duration (Duration/ofMinutes 5)
        sleep-duration (Duration/ofSeconds 30)]
    (loop [stack (cfn-describe stack-name)
           wait-until (+ (System/currentTimeMillis) (.toMillis wait-duration))]
      (cond
        (nil? stack) (throw (ex-info "Stack does not exist" {:stack-name stack-name}))
        (= "CREATE_COMPLETE" (get stack "StackStatus")) (do (log stack-name "- stack created") stack)
        :else
        (if (< (System/currentTimeMillis) wait-until)
          (do (Thread/sleep (.toMillis sleep-duration))
              (recur (cfn-describe stack-name) wait-until))
          (throw (ex-info "Timed out waiting for stack" {:stack-timeout true, :stack-name stack-name})))))))

(def repl-port
  "The default port that will be opened for repl interaction with dev nodes, to be port forwarded with ssh-fwd"
  5555)

(defn get-dev-box [stack-name]
  (let [stack-name stack-name
        stack (wait-for-stack stack-name)]
    (merge stack
           {:role-name (get (stack-output-map stack) "RoleName")
            :repl-local-port repl-port
            :repl-remote-port repl-port}
           (ssh-remote-info stack))))

(defn create-s3-stack [stack-name]
  (cfn-create {"StackName" stack-name
               "TemplateBody" (slurp (io/resource "aws-dev-node-s3.yml"))
               "Parameters" [{"ParameterKey" "S3BucketName"
                              "ParameterValue" stack-name}
                             {"ParameterKey" "S3BucketDeletionPolicy"
                              "ParameterValue" "Delete"}]
               "OnFailure" "DELETE"
               "Tags" [{"Key" "XTDBDevNode", "Value" "s3"}]}))

(defn provision-dev-box
  "Creates a new dev box stack with the given name, blocks until the stack is created.

  To pass cloudformation stack parameters, use the kw-arg :parameters, e.g:
  (provision-dev-box \"foo\" :parameters {\"InstanceType\" \"m7gd.medium\"})

  See aws-dev-node.yml for the cloudformation template and its available parameters."
  [stack-name & {:as opts}]
  (create-dev-box-stack stack-name opts)
  (log "Waiting for stack, this may take a while:" stack-name)
  (let [dev-box (get-dev-box stack-name)]
    (wait-for-ssh dev-box)
    (log stack-name "- installing java")
    (ssh dev-box "sudo" "yum" "install" "-y" "git" "java-17-amazon-corretto-headless")
    (log stack-name "- cloning xtdb")
    (ssh dev-box "git" "clone" "https://github.com/xtdb/xtdb.git" "--depth" "1" "--branch" "2.x" "--single-branch")
    (log stack-name "- provisioning complete")
    dev-box))

(defn checkout
  "Fetches and checkouts a commit on a remote dev box, e.g (checkout \"2.x\")"
  [dev-box sha-or-branch]
  (doto dev-box
    (ssh "cd" "xtdb" ";" "git" "fetch" "origin" sha-or-branch)
    (ssh "cd" "xtdb" ";" "git" "checkout" sha-or-branch))
  nil)

(defn provision-s3
  "Creates a new s3 dev stack with the given name, blocks until the stack is created."
  [stack-name]
  (create-s3-stack stack-name)
  (log "Waiting for stack, this may take a while:" stack-name)
  (wait-for-stack stack-name))

(defn list-attached-policies [dev-box]
  (let [{:keys [role-name]} dev-box]
    (aws "iam" "list-attached-role-policies" "--role-name" role-name)))

(defn detach-all-policies [dev-box]
  (let [{:keys [role-name]} dev-box
        {:strs [AttachedPolicies]} (list-attached-policies dev-box)]
    (doseq [{:strs [PolicyArn]} AttachedPolicies]
      (aws "iam" "detach-role-policy" "--role-name" role-name "--policy-arn" PolicyArn))))

(defn attach-s3-policy [dev-box s3-stack]
  (let [{:keys [role-name]} dev-box
        {:strs [XTDBPolicyArn]} (stack-output-map s3-stack)]
    (aws "iam" "attach-role-policy" "--role-name" role-name "--policy-arn" XTDBPolicyArn)))

(defn delete-dev-box [{:strs [StackName] :keys [key-file] :as dev-box}]
  (some-> key-file (io/delete-file true))
  (detach-all-policies dev-box)
  (cfn-delete StackName))

(defn delete-s3 [s3-stack]
  (let [{:strs [StackName]} s3-stack
        {:strs [BucketName]} (stack-output-map s3-stack)]
    (aws "s3" "rm" "--recursive" (str "s3://" BucketName))
    (cfn-delete StackName)))

(defn get-s3 [stack-name] (cfn-describe stack-name))

(defn s3-module-partial-config [s3-stack]
  (let [{:strs [BucketName SNSTopicArn]} (stack-output-map s3-stack)]
    {:bucket BucketName,
     :sns-topic-arn SNSTopicArn}))

(defn s3-module-config [s3-stack prefix]
  (merge (s3-module-partial-config s3-stack)
         {:prefix prefix
          :configurator (reify S3Configurator)}))

(comment

  ;; if we want to see what is there we can do something like this
  (-> (aws "cloudformation" "list-stacks" "--stack-status-filter" "CREATE_COMPLETE" "CREATE_IN_PROGRESS" "DELETE_IN_PROGRESS")
      (get "StackSummaries")
      (->> (map (juxt #(get % "StackName") #(get % "StackStatus")))))

  ;; provisioning might take some time, use futures to not block your repl
  (def wot-s3-fut (future (provision-s3 "wot-s3-test")))
  (def wot-xtdb-fut (future (provision-dev-box "wot-xtdb-test")))
  ;; parameters to the stack can be forwarded
  (def wot-xtdb-fut (future (provision-dev-box "wot-xtdb-test" :parameters {"InstanceType" "m7gd.medium"})))

  (realized? wot-s3-fut)
  (realized? wot-xtdb-fut)

  @wot-s3-fut
  @wot-xtdb-fut

  ;; once provisioned you can get a handle for the stacks used as args to other fns
  ;; in this namespace
  (def wot-s3 (get-s3 "wot-s3-test"))
  (def wot-xtdb (get-dev-box "wot-xtdb-test"))

  wot-s3
  wot-xtdb

  ;; delete the boxes safely, tears down the stack
  (delete-dev-box wot-xtdb)
  ;; the s3 delete function must first delete all data in the bucket
  (delete-s3 wot-s3)

  ;; attach/detach permissions to talk to the s3 resources to a dev node
  (attach-s3-policy wot-xtdb wot-s3)
  (detach-all-policies wot-xtdb)

  ;; if something is not working it can be useful to ssh in
  (str/join " " (ssh-cmd wot-xtdb))

  ;; a repl handle is a Closeable resource, sets up the remote clojureRepl process and port forwards
  ;; the :repl-local-port, you need to still wait for the remote clojureRepl process to be up before connections
  ;; will succeed, it might take some time the 1st time you run it as the deps need to be downloaded, gradle etc
  (:repl-local-port wot-xtdb)
  (def wot-repl (repl wot-xtdb))
  (.close wot-repl)

  ;; use this to test repl connection is up, will throw if not, check your output buffer
  ;; once working you can connect to the remote process using a standard nrepl client
  (do
    (require '[nrepl.core :as nrepl])
    (let [{:keys [repl-local-port]} wot-xtdb]
      (with-open [conn (nrepl/connect :port repl-local-port)]
        (-> (nrepl/client conn 5000)
            (nrepl/message {:op "eval" :code "'connected"})
            doall))))

  ;; a valid :xtdb.s3/object-store configuration for the stack with the given :prefix
  (s3-module-config wot-s3 "foo")

  ;; if you want to checkout a different commit, branch or tag (available on GitHub), by default the latest 2.x is checked out
  ;; when you provision, this command will fetch the latest from the remote
  (checkout wot-xtdb "2.x")

  )
