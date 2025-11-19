---
title: Clojure Configuration Cookbook
---

<details>
<summary>Changelog (last updated v2.1)</summary>

v2.1: multi-database support

: Prior to 2.1, the `:disk-cache` and `:memory-cache` keys were nested under the local/remote storage:

  ``` clojure
  {:storage [:local
             {;; -- required

              :path "/var/lib/xtdb/storage"
 
              ;; -- optional
 
              ;; :max-cache-bytes 1024
              ;; :max-cache-entries 536870912
             }]}

  {:storage [:remote 
             {;; --required

              :object-store [:object-store-implementation {}]

              :local-disk-cache "/tmp/local-disk-cache"

              ;; -- optional
              ;; :max-cache-entries 1024
              ;; :max-cache-bytes 536870912
              ;; :max-disk-cache-percentage 75
              ;; :max-disk-cache-bytes 107374182400
  }]}
  ```
  
</details>

This document provides examples for the EDN configuration of XTDB components, to be supplied to `xtdb.node/start-node`.

See the [main configuration documentation](/ops/config) for more details.

## Log

Main article: [Log](/ops/config/log)

### In-Memory

Main article: [in-memory log](/ops/config/log#_in_memory)

This is the default, and can be omitted.

``` clojure
{:log [:in-memory
       {;; -- optional

        ;; :instant-src (java.time.InstantSource/system)
        }]}
```

### Local disk

Main article: [local-disk log](/ops/config/log#_local_disk)

``` clojure
{:log [:local
        {;; -- required
         ;; accepts `String`, `File` or `Path`
         :path "/tmp/log"

         ;; -- optional

         ;; accepts `java.time.InstantSource`
         ;; :instant-src (InstantSource/system)

         ;; :buffer-size 4096
         ;; :poll-sleep-duration "PT1S"
         }]}
```

### Kafka

Main article: [Kafka](/ops/config/log/kafka)

``` clojure
{:log [:kafka
       {;; -- required
        :bootstrap-servers "localhost:9092"
        :topic-name "xtdb-log"

        ;; -- optional

        ;; :create-topic? true
        ;; :poll-duration #xt/duration "PT1S"
        ;; :properties-file "kafka.properties"
        ;; :properties-map {}
        ;; :replication-factor 1
        ;; :topic-config {}
        }]}
```

## Storage

Main article: [Storage](/ops/config/storage)

### In-Memory

Main article: [in-memory storage](/ops/config/storage#in-memory)

This is the default, and should be omitted.

### Local disk

Main article: [local-disk storage](/ops/config/storage#local-disk)

``` clojure
{:storage [:local
           {;; -- required

            ;; accepts `String`, `File` or `Path`
            :path "/var/lib/xtdb/storage"

             ;; -- optional

             ;; :max-cache-bytes 536870912
            }]}
```

### Remote

Main article: [remote storage](/ops/config/storage#remote)

``` clojure
{:storage [:remote {;; -- required

                    ;; Each object store implementation has its own configuration -
                    ;; see below for some examples.
                    :object-store [:object-store-implementation {}]}]}

 ;; -- required for remote storage
 ;; Local directory to store the working-set cache in.
 :disk-cache {;; -- required

              ;; accepts `String`, `File` or `Path`
              :path "/tmp/local-disk-cache"

              ;; -- optional
              ;; The maximum proportion of space to use on the filesystem for the diskCache directory
              ;; (overridden by maxSizeBytes, if set).
              :max-size-ratio 0.75

              ;; The upper limit of bytes that can be stored within the diskCache directory (unset by default).
              :max-size-bytes 107374182400}

 ;; -- optional - in-memory cache created with default config if not supplied
 ;; configuration for XTDB's in-memory cache
 ;; if not provided, an in-memory cache will still be created, with the default size
 :memory-cache {;; -- optional

                ;; The maximum proportion of the JVM's direct-memory space to use for the in-memory cache
                ;; (overridden by `:max-size-bytes`, if set).
                :max-size-ratio 0.5

                ;; unset by default
                :max-size-bytes 536870912}

}
```

### S3

Main article: [S3](/ops/aws#storage)

``` clojure
{:storage [:remote
           {:object-store [:s3
                           {;; -- required
                            :bucket "my-bucket"

                            ;; -- optional

                            ;; :prefix "my-xtdb-node"
                            ;; :configurator (reify S3Configurator
                            ;;                 ...)
                           }]}]}
```

### Azure Blob Storage

Main article: [Azure Blob Storage](/ops/azure#storage)

``` clojure
{:storage [:remote
           {:object-store [:azure
                           {;; -- required
                            ;; --- At least one of storage-account or storage-account-endpoint is required
                            :storage-account "storage-account"
                            ;; :storage-account-endpoint "https://storage-account.privatelink.blob.core.windows.net"
                            :container "xtdb-container"

                            ;; -- optional

                            ;; :prefix "my-xtdb-node"
                            ;; :user-managed-identity-client-id "user-managed-identity-client-id"
                           }]}]}
```

### Google Cloud Storage

Main article: [Google Cloud Storage](/ops/google-cloud#storage)

``` clojure
{:storage [:remote
           {:object-store [:google-cloud
                           {;; -- required
                            :project-id "xtdb-project"
                            :bucket "xtdb-bucket"

                            ;; -- optional

                            ;; :prefix "my-xtdb-node"
                           }]}]}
```

## Tracing

Main article: [Tracing](/ops/config/monitoring#tracing)

``` clojure
{:tracer {;; -- required
          :enabled? true
          :endpoint "http://localhost:4318/v1/traces"

          ;; -- optional

          ;; :service-name "xtdb"
        }}
```
