# crux-generate-api

A small module to generate Java files for the different types of topologies.

Usage: 
Call **gen-topology-file** with the desired classname, and a symbol representing one of the topologies. For example:
```clojure
(gen-topology-file "KafkaNode" 'crux.kafka/topology)
```
