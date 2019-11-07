# crux-generate-api

A small module to generate Java files for the different types of topologies.

Usage:
Can use **lein generate** from the base of the folder to generate classes for *crux.kafka/topology*, *crux.jdbc/topology* and *crux.standalone/topology*.

To generate a class for an individual topology, call `gen-topology-file` within **gen_topology_classes**  with the desired classname, and a string representing one of the topologies. For example:
```clojure
(crux.gen-topology-classes/gen-topology-file "KafkaTopology" "crux.kafka/topology")
```
