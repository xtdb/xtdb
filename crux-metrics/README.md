# crux-metrics

This module provides components that can expose metrics about a node. Currently
only ingest and query-metrics have been implemented, although this is in active
development.

See cards https://github.com/juxt/crux/issues?q=is%3Aissue+is%3Aopen+label%3Aprod-monitoring-384

Full docs [here](https://opencrux.com/docs#config-metrics)

## Dropwizard metrics

[Dropwizard metrics](https://metrics.dropwizard.io/4.1.2/) are used to store and display metrics.

rocksdb metrics can be also included by using the module `'crux.kv.rocksdb/kv-store-with-metrics`

The following 5 components can be used to display these.

### Console

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics.dropwizard.console/reporter]
                 ;; optional args
                 :crux.metrics.dropwizard.console/report-frequency "PT1S"
                 :crux.metrics.dropwizard.console/rate-unit "seconds"
                 :crux.metrics.dropwizard.console/duration-unit "hours"})
```

### CSV

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics.dropwizard.csv/reporter]
                 :crux.metrics.dropwizard.csv/file-name "out.csv"
                 ;; optional args
                 :crux.metrics.dropwizard.csv/report-frequency "PT1S"
                 :crux.metrics.dropwizard.csv/rate-unit "seconds"
                 :crux.metrics.dropwizard.csv/duration-unit "hours"})
```

### JMX

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics.dropwizard.jmx/reporter]
                 ;; optional args
                 :crux.metrics.dropwizard.jmx/domain "prod-node"
                 :crux.metrics.dropwizard.jmx/rate-unit "seconds"
                 :crux.metrics.dropwizard.jmx/duration-unit "hours"})
```

### Prometheus

#### HTTP exporter

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics.dropwizard.prometheus/http-exporter]
                 ;; optional args
                 :crux.metrics.dropwizard.prometheus/port 8080
                 :crux.metrics.dropwizard.prometheus/jvm-metrics? false})
```

#### Reporter

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics.dropwizard.prometheus/reporter]
                 :crux.metrics.dropwizard.prometheus/push-gateway "localhost:9090"
                 ;; optional args
                 :crux.metrics.dropwizard.prometheus/report-frequency "PT1S"
                 :crux.metrics.dropwizard.prometheus/prefix "prod-node"})
```

### Cloudwatch

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics.dropwizard.cloudwatch/reporter]
                 ;; optional args
                 :crux.metrics.dropwizard.cloudwatch/dry-run-report-frequency "PT1S"
                 :crux.metrics.dropwizard.cloudwatch/dry-run? false
                 :crux.metrics.dropwizard.cloudwatch/jvm-metrics? false
                 :crux.metrics.dropwizard.cloudwatch/jvm-dimensions {"foo" "bar"}
                 :crux.metrics.dropwizard.cloudwatch/region "eu-west-2"
                 :crux.metrics.dropwizard.cloudwatch/ignore-rules ["crux.tx" "!crux.tx.ingest"]})
```
