# crux-metrics

This module provides some components that can expose metrics about a node in
various different ways. Currenly only ingest and querymetrics have been
implemented, although this is in active development.
See cards https://github.com/juxt/crux/issues?q=is%3Aissue+is%3Aopen+label%3Aprod-monitoring-384

Full docs [here](https://opencrux.com/docs#config-metrics)

## Dropwizard metrics

We provide 5 different ways to expose metics using dropwizard (or rather `metrics-clojure`).

https://metrics-clojure.readthedocs.io/en/latest/reporting.html

### Console

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics/with-console]
                 ;; optional args
                 :crux.metrics.dropwizard.console/report-rate 1
                 :crux.metrics.dropwizard.console/rate-unit "seconds"
                 :crux.metrics.dropwizard.console/duration-unit "hours"})
```

### CSV

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics/with-csv]
                 :crux.metrics.dropwizard.csv/file-name "out.csv"
                 ;; optional args
                 :crux.metrics.dropwizard.csv/report-rate 1
                 :crux.metrics.dropwizard.csv/rate-unit "seconds"
                 :crux.metrics.dropwizard.csv/duration-unit "hours"})
```

### JMX

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics/with-jmx]
                 ;; optional args
                 :crux.metrics.dropwizard.jmx/domain "prod-node"
                 :crux.metrics.dropwizard.jmx/rate-unit "seconds"
                 :crux.metrics.dropwizard.jmx/duration-unit "hours"})
```

### Prometheus

#### HTTP exporter

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics/with-prometheus-https-exporter]
                 ;; optional args
                 :crux.metrics.dropwizard.prometheus/port 8080
                 :crux.metrics.dropwizard.prometheus/jvm-metrics? false)}
```

#### Reporter

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics/with-prometheus-reporter]
                 :crux.metrics.dropwizard.prometheus/pushgateway "localhost:9090"
                 ;; optional args
                 :crux.metrics.dropwizard.prometheus/duration "PT1S"
                 :crux.metrics.dropwizard.prometheus/prefix "prod-node")}
```

### Cloudwatch

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics/with-cloudwatch]
                 ;; optional args
                 :crux.metrics.dropwizard.prometheus/duration "PT1S"
                 :crux.metrics.dropwizard.prometheus/dry-run? false
                 :crux.metrics.dropwizard.prometheus/jmv-metrics? false
                 :crux.metrics.dropwizard.prometheus/jmv-dimensions {"foo" "bar"}
                 :crux.metrics.dropwizard.prometheus/region "eu-west-2"
```
