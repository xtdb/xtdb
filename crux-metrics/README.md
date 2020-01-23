# crux-metrics

This module provides some components that can expose metrics about a node in
various different ways. Currenly only ingest metrics have been implemented,
although this is in active development.
See cards https://github.com/juxt/crux/issues?q=is%3Aissue+is%3Aopen+label%3Aprod-monitoring-384

## Dropwizard metrics

We provide 3 different ways to expose metics using dropwizard (or rather `metrics-clojure`).

https://metrics-clojure.readthedocs.io/en/latest/reporting.html

### Console

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics.dropwizard/with-console]
                                     ;; optional args
                                     :crux.metrics.dropwizard/console-reporter-opts {}
                                     :crux.metrics.dropwizard/console-reporter-rate 1})
```

### CSV

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics.dropwizard/with-csv]
                                     ;; optional args
                                     :crux.metrics.dropwizard/csv-reporter-opts {}
                                     :crux.metrics.dropwizard/csv-reporter-file "/tmp/csv_reporter"
                                     :crux.metrics.dropwizard/csv-reporter-rate 1})
```

### JMX

```
(api/start-node {:crux.node/topology ['crux.standalone/topology
                                      'crux.metrics.dropwizard/with-jmx]
                                     ;; optional args
                                     :crux.metrics.dropwizard/jmx-reporter-opts {}})
```


