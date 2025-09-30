---
title: Monitoring XTDB with Grafana
---

XTDB provides tools and templates to facilitate the monitoring and observability of XTDB nodes.
Metrics are exposed in the **Prometheus** format, which can be scraped by **Prometheus** and visualized in **Grafana** using XTDB's pre-built dashboards.

:::note
The XTDB cloud images come pre-configured with Prometheus metrics exposed - see the ["Monitoring docs"](../config/monitoring) for more information.
:::

## Prerequisites

You will need:

- A running Grafana instance
- Prometheus configured to scrape metrics from XTDB nodes
- Prometheus configured as a data source in Grafana

Refer to the official documentation for setup instructions:

- [Grafana](https://grafana.com/docs/grafana/latest/installation/)
- [Prometheus](https://prometheus.io/docs/prometheus/latest/getting_started/)
- [Adding Prometheus as a Grafana datasource](https://prometheus.io/docs/visualization/grafana/#using)

## Setting Up Grafana Dashboards

To import XTDB's pre-built dashboards:

1. In Grafana, navigate to `Dashboards → New → Import dashboard`.
2. Upload the dashboard JSON file from the XTDB repository.
3. Select the Prometheus data source and click `Import`.

The following dashboards are available:

### Cluster Monitoring Dashboard

Provides an overview of the entire XTDB cluster, including node health and performance.

![Cluster Monitoring Dashboard](/images/docs/cluster-monitoring.png)

Download the JSON template: [here](https://github.com/xtdb/xtdb/blob/main/monitoring/public-dashboards/xtdb-monitoring.json).

### Node Debugging Dashboard

Focuses on individual XTDB nodes, showing metrics such as resource usage, performance, and health.

![Node Debugging Dashboard](/images/docs/node-debugging.png)

Download the JSON template: [here](https://github.com/xtdb/xtdb/blob/main/monitoring/public-dashboards/xtdb-node-debugging.json).
