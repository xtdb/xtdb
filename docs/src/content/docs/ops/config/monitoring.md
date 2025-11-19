---
title: Monitoring & Observability
---

XTDB offers a suite of tools & templates to facilitate monitoring and observability.
These include a **Healthz Server** for health checks, **Metrics** for performance insights, integrations with third-party monitoring systems and **Grafana** dashboards for visualizing the health and performance of XTDB nodes.

## Healthz Server

The Healthz Server is a lightweight HTTP server that runs by default on all XTDB nodes.
It provides health indicators, making it useful for monitoring in containerized and orchestrated environments.

### Configuration

The Healthz Server can be configured to run on a custom port:

``` yaml
healthz:
  # Port to run the Healthz Server on.
  # Default: 8080 (can be set as an !Env value).
  port: 8080
```

### Health Routes

The following routes are exposed by the Healthz Server and can be used to monitor the node's status:

- `/healthz/started`: Indicates whether the node has completed startup and has caught up on indexing.
    - Recommended for [**startup probes**](https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/#startup-probe).
    - We recommend configuring a generous initial timeout, as it waits for indexing to stabilize.
- `/healthz/alive`: Confirms the application is running without critical errors.
    - Suitable for [**liveness probes**](https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/#liveness-probe).
- `/healthz/ready`: Signals that the node is ready to process requests.
    - Suitable for [**readiness probes**](https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/#readiness-probe).

## Metrics

XTDB provides various metrics for monitoring the health and performance of its nodes.
These metrics are available via Prometheus and can also be integrated with cloud-based observability services.

### Prometheus Metrics

By default, XTDB nodes expose metrics in the [Prometheus](https://prometheus.io/) format.
These can be accessed at the following endpoint on the **Healthz Server**:

    <node_url>:<healthz_port>/metrics

### Cloud Integrations

XTDB nodes can be configured to report metrics to the following cloud-based monitoring services:

- [Azure Application Insights](../azure#monitoring)
- [AWS CloudWatch](../aws#monitoring)

## Grafana Dashboards

XTDB provides [pre-built Grafana dashboards](https://github.com/xtdb/xtdb/tree/main/monitoring/public-dashboards) for monitoring the health and performance of XTDB clusters and individual nodes.

For more information on how to set these up on Grafana, see the ["Monitoring XTDB with Grafana"](../guides/monitoring-with-grafana) guide.

## Tracing

XTDB supports distributed tracing using OpenTelemetry, providing introspection into query execution and performance.

Traces are sent via the OTLP (OpenTelemetry Protocol) HTTP endpoint to your tracing backend (e.g., Grafana Tempo, Jaeger, etc).

Tracing is disabled by default.

### Configuration

``` yaml
tracer:
  # -- required

  # Enable OpenTelemetry tracing.
  enabled: true

  # OTLP HTTP endpoint for sending traces.
  # (Can be set as an !Env value)
  endpoint: "http://localhost:4318/v1/traces"

  # -- optional

  # Service name identifier for traces.
  # (Can be set as an !Env value)
  # serviceName: "xtdb"
```

For more information on viewing and analyzing traces with Grafana, see the ["Monitoring XTDB with Grafana"](../guides/monitoring-with-grafana#distributed-tracing-with-tempo) guide.
