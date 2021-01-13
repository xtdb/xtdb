# AWS CDK Deploy of Crux Bench Dashboards and Alerts

## Overview

A grid of graph definitions is used to generate metrics, alarms and
widgets, one per benchmark.  Done for query and ingest benchmarks.

A composite alarm is built off of the grid alarms and alerts are sent
via SNS.

The derivative squared of the time-taken is used to alert if a
benchmark has significantly changed overnight.

The dashboards need to be manually shared at present as it's not
possible to do via an API.

Lein dependency resolution does not work well with AWS CDK hence the
switch to `deps.edn`

## Install CDK

``` shell
npm install -g aws-cdk
```

## Edit Resources

```
edit src/dashboards.clj
```

## Plan or Diff the running

```shell
cdk diff
```

## Deploy

```shell
cdk deploy
```
