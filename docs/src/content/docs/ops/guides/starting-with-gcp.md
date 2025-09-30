---
title: Setting up a cluster on Google Cloud
---

This guide will walk you through the process of configuring and running an XTDB Cluster on Google Cloud.
This setup includes:

- Using **Google Cloud Storage** as the remote storage implementation.
- Utilizing **Apache Kafka** as the shared message log implementation.
- Exposing the cluster to the internet via a Postgres wire-compatible server.

The required Google Cloud infrastructure is provisioned using **Terraform**, and the XTDB cluster and it's resources are deployed on [**Google Kubernetes Engine**](https://cloud.google.com/kubernetes-engine?hl=en) using **Helm**.

Although we provide numerous parameters to configure the templates, you are encouraged to edit them, use them as a foundation for more advanced use cases, and reuse existing infrastructure when suitable.
These templates serve as a simple starting point for running XTDB on Google Cloud and Kubernetes, and should be adapted to meet your specific needs, especially in production environments.

This guide assumes that you are using the default templates.

## Requirements

Before starting, ensure you have the following installed:

- The **Google Cloud CLI** - See the [**Installation Instructions**](https://cloud.google.com/sdk/docs/install).
- **Terraform** - See the [**Installation Instructions**](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli).
- **kubectl** - The Kubernetes CLI, used to interact with the AKS cluster.
  See the [**Installation Instructions**](https://kubernetes.io/docs/tasks/tools/install-kubectl/).
- **Helm** - The Kubernetes package manager, used to deploy numerous components to AKS.
  See the [**Installation Instructions**](https://helm.sh/docs/intro/install/).

### Requirements on Google Cloud

Within Google Cloud, ensure that you have an existing Google Cloud project you can deploy into, and ensure the following APIs are enabled on the project:

- Cloud Storage API
- IAM API
- Compute Engine API
- Kubernetes Engine API

Additionally, ensure that the following permissions are granted to the logged-in user (at a minimum):

- `Storage Admin` - Required for creating and managing Google Cloud Storage buckets.
- `Service Account Admin` - Required for creating and managing service accounts.
- `Kubernetes Engine Admin` - Required for creating and managing Google Kubernetes Engine clusters and their resources.

To login into the Google cloud project using the command line, run the following command and run through the steps to authenticate:

``` bash
gcloud init
```

This allows you to perform necessary operations on Google Cloud - primarily, creating and managing infrastructure using Terraform.

## Getting started with Terraform

The following assumes that you are authenticated on the Google Cloud CLI, have Terraform installed on your machine, and are located within a directory that you wish to use as the root of the Terraform configuration.

First, make the following `terraform init` call:

    terraform init -from-module github.com/xtdb/xtdb.git//google-cloud/terraform

This will download the Terraform files from the XTDB repository, and initialize the working directory.

:::note
For the sake of this guide, we store Terraform state locally.
However, to persist the state onto Google Cloud, you will need to configure a remote backend using Google Cloud Storage.
This allows you to share the state file across teams, maintain versioning, and ensure consistency during deployments.
For more info, see the [**Terraform gcs backend**](https://developer.hashicorp.com/terraform/language/backend/gcs) documentation.
:::

## What is being deployed on Google Cloud?

The sample Terraform directory sets up a few distinct parts of the infrastructure required by XTDB. If using the default configuration, the following will be created:

- **IAM Service Account** for accessing required Google Cloud resources.
- **Google Cloud Storage Bucket** for remote storage.
    - Configured with associated resources using the [**GoogleCloud/storage-bucket**](https://registry.terraform.io/modules/terraform-google-modules/cloud-storage/google/latest) Terraform module.
    - Adds required permissions to the Service Account.
- **Virtual Private Cloud Network** for the XTDB GKE cluster.
    - Configured with associated resources using the [**GoogleCloud/network**](https://registry.terraform.io/modules/terraform-google-modules/network/google/latest) Terraform module.
- **Google Kubernetes Engine Cluster** for running the XTDB resources.
    - Configured with associated resources using the [**GoogleCloud/kubernetes-engine**](https://registry.terraform.io/modules/terraform-google-modules/kubernetes-engine/google/latest) Terraform module.

The above infrastructure is designed for creating a starting point for running XTDB on Google Cloud & Kubernetes.
The VM sizes and resource tiers can & should be adjusted to suit your specific requirements and cost constraints, and the templates should be configured with any desired changes to security or networking configuration.

### GKE Machine Types

By default, our terraform templates will create the GKE Cluster with:

- A single node default node pool
- A three node application node pool spread across three zones.
- All nodes using the `n2-highmem-2` machine type (`node_machine_type` in `terraform.tfvars`).

`n2-highmem-2` is intended to work on most projects/accounts.
Dependent on your project limits, you may wish to adjust this machine type:

- We would recommend a machine type better suited for database loads, such as the `c3-*-lssd` instances.
- For more information on the available machine types and their optimal workloads, see the [**Google Cloud Documentation**](https://cloud.google.com/compute/docs/general-purpose-machines).

## Deploying the Google Cloud Infrastructure

Before creating the Terraform resources, review and update the `terraform.tfvars` file to ensure the parameters are correctly set for your environment:

- You are **required** to set the `project_id` parameter to the Google Cloud project ID you wish to deploy into.
- You are also **required** to set a unique and valid `storage_bucket_name` for your environment.
- You may also wish to change resource tiers, the location of the resource group, or the VM sizes used by the Google Cloud cluster.

To get a full list of the resources that will be deployed by the templates, run:

``` bash
terraform plan
```

Finally, to create the resources, run:

``` bash
terraform apply
```

This will create the necessary infrastructure on the Google Cloud Project.

### Fetching the Terraform Outputs

The Terraform templates will generate several outputs required for setting up the XTDB nodes on the GKE cluster.

To retrieve these outputs, execute the following command:

``` bash
terraform output
```

This will return the following outputs:

- `project_id` - The Google Cloud project ID.
- `bucket_name` - The name of the Google Cloud Storage bucket.
- `iam_service_account_email` - The email address of the IAM service account.

## Deploying on Kubernetes

With the infrastructure created on Google Cloud, we can now deploy the XTDB nodes and a simple Kafka instance on the Google Kubernetes Engine cluster.

Prior to deploying the Kubernetes resources, ensure that the `kubectl` CLI is installed and configured to interact with the GKE cluster.
Run the following command:

``` bash
gcloud container clusters get-credentials xtdb-cluster --region us-central1
```

:::note
The above will require `gke-gcloud-auth-plugin` to be installed - see instructions [**here**](https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke).
:::

Now that `kubectl` is authenticated with the GKE cluster, you can set up the namespace for the XTDB deployment:

``` bash
kubectl create namespace xtdb-deployment
```

The GKE cluster is now ready for deployment,

### Deploying an example Kafka

To deploy a basic set of Kafka resources within GKE, you can make use of the `bitnami/kafka` Helm chart.
Run the following command:

``` bash
helm install kafka oci://registry-1.docker.io/bitnamicharts/kafka
  --version 31.3.1
  --namespace xtdb-deployment
  --set listeners.client.protocol=PLAINTEXT
  --set listeners.controller.protocol=PLAINTEXT
  --set controller.resourcesPreset=medium
  --set controller.nodeSelector.node_pool=xtdb-pool
```

This command will create:

- A simple, **unauthenticated** Kafka deployment on the GKE cluster, which XTDB will use as its message log, along with its dependent infrastructure and persistent storage.
- A Kubernetes service to expose the Kafka instance to the XTDB cluster.

#### Considerations of the Kafka Deployment

The Kafka instance set up above is for **demonstration purposes** and is **not recommended for production use**.
This example lacks authentication for the Kafka cluster and allows XTDB to manage Kafka topic creation and configuration itself.

For production environments, consider the following:

- Use a more robust Kafka deployment.
- Pre-create the required Kafka topics.
- Configure XTDB appropriately to interact with the production Kafka setup.

Additional resources:

- For further configuration options for the Helm chart, refer to the [**Bitnami Kafka Chart Documentation**](https://artifacthub.io/packages/helm/bitnami/kafka).
- For detailed configuration guidance when using Kafka with XTDB, see the [**XTDB Kafka Setup Documentation**](https://docs.xtdb.com/ops/config/log/kafka.html#setup).

### Verifying the Kafka Deployment

After deployment, verify that the Kafka instance is running properly by checking its status and logs.

To check the status of the Kafka deployment, run the following command:

``` bash
kubectl get pods --namespace xtdb-deployment
```

To view the logs of the Kafka deployment, use the command:

``` bash
kubectl logs -f statefulset/kafka-controller --namespace xtdb-deployment
```

By verifying the status and reviewing the logs, you can ensure the Kafka instance is correctly deployed and ready for use by XTDB.

### Setting up the XTDB Workload Identity

In order for the XTDB nodes to access the Google Cloud Storage bucket, we need to set up a Kubernetes Service Account that can access the Google Cloud IAM service account using [**Workload Identity Federation**](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#using_from_your_code).

To set up the Kubernetes Service Account, run the following command:

``` bash
kubectl create serviceaccount xtdb-service-account --namespace xtdb-deployment
```

We fetch the IAM service account email from the Terraform outputs, `iam_service_account_email`.
To create an IAM allow policy that gives the Kubernetes ServiceAccount access to impersonate the IAM service account, run the following command:

``` bash
gcloud iam service-accounts add-iam-policy-binding <iam_service_account_email>
  --role roles/iam.workloadIdentityUser
  --member "serviceAccount:<project_id>.svc.id.goog[xtdb-deployment/xtdb-service-account]"
```

The member name must include the namespace and Kubernetes ServiceAccount name.

Finally, annotate the Kubernetes ServiceAccount so that GKE sees the link between the service accounts:

``` bash
kubectl annotate serviceaccount xtdb-service-account
  --namespace xtdb-deployment
  iam.gke.io/gcp-service-account=<iam_service_account_email>
```

With the XTDB service account set up, we can now deploy the XTDB cluster to the GKE cluster.

### Deploying the XTDB cluster

In order to deploy the XTDB cluster and it's constituent parts into the GKE cluster, we provide an `xtdb-google-cloud` Helm chart/directory.

This can be found on the [**XTDB Github Container Registry**](https://github.com/xtdb/xtdb/pkgs/container/helm-xtdb-google-cloud), and can be used directly with `helm` commands.

With the values from the [Terraform outputs](#terraform-outputs), you can now deploy the XTDB cluster.
Run the following command, substituting the values as appropriate:

``` bash
helm install xtdb-google-cloud oci://ghcr.io/xtdb/helm-xtdb-google-cloud
  --version 2.0.0-snapshot
  --namespace xtdb-deployment
  --set xtdbConfig.serviceAccount=xtdb-service-account
  --set xtdbConfig.gcpProjectId=<project_id>
  --set xtdbConfig.gcpBucket=<bucket_name>
```

The following are created by the templates:

- A `ConfigMap` containing the XTDB YAML configuration.
- A `StatefulSet` containing the XTDB nodes.
- A `LoadBalancer` Kubernetes service to expose the XTDB cluster to the internet.

To check the status of the XTDB statefulset, run:

``` bash
kubectl get statefulset --namespace xtdb-deployment
```

To view the logs of the first StatefulSet member, run:

``` bash
kubectl logs -f xtdb-statefulset-0 --namespace xtdb-deployment
```

#### Customizing the XTDB Deployment

The above deployment uses the `helm-xtdb-google-cloud` chart defaults, individually setting the terraform outputs as `xtdbConfig` settings using the command line.

For more information on the available configuration options and fetching the charts locally for customization, see the [`helm-xtdb-google-cloud` Helm documentation](/ops/google-cloud#helm)

### Accessing the XTDB Cluster

:::note
As it will take some time for the XTDB nodes to be marked as ready (as they need to pass their initial startup checks) it may take a few minutes for the XTDB cluster to be accessible.
:::

:::note
The xtdb service is only available via ClusterIP by default so as to not expose the service publicly
:::

Once the XTDB cluster is up and running, you can access it via the ClusterIP service that was created.

To port forward the service locally

``` bash
kubectl port-forward service/xtdb-service --namespace xtdb-deployment 8080:8080
```

You can do the same for the following components:

- Postgres Wire Server (on port `5432`)
- Healthz Server (on port `8080`)

To check the status of the XTDB cluster using the forwarded port, run:

``` bash
curl http://localhost:8080/healthz/alive

## alternatively `/healthz/started`, `/healthz/ready`
```

If the above command succeeds, you now have a running XTDB cluster.
