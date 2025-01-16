# XTDB Azure Helm Chart

This Helm chart deploys a simple XTDB cluster, backed by Azure infrastructure, on Kubernetes.

## Required Infrastructure

- Azure Blob Storage Account/Container for use as the Storage module.
- A User Assigned Managed Identity with access to use the Storage Account/Container
- A connectable Kafka Cluster for the Transaction Log.
- Federated Identity Credential setup for desired Kubernetes Namespace/Service account to give access to the User Assigned Managed Identity 

## Resources

The following are created by the templates:

* A `StatefulSet` containing the XTDB nodes.
* A `PersistentVolumeClaim` for each member of the `StatefulSet` (default size of 50 GiB, default storage class of [**managed-csi**](https://learn.microsoft.com/en-us/azure/aks/azure-disk-csi#dynamically-create-azure-disks-pvs-by-using-the-built-in-storage-classes)).
* A `LoadBalancer` Kubernetes service to expose the XTDB cluster to the internet.
* A `ClusterIP` service for exposing the **Prometheus** metrics from the nodes.
* A `ServiceAccount` used for authenticating the XTDB nodes with the Azure Storage Account (setup with Federated Identity Credential).
