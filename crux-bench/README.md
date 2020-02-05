## Downloading the datasets

In order to run the benchmarks locally, you need the required datasets. Within the base of this directory, you can run the `./bin/download_dataset.sh` script to download them into the **data** folder.

## Running the benchmark locally

With the datasets downloaded, you can run the benchmark using `lein run -m crux.main.bench`. This will run a [**Watdiv**](https://dsg.uwaterloo.ca/watdiv/) benchmark, and a benchmark using the [**DevOps devices** and **Weather** datasets](https://docs.timescale.com/v1.2/tutorials/other-sample-datasets#in-depth-devices).

## Nightly benchmarks
Every push on `master` triggers a deployment job on CircleCI, building a new docker image and uploading it to the **Elastic Container Registry** on AWS. The latest of these `master` images is ran at midnight every single weekday.

## Manually running a benchmark on AWS

Assuming you have the `awscli` are authenticated with AWS and the ECR (see [**here**](https://docs.aws.amazon.com/cli/latest/reference/ecr/get-login.html)), you can use the `bin/docker-build-push.sh` script to push a docker image (tagged `latest`) to the `crux-bench` ECR repository. After this, you can trigger a task to run the benchmark using the `bin/run_task.sh`.
