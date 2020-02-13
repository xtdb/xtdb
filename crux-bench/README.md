## Downloading the datasets

In order to run the benchmarks locally, you need the required datasets. Within the base of this directory, you can run the `./bin/download_dataset.sh` script to download them into the **data** folder.

## Running the benchmark locally

With the datasets downloaded, you can run the benchmark using `lein run -m crux.main.bench`. This will run a [**Watdiv**](https://dsg.uwaterloo.ca/watdiv/) benchmark, and a benchmark using the [**DevOps devices** and **Weather** datasets](https://docs.timescale.com/v1.2/tutorials/other-sample-datasets#in-depth-devices). The Watdiv benchmark against Crux will attempt to load results from other database benchmarks from AWS S3, so credentials are required if you wish to have comparison data in the outputs.

## Setting up AWS Credentials

If you wish to save to / load from S3 on your local benchmarks (used within the various Watdiv benchmarks) or to trigger AWS tasks locally, you will first need to set up AWS credentials. See further information on the [AWS documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

## Nightly benchmarks

Every push on `master` triggers a deployment job on CircleCI, building a new docker image and uploading it to the **Elastic Container Registry** on AWS. The latest of these `master` images is ran at midnight every single weekday.

## Manually running a benchmark on AWS

Assuming you have the `awscli`, and you are authenticated with AWS and the ECR (see [**here**](https://docs.aws.amazon.com/cli/latest/reference/ecr/get-login.html)), you can use the `bin/docker-build-push.sh` script to push a docker image (tagged `latest`) to the `crux-bench` ECR repository. After this, you can trigger a task to run the benchmark using the `bin/run_task.sh`.

## Running Non-Crux Benchmarks

There are a number of other benchmarks for Watdiv on different databases, using our benchmark harness. We generally wish to run these on AWS, and use their results for comparison against Crux. They are ran based off of the `latest` docker image pushed to AWS, passing a command to docker to override the default main namespace (which runs the Crux benchmark) with whichever namespace you wish to run. There are scripts to run these task overrides under `bin`.