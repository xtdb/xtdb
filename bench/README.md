## Downloading the datasets

In order to run the benchmarks locally, you need the required datasets. Within the base of this directory, you can run the `./bin/download-dataset.sh` script to download them into the **data** folder.

## Running the benchmark locally

With the datasets downloaded, you can run the benchmark using `lein run -m xtdb.bench.main`. This will run a [**Watdiv**](https://dsg.uwaterloo.ca/watdiv/) benchmark, and a benchmark using the [**DevOps devices** and **Weather** datasets](https://docs.timescale.com/v1.2/tutorials/other-sample-datasets#in-depth-devices). The Watdiv benchmark against XTDB will attempt to load results from other database benchmarks from AWS S3, so credentials are required if you wish to have comparison data in the outputs.

You can run individual benchmarks at the REPL by passing them a node (`(user/xtdb-node)` works well for this, if you follow the instructions in the main README) - e.g. `(run-sorted-maps-microbench (user/xtdb-node))`.

## Setting up AWS Credentials

If you wish to save to / load from S3 on your local benchmarks (used within the various Watdiv benchmarks) or to trigger AWS tasks locally, you will first need to set up AWS credentials. See further information on the [AWS documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

## Nightly benchmarks

Every push on `master` triggers a deployment job on CircleCI, building a new docker image and uploading it to the **Elastic Container Registry** on AWS. The latest of these `master` images is ran at midnight every single weekday.

## Manually running a benchmark on AWS

Circle is responsible for building and pushing the bench docker images - it does this for every commit on the origin repo for branches with an active PR.

To deploy Docker images to ECR from your own branches, set up CircleCI to build your fork, and ensure you've added the four AWS env vars (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_ECR_ACCOUNT_URL`).

Assuming you have `awscli` and `jq` installed, and you are authenticated with AWS and ECR (see [**here**](https://docs.aws.amazon.com/cli/latest/reference/ecr/get-login.html)), you can run the bench for a CI-built commit using `bin/run-bench.sh [-r <COMMIT-ISH>] [--nodes node1,node2] [--tests test1,test2]`.

## Running Non-XTDB Benchmarks

There are a number of other benchmarks for Watdiv on different databases, using our benchmark harness.
We generally wish to run these on AWS, and use their results for comparison against XTDB.
They are ran based off of the `latest` docker image pushed to AWS, passing a command to docker to override the default main namespace (which runs the XTDB benchmark) with whichever namespace you wish to run.
There are scripts to run these task overrides under `bin`.

If running Neo4J, you'll need to include the `with-neo4j` Lein profile.
XTDB supports Java 8, so our CI runs using Java 8, but Neo4J 4.x doesn't - if you're running the Neo4J bench, you'll want to use Java 11, and `lein with-profile +with-neo4j ...`.
