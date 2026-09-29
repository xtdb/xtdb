// Run from the repo root, naming the target explicitly so `docker buildx bake` (no args)
// doesn't accidentally rebuild/push everything as more targets get added here:
//   docker buildx bake <target>              # build only
//   docker buildx bake <target> --push       # build and push to ghcr.io
//
// Multi-arch push needs a buildx builder with both linux/amd64 and linux/arm64 — set one up
// once with `docker buildx create --use --bootstrap` (and QEMU, if not already installed).

variable "GIT_SHA" {
  default = ""
}

variable "XTDB_VERSION" {
  default = "dev"
}

// The XTDB images — CD overrides their tags and labels with docker/metadata-action's bake files.
// The uberjars come from `./gradlew :docker:<variant>:shadowJar`, which has to run first.

group "release" {
  targets = ["standalone", "cloud"]
}

target "_xtdb" {
  context = "."
  dockerfile = "docker/Dockerfile"
  platforms = ["linux/amd64", "linux/arm64/v8"]
  args = {
    GIT_SHA = GIT_SHA
    XTDB_VERSION = XTDB_VERSION
  }
}

target "standalone" {
  inherits = ["_xtdb"]
  tags = ["ghcr.io/xtdb/xtdb:dev"]
  args = {
    VARIANT = "standalone"
  }
}

target "cloud" {
  name = cloud.variant
  matrix = {
    cloud = [
      { variant = "aws", log_template = "classpath:xtdb/logging/AwsCloudWatchLayout.json" },
      { variant = "azure", log_template = "classpath:xtdb/logging/AzureMonitorLayout.json" },
      { variant = "google-cloud", log_template = "classpath:GcpLayout.json" },
    ]
  }
  inherits = ["_xtdb"]
  tags = ["ghcr.io/xtdb/xtdb-${cloud.variant}:dev"]
  args = {
    VARIANT = cloud.variant
    XTDB_LOG_JSON_TEMPLATE = cloud.log_template
  }
}

target "bench" {
  context = "."
  dockerfile = "modules/bench/Dockerfile"
  tags = ["ghcr.io/xtdb/xtdb-bench:latest"]
}

// xtdb-builder — a pre-built multi-arch image containing a custom JRE, used as the
// `jlink` stage of docker/Dockerfile so CD doesn't re-run `./gradlew buildCustomJre`
// (5+ minutes under arm64 emulation) on every build. Bump BUILDER_TAG (and the
// BUILDER_IMAGE default in docker/Dockerfile) when build-logic/jlink/ or the JDK base
// image changes.

variable "BUILDER_TAG" {
  default = "20260926-1"
}

target "builder" {
  context = "."
  dockerfile = "docker/builder.Dockerfile"
  platforms = ["linux/amd64", "linux/arm64"]
  tags = ["ghcr.io/xtdb/xtdb-builder:${BUILDER_TAG}"]
}
