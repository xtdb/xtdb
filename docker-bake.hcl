// Docker Bake file for XTDB images.
// Tags/labels are provided at build time via the metadata-action bake files.

variable "GIT_SHA" {
  default = ""
}

variable "XTDB_VERSION" {
  default = "dev"
}

group "default" {
  targets = ["standalone", "cloud"]
}

target "standalone" {
  context    = "."
  dockerfile = "docker/Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64/v8"]
  tags       = ["ghcr.io/xtdb/xtdb:dev"]
  args = {
    VARIANT      = "standalone"
    GIT_SHA      = GIT_SHA
    XTDB_VERSION = XTDB_VERSION
  }
}

target "cloud" {
  name       = variant
  matrix = {
    variant = ["aws", "azure", "google-cloud"]
  }
  context    = "."
  dockerfile = "docker/Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64/v8"]
  tags       = ["ghcr.io/xtdb/xtdb-${variant}:dev"]
  args = {
    VARIANT      = variant
    GIT_SHA      = GIT_SHA
    XTDB_VERSION = XTDB_VERSION
  }
}

target "bench" {
  context    = "."
  dockerfile = "modules/bench/Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64/v8"]
  tags       = ["ghcr.io/xtdb/xtdb-bench:latest"]
  args = {
    GIT_SHA = GIT_SHA
  }
}
