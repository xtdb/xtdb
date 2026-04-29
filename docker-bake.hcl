// Run from the repo root, naming the target explicitly so `docker buildx bake` (no args)
// doesn't accidentally rebuild/push everything as more targets get added here:
//   docker buildx bake <target>              # build only
//   docker buildx bake <target> --push       # build and push to ghcr.io
//
// Multi-arch push needs a buildx builder with both linux/amd64 and linux/arm64 — set one up
// once with `docker buildx create --use --bootstrap` (and QEMU, if not already installed).

target "bench" {
  context = "modules/bench"
  tags = ["ghcr.io/xtdb/xtdb-bench:latest"]
}

// xtdb-builder — a pre-built multi-arch image containing a custom JRE, used as the
// `jlink` stage of docker/Dockerfile so CD doesn't re-run `./gradlew buildCustomJre`
// (5+ minutes under arm64 emulation) on every build. Bump BUILDER_TAG (and the
// BUILDER_IMAGE default in docker/Dockerfile) when build-logic/jlink/ or the JDK base
// image changes.

variable "BUILDER_TAG" {
  default = "20260429-1"
}

target "builder" {
  context = "."
  dockerfile = "docker/builder.Dockerfile"
  platforms = ["linux/amd64", "linux/arm64"]
  tags = ["ghcr.io/xtdb/xtdb-builder:${BUILDER_TAG}"]
}
