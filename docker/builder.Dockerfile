# Builds a custom JRE via jlink and packages just the JRE into a scratch image.
# The resulting image is consumed by docker/Dockerfile as a `FROM ... AS jlink` source.
#
# Bump the tag in docker-bake.hcl AND the BUILDER_IMAGE default in docker/Dockerfile
# whenever build-logic/jlink/ or the JDK base image changes, then rebuild via
#   docker buildx bake builder --push
# from the repo root. See docker/README.adoc for full instructions.

FROM eclipse-temurin:21-jdk-alpine AS jlink
WORKDIR /build
COPY gradlew gradlew
COPY gradle/wrapper gradle/wrapper
COPY build-logic/jlink/settings.gradle.kts settings.gradle.kts
COPY build-logic/jlink/build.gradle.kts build.gradle.kts
RUN chmod +x gradlew && ./gradlew buildCustomJre --no-daemon

FROM scratch
COPY --from=jlink /build/build/custom-jre /opt/java
