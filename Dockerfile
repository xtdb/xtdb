FROM openjdk:15-alpine

WORKDIR /usr/local/lib/crux

RUN apk --no-cache add curl

# TODO allow customising Xms/Xmx etc
ENTRYPOINT ["java", \
    "-Dclojure.main.report=stderr", \
    "-Xms3g","-Xmx3g", \
    "-jar","core2-standalone.jar"]

HEALTHCHECK --start-period=15s --timeout=3s \
    CMD curl -f http://localhost:3000/status || exit 1

ADD docker/core2.edn core2.edn
ADD target/core2-standalone.jar core2-standalone.jar
