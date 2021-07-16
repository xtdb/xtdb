FROM openjdk:15-alpine

WORKDIR /usr/local/lib/crux

# TODO allow customising Xms/Xmx etc
ENTRYPOINT ["java","-Dclojure.main.report=stderr","-Xms3g","-Xmx3g","-jar","core2-standalone.jar"]

ADD target/core2-standalone.jar core2-standalone.jar
