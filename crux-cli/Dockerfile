FROM openjdk:8

WORKDIR /usr/lib/crux

EXPOSE 3000
ENTRYPOINT ["java", "-jar", "crux-standalone.jar"]

ADD target/crux-standalone.jar .