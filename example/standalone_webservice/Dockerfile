FROM clojure:openjdk-11-lein-2.9.0 as standalone-demo-with-volume

RUN apt-get update
RUN apt-get install -y awscli

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY project.clj project.clj
RUN lein deps
COPY . /usr/src/app/
RUN rm -rf /usr/src/app/data
RUN rm -rf /usr/src/app/backup
CMD lein run
