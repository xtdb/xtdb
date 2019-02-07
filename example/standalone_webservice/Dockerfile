FROM clojure

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY project.clj project.clj
RUN lein deps
COPY . /usr/src/app/
RUN lein deps # to refresh any -SNAPSHOT versions!
CMD lein run
