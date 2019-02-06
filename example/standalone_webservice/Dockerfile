FROM clojure

RUN mkdir -p /usr/src/app
RUN mkdir -p /usr/src/crux
COPY . /usr/src/crux
WORKDIR /usr/src/crux
RUN lein install
WORKDIR /usr/src/app
COPY example/standalone_webservice/project.clj /usr/src/app/
RUN lein deps
COPY example/standalone_webservice/. /usr/src/app
RUN rm -rf data
CMD lein run
