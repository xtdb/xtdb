FROM clojure

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY project.clj project.clj
RUN lein deps
COPY . /usr/src/app/
CMD lein run
