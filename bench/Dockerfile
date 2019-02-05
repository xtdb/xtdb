FROM clojure

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY project.clj /usr/src/app/
RUN lein deps
COPY . /usr/src/app
RUN apt-get update
RUN apt-get install -y awscli
RUN bench/bin/download_watdiv_files.sh
CMD bench/bin/run_watdiv.sh
