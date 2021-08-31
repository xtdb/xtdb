FROM clojure:openjdk-11-tools-deps

WORKDIR /usr/lib/xtdb
VOLUME /var/lib/xtdb
ENTRYPOINT ["clojure", "-m", "xtdb.main"]
EXPOSE 3000

ADD deps.edn deps.edn
RUN mkdir /usr/lib/xtdb/resources
ADD resources/logback.xml resources/logback.xml
RUN clojure -Sforce -Spath >/dev/null

ENV MALLOC_ARENA_MAX=2
ADD xtdb.edn xtdb.edn
