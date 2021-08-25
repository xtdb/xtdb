FROM clojure:openjdk-11-tools-deps

WORKDIR /usr/lib/crux
VOLUME /var/lib/crux
ENTRYPOINT ["clojure", "-m", "crux.main"]
EXPOSE 3000

ADD deps.edn deps.edn
RUN mkdir /usr/lib/crux/resources
ADD resources/logback.xml resources/logback.xml
RUN clojure -Sforce -Spath >/dev/null

ENV MALLOC_ARENA_MAX=2
ADD crux.edn crux.edn
