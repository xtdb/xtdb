STYLESDIR = styles
STYLESHEET = juxt.css

index.html:	*.adoc ${STYLESDIR}/${STYLESHEET}
	printf "`cat 'index.adoc'`\\n\\n`sed 's/\*\s<<\(.*\)\#.*/include::\1[]\n\n/' nav.adoc`" | asciidoctor -d book -a "webfonts!" -a stylesdir=${STYLESDIR} -a stylesheet=${STYLESHEET} -a docdir=. -o index.html -

docs.zip: index.html javadoc
	zip -r docs.zip index.html img/* javadoc/*

%.adoc:		%.md
	pandoc --atx-headers $< -o $@

crux.epub:	index.html
	pandoc $< -o $@

javadoc: ../crux-core/src/crux/api/*.java
	javadoc -cp `cd ../crux-core; lein classpath` -d $@ -sourcepath ../crux-core/src -exclude crux crux.api

clean:
	rm -rf index.html javadoc docs.zip
