STYLESDIR = styles
STYLESHEET = juxt.css

index.html:	*.adoc ${STYLESDIR}/${STYLESHEET}
	printf "`cat 'index.adoc'`\\n\\n`sed 's/\*\s<<\(.*\)\#.*/include::\1[]\n\n/' nav.adoc`" | asciidoctor -d book -a docdir=. -o index.html -

docs.zip: index.html javadoc
	zip -r docs.zip index.html img/* javadoc/*

%.adoc:		%.md
	pandoc --atx-headers $< -o $@

crux.epub:	index.html
	pandoc $< -o $@

javadoc:
	javadoc -cp `cd ..; lein classpath` -d $@ -sourcepath ../src -exclude crux crux.api

clean:
	rm -rf index.html javadoc docs.zip
