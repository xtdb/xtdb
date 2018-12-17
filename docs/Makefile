STYLESDIR = styles
STYLESHEET = juxt.css

index.html:	*.adoc ${STYLESDIR}/${STYLESHEET}
	asciidoctor -d book -a "webfonts!" -a stylesdir=${STYLESDIR} -a stylesheet=${STYLESHEET} index.adoc

docs.zip: index.html
	zip -r docs.zip index.html img/*

%.adoc:		%.md
	pandoc --atx-headers $< -o $@

crux.epub:	index.html
	pandoc $< -o $@

clean:
	rm index.html
