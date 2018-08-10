STYLESDIR = styles
STYLESHEET = crux.css

index.html:	*.adoc ${STYLESDIR}/${STYLESHEET}
	asciidoctor -d book -a "webfonts!" -a stylesdir=${STYLESDIR} -a stylesheet=${STYLESHEET} index.adoc

%.adoc:		%.md
	pandoc --atx-headers $< -o $@

crux.epub:	index.html
	pandoc $< -o $@
