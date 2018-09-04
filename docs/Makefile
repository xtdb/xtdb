STYLESDIR = styles
STYLESHEET = crux.css

index.html:	*.adoc ${STYLESDIR}/${STYLESHEET}
	asciidoctor -d book -a "webfonts!" -a stylesdir=${STYLESDIR} -a stylesheet=${STYLESHEET} index.adoc

docs.zip: index.html
	zip docs.zip index.html

%.adoc:		%.md
	pandoc --atx-headers $< -o $@

crux.epub:	index.html
	pandoc $< -o $@

deploy: docs.zip
	scp docs.zip "prod@juxt.pro:/srv/website-static/crux/$$(date +%s).zip"
