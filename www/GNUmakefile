
all: index.html muchsync.html logo.png

CSS = http://netdna.bootstrapcdn.com/bootstrap/3.1.1/css/bootstrap.min.css \
        local.css

PANDOC = pandoc -s -t html5 --template=template.html \
	$(patsubst %, --css=%, $(CSS))

DEPS = template.html GNUmakefile

index.html: index.md $(DEPS)
	$(PANDOC) -Vskiptitle:1 -o $@ $<

muchsync.html: ../muchsync.1.md $(DEPS)
	$(PANDOC) -o $@ $<

logo.png: logo.svg GNUmakefile
	inkscape -e $@ $<

clean:
	rm -f *~ index.html muchsync.html
