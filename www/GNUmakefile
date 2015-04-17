
all: index.html muchsync.html

CSS = http://netdna.bootstrapcdn.com/bootstrap/3.1.1/css/bootstrap.min.css \
        local.css

PANDOC = pandoc -s -t html5 --template=template.html \
	$(patsubst %, --css=%, $(CSS)) -o $@ $<

DEPS = template.html GNUmakefile

index.html: index.md $(DEPS)
	$(PANDOC)

muchsync.html: ../muchsync.1.md $(DEPS)
	$(PANDOC)

clean:
	rm -f *~ index.html muchsync.html
