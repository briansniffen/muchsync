
all: out/index.html out/muchsync.html out/logo.png out/muchsync.1

CSS = https://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css

PANDOC = pandoc -S -s -t html5 --template=template.html \
	$(patsubst %, --css=%, $(CSS))

DEPS = template.html GNUmakefile

out/index.html: index.md $(DEPS)
	@mkdir -p out
	$(PANDOC) -Vskiptitle:1 -o $@ $<

out/muchsync.html: ../muchsync.1.md $(DEPS)
	@mkdir -p out
	$(PANDOC) -o $@ $<

out/logo.png: logo.svg
	@mkdir -p out
	inkscape -e $@ $<

out/muchsync.1: ../muchsync.1.md
	@mkdir -p out
	$(MAKE) -C.. muchsync.1
	cp ../muchsync.1 $@

clean:
	rm -rf *~ out
