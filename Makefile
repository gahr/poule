test: tests/run
	./tests/run

simulate: examples/simulate
	./examples/simulate

poule.so: poule.scm
	chicken-install

examples/simulate: poule.so examples/simulate.scm
	chicken-csc examples/simulate.scm

tests/run: poule.so tests/run.scm
	chicken-csc tests/run.scm

docs/poule.html: docs/poule.wiki
	svnwiki2html --title poule --css ./css.css docs/poule.wiki > docs/poule.html
	sed -i '' "1s|^|<div class='fossil-doc' data-title='poule - CHICKEN extension to manage a pool of worker processes'>\n|" docs/poule.html

clean:
	fossil extras | awk '!/Makefile/' | xargs rm
