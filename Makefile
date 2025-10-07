REPO=	fossil info | grep ^repository | awk '{print $$2}'

test: tests/run
	./tests/run

simulate: examples/simulate
	./examples/simulate

poule.so: poule.scm
	chicken-install

examples/simulate: poule.so examples/simulate.scm
	chicken-csc examples/simulate.scm

tests/run: poule.so tests/run.scm
	chicken-install test
	chicken-csc tests/run.scm

docs/poule.html: docs/poule.wiki
	chicken-install svnwiki2html && \
	svnwiki2html --title poule --css ./css.css docs/poule.wiki > docs/poule.html && \
	sed -i '' "1s|^|<div class='fossil-doc' data-title='Home'>\n|" docs/poule.html

clean:
	fossil clean

git:
	@if [ -e git-import ]; then \
	    echo "The 'git-import' directory already exists"; \
	    exit 1; \
	fi; \
	git init -b main git-import && cd git-import && \
	fossil export --git --rename-trunk main --repository `${REPO}` | \
	git fast-import && git reset --hard HEAD && \
	git remote add origin git@github.com:gahr/poule.git && \
	git push -f origin main && \
	cd .. && rm -rf git-import

