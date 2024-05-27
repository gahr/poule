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

clean:
	fossil extras | awk '!/Makefile/' | xargs rm
