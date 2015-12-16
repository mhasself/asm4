ifneq ($(wildcard Makefile.local),)
include Makefile.local
endif

default: .build

.build:
	python setup.py build

install:
	python setup.py install --prefix=$(PREFIX)

clean:
	-python setup.py clean
	-rm -r build
