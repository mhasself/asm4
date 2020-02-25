ifneq ($(wildcard Makefile.local),)
include Makefile.local
endif

PYTHON ?= python

default: .build

.build:
	$(PYTHON) setup.py build

install:
	$(PYTHON) setup.py install --prefix=$(PREFIX)

clean:
	-$(PYTHON) setup.py clean
	-rm -r build
