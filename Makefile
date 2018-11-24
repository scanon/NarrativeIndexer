SERVICE = narrativeindexer
SERVICE_CAPS = NarrativeIndexer
SPEC_FILE = NarrativeIndexer.spec
URL = https://kbase.us/services/narrativeindexer
DIR = $(shell pwd)
LIB_DIR = lib
SCRIPTS_DIR = scripts
TEST_DIR = test
LBIN_DIR = bin
TEST_SCRIPT_NAME = run_tests.sh

.PHONY: test

default: compile

all: compile build

compile:
	kb-sdk compile $(SPEC_FILE) \
		--out $(LIB_DIR) \
		--pyclname $(SERVICE_CAPS).$(SERVICE_CAPS)Client \
		--pysrvname $(SERVICE_CAPS).$(SERVICE_CAPS)Server \
		--pyimplname $(SERVICE_CAPS).$(SERVICE_CAPS)Impl;

build:
	chmod +x $(SCRIPTS_DIR)/entrypoint.sh

test:
	if [ ! -f /kb/module/work/token ]; then echo -e '\nOutside a docker container please run "kb-sdk test" rather than "make test"\n' && exit 1; fi
	bash $(TEST_DIR)/$(TEST_SCRIPT_NAME)

ltest:
	export SDK_CALLBACK_URL=bogus ; \
	export KB_DEPLOYMENT_CONFIG=$(DIR)/deploy-local.cfg  ; \
	PYTHONPATH=../lib/ python -m nose --with-coverage --cover-package=$(SERVICE_CAPS),Utils,Indexers --cover-html --cover-html-dir=./test_coverage --nocapture  --nologcapture

clean:
	rm -rfv $(LBIN_DIR)
