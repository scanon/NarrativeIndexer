#!/bin/bash
script_dir=$(dirname "$(readlink -f "$0")")
export KB_DEPLOYMENT_CONFIG=$script_dir/../deploy.cfg
export KB_AUTH_TOKEN=`cat /kb/module/work/token`
export PYTHONPATH=$script_dir/../$(LIB_DIR):$PATH:$PYTHONPATH
cd $script_dir/../$(TEST_DIR)
python -m nose --with-coverage --cover-package=$(SERVICE_CAPS) --cover-html --cover-html-dir=/kb/module/work/test_coverage --nocapture  --nologcapture .
