virtualenv venv
. venv/bin/activate
make develop
export PYTEST_ADDOPTS="--junitxml=test-report.xml"
make $make_targets
