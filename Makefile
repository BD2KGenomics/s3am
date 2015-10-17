# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

define help

Supported targets: 'develop', 'sdist', 'clean', 'test', 'pypi', or 'pypi_stable'.

The 'develop' target creates an editable install (aka develop mode).

The 'sdist' target creates a source distribution of S3AM suitable.

The 'clean' target undoes the effect of 'develop', 'sdist' and 'pypi'.

The 'test' target runs S3AM's unit tests.

The 'pypi' target publishes the current commit of S3AM to PyPI after enforcing that the working
copy and the index are clean, and tagging it as an unstable .dev build.

The 'pypi_stable' target is like 'pypi' except that it doesn't tag the build as an unstable build.
IOW, it publishes a stable release.

endef
export help
help:
	@echo "$$help"


python=python2.7

green=\033[0;32m
normal=\033[0m
red=\033[0;31m


develop: _check_venv
	$(python) setup.py egg_info develop


clean_develop: _check_venv
	- $(python) setup.py develop -u
	- rm -rf src/*.egg-info


sdist:
	$(python) setup.py sdist


clean_sdist:
	- rm -rf dist


test:
	# https://github.com/pytest-dev/pytest/issues/1143
	$(python) setup.py test --pytest-args "-vv --assert=plain src"


pypi: _check_clean_working_copy _check_running_on_jenkins
	@test "$$(git rev-parse --verify remotes/origin/master)" != "$$(git rev-parse --verify HEAD)" \
		&& echo "Not on master branch, silently skipping deployment to PyPI." \
		|| $(python) setup.py egg_info --tag-build=.dev$$BUILD_NUMBER sdist bdist_egg upload


pypi_stable: _check_clean_working_copy _check_running_on_jenkins
	test "$$(git rev-parse --verify remotes/origin/master)" != "$$(git rev-parse --verify HEAD)" \
		&& echo "Not on master branch, silently skipping deployment to PyPI." \
		|| $(python) setup.py egg_info register sdist bdist_egg upload


clean_pypi:
	- rm -rf build/


clean: clean_develop clean_sdist clean_pypi


_check_venv:
	@$(python) -c 'import sys; sys.exit( int( not hasattr(sys, "real_prefix") ) )' \
		|| ( echo "$(red)A virtualenv must be active.$(normal)" ; false )


_check_clean_working_copy:
	@echo "$(green)Checking if your working copy is clean ...$(normal)"
	git diff --exit-code > /dev/null \
		|| ( echo "$(red)Working copy looks dirty.$(normal)" ; false )
	git diff --cached --exit-code > /dev/null \
		|| ( echo "$(red)Index looks dirty.$(normal)" ; false )
	test -z "$$(git ls-files --other --exclude-standard --directory)" \
		|| ( echo "$(red)Untracked files:$(normal)" \
			; git ls-files --other --exclude-standard --directory \
			; false )


_check_running_on_jenkins:
	@echo "$(green)Checking if running on Jenkins ...$(normal)"
	test -n "$$BUILD_NUMBER" \
		|| ( echo "$(red)This target should only be invoked on Jenkins.$(normal)" ; false )


.PHONY: help develop clean_develop sdist clean_sdist test 
		pypi pypi_stable clean_pypi clean 
		_check_venv _check_clean_working_copy _check_running_on_jenkins 