PROJECT_NAME = de-project
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PIP:=pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

requirements: 
	$(call execute_in_env, $(PIP) install flake8 pytest pytest-mock autopep8 coverage safety bandit pycodestyle moto boto3 pandas awswrangler ccy freezegun psycopg2)


# Set Up
flake:
	$(call execute_in_env, $(PIP) install flake8)

autopep8:
	$(call execute_in_env, $(PIP) install autopep8)

pytest:
	$(call execute_in_env, $(PIP) install pytest)

pytest-mock:
	$(call execute_in_env, $(PIP) install pytest-mock)

coverage:
	$(call execute_in_env, $(PIP) install coverage)

safety:
	$(call execute_in_env, $(PIP) install safety)

bandit:
	$(call execute_in_env, $(PIP) install bandit)

boto3:
	$(call execute_in_env, $(PIP) install boto3)

moto:
	$(call execute_in_env, $(PIP) install moto)

pandas:
	$(call execute_in_env, $(PIP) install pandas)

pycodestyle:
	$(call execute_in_env, $(PIP) install pycodestyle)

ccy:
	$(call execute_in_env, $(PIP) install ccy)

freezegun:
	$(call execute_in_env, $(PIP) install freezegun)

awswrangler:
	$(call execute_in_env, $(PIP) install awswrangler)

psycopg2: 
	$(call execute_in_env, $(PIP) install psycopg2)


security-test:
	$(call execute_in_env, safety check -r ./requirements.txt)
	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)


format-code:
	$(call execute_in_env, autopep8 --in-place --aggressive --aggressive ./src/*/*.py)
	$(call execute_in_env, autopep8 --in-place --aggressive --aggressive ./test/*.py)


run-flake:
	$(call execute_in_env, flake8 --ignore=E501 ./src/*/*.py ./test/*.py)

unit-tests:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -vrP)

check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage run --omit 'venv/*' -m pytest && coverage report -m)


run-code-quality: security-test format-code run-flake

run-tests: unit-tests check-coverage

run-all-checks: security-test format-code run-flake unit-tests check-coverage