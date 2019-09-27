# Migrations Toolbelt

[![Build Status](https://dev.azure.com/isogeo/PythonTooling/_apis/build/status/isogeo.migrations-toolbelt?branchName=master)](https://dev.azure.com/isogeo/PythonTooling/_build/latest?definitionId=37&branchName=master)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Documentation Status](https://readthedocs.org/projects/migrations-toolbelt/badge/?version=latest)](https://migrations-toolbelt.readthedocs.io/en/latest/?badge=latest)

Set of tools and scripts to perform metadata migrations in Isogeo database using the API and its Python wrapper.

## Requirements

- Python 3.6+
- Isogeo Python SDK >= 3.0.0
- Authentication :
  - an user with `staff` status
  - [Legacy Application Flow credentials](https://requests-oauthlib.readthedocs.io/en/latest/oauth2_workflow.html#legacy-application-flow)

## Configuration

Rename the `.env.example` file into `.env`, then complete it with required information.

## Development

Using `pip`:

```powershell
# create virtual env
py -3 -m venv .venv
# activate it
.\.venv\Scripts\activate
# update basic tooling
python -m pip install -U pip setuptools wheel
# install requirements
python -m pip install -U -r ./requirements.txt
# install package for development
python -m pip install --editable .
```

Using `pipenv`:

```powershell
# install/upgrade pip and pipenv
python -m pip install -U pip pipenv
# install requirements
python -m pipenv install --dev
```
