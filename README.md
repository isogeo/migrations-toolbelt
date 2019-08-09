# Migrations Toolbelt

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

```powershell
# install/upgrade pip and pipenv
python -m pip install -U pip pipenv
# install requirements
python -m pipenv install --dev
```

<!-- ## Structure

This repository is organized in sub-projects.

### Duplicate

Used to completely duplicate many metadata. Usage example:

- copy the whole content of a workgroup into antoher one


### Import

Set of tools to import information  metadata 
 -->
