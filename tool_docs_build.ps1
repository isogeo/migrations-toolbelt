#############################################################################
#
# Script to build documentation of Isogeo Python package.
#
# Prerequisites: download markupsafe and pyyaml wheels into libs subdirectory
#
#############################################################################

# make a virtualenv to perform packaging
Set-Location -Path "./docs"
"-- STEP -- Creating temp virtualenv to perform dependencies packaging"
py -3 -m venv .env3_docs
./env3_docs/Scripts/activate

# dependencies
"-- STEP -- Install and display dependencies within the virtualenv"
python -m pip install -U pip pipenv
pipenv install --dev

# remove previous builds
"-- STEP -- Clean up previous build"
rm -r _build/*

# build
"-- STEP -- Build docs"
sphinx-apidoc -e -f -M -o ".\_apidoc\" "..\migrations_toolbelt\"
./make.bat html

"-- STEP -- Get out the virtualenv"
deactivate
Invoke-Item _build/html/index.html
# rm -r env3_docs
Set-Location -Path ".."
