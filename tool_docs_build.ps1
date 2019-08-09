#############################################################################
#
# Script to build documentation of Isogeo Python package.
#
# Prerequisites: download markupsafe and pyyaml wheels into libs subdirectory
#
#############################################################################

# virtual env and dependencies
"-- STEP -- Install and display dependencies within the virtualenv"
python -m pip install -U pip pipenv
python -m pipenv install --dev
Set-Location -Path "./docs"

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
Set-Location -Path ".."
