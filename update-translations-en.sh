#!/bin/bash

set -Eeuxo pipefail

# Some of these change their output when run multiple times..
pybabel extract --omit-header -F babel.cfg -o messages.pot .
pybabel update -l en --no-wrap --omit-header -i messages.pot -d allthethings/translations --no-fuzzy-matching
pybabel compile -l en -f -d allthethings/translations
