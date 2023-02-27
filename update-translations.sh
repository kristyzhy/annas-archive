#!/bin/bash

set -Eeuxo pipefail

# Some of these change their output when run multiple times..
pybabel extract --omit-header -F babel.cfg -o messages.pot .
pybabel extract --omit-header -F babel.cfg -o messages.pot .
pybabel extract --omit-header -F babel.cfg -o messages.pot .
pybabel update --no-wrap --omit-header -i messages.pot -d allthethings/translations --no-fuzzy-matching
pybabel update --no-wrap --omit-header -i messages.pot -d allthethings/translations --no-fuzzy-matching
pybabel update --no-wrap --omit-header -i messages.pot -d allthethings/translations --no-fuzzy-matching
pybabel compile -f -d allthethings/translations
pybabel compile -f -d allthethings/translations
pybabel compile -f -d allthethings/translations
