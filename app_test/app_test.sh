#!/bin/sh

python ../TAP/tapquery.py --sql "select ra, dec from ps where dec between -5 and 5" --filename app_test.tbl
