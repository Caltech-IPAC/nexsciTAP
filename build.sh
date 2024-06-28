#!/bin/sh

rm -f dist/*.whl

make_local.sh

pip uninstall -y nexsciTAP

pip install dist/*

rm -f /tmp/tap*
rm -f /tmp/writerec*
