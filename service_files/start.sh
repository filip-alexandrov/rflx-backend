#!/bin/bash

./venv/bin/mitmdump --quiet --mode transparent \
 --allow-hosts '^(.+\.)?bloomberg\.com(:[0-9]+)?$' \
    --showhost -s /root/project/inspect_ios_reqs.py