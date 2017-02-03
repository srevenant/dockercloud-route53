#!/bin/bash

cfg=$(json_reformat -m < $1)
echo "CONFIG=$(echo $cfg | base64 -w 0)"
