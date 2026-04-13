#!/bin/bash
DIR="$(cd "$(dirname "$0")" && pwd)"
CLIENT_SRC="$DIR/../../client/src"
PYTHONPATH="$CLIENT_SRC:$PYTHONPATH" python3 -m pytest "$DIR" "$@"
