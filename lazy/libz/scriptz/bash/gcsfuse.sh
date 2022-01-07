#!/bin/bash

SCRIPTS_DIR=$(dirname "$(realpath $0)")
FUSE_FUNCS="$SCRIPTS_DIR/fuse_v1.sh"

echo "Mounting GCS: $1 -> $2"
source "$FUSE_FUNCS"

gcsfuse_mount $1 $2