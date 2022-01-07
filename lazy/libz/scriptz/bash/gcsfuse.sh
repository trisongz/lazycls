#!/bin/bash

#BUCKET=$1
#MOUNT_PATH=$2

SCRIPTS_DIR=$(dirname "$(realpath $0)")
FUSE_FUNCS="$SCRIPTS_DIR/fuse_v1.sh"
source "$FUSE_FUNCS"

echo "Mounting $1 -> $2"
gcsfuse_mount $1 $2