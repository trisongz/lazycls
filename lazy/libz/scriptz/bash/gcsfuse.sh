#!/bin/bash

BUCKET=$1
MOUNT_PATH=$2

SCRIPTS_DIR=$(dirname "$(realpath $0)")
FUSE_FUNCS="$SCRIPTS_DIR/fuse_v1.sh"

source "$FUSE_FUNCS"
echo "Mounting $BUCKET -> $MOUNT_PATH"
gcsfuse_mount $BUCKET $MOUNT_PATH