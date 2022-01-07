#!/bin/bash

SCRIPTS_DIR=$(dirname "$(realpath $0)")
FUSE_FUNCS="$SCRIPTS_DIR/fuse_v1.sh"

echo "Mounting S3:$3 $1 -> $2"

source "$FUSE_FUNCS"
s3fs_mount $1 $2 $3 $4 $5 $6