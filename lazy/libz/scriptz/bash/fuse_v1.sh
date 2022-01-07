#!/bin/bash

SCRIPTS_DIR=$(dirname "$(realpath $0)")
BASE_FUNCS=${FUSE_FUNCS:-"$SCRIPTS_DIR/base_funcs.sh"}

source "$BASE_FUNCS"


################################################################
#####           Functions for FUSE_v1                    #######
################################################################

gcsfuse_mount() {
    BUCKET=$1
    MOUNT_PATH=$2
    gcsfuse_install
    if [[ "$BUCKET" != "" ]]; then
        if [[ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
            echo "No GOOGLE_APPLICATION_CREDENTIALS found at $GOOGLE_APPLICATION_CREDENTIALS. This may not work."
        fi
        mkdir -p "$MOUNT_PATH"
        if [[ "$(ls -A $MOUNT_PATH)" ]]; then
            echo "Another Bucket is already mounted at $MOUNT_PATH"
        else
            echo "Mounting Google Storage Bucket $BUCKET to $MOUNT_PATH"
            gcsfuse "$BUCKET" "$MOUNT_PATH"
        fi
    fi
}

s3fs_mount() {
    # Should be explicit
    BUCKET=$1
    MOUNT_PATH=$2
    S3_TYPE=$3

    # Inferred
    S3_ACCESS=$4
    S3_SECRET=$5
    ENDPOINT=$6

    S3FS_TYPE=${S3_TYPE:-aws}
    S3FS_ACCESS=${S3_ACCESS:-AWS_ACCESS_KEY_ID}
    S3FS_SECRET=${S3_SECRET:-AWS_SECRET_ACCESS_KEY}        

    if [[ "$S3FS_TYPE" == "minio" ]]; then
        S3FS_ACCESS=${S3_ACCESS:-MINIO_ACCESS_KEY}
        S3FS_SECRET=${S3_SECRET:-MINIO_SECRET_KEY}
        S3FS_ENDPOINT=${ENDPOINT:-MINIO_ENDPOINT}

    elif [[ "$S3FS_TYPE" == "s3compat" ]]; then
        S3FS_ACCESS=${S3_ACCESS:-S3COMPAT_ACCESS_KEY}
        S3FS_SECRET=${S3_SECRET:-S3COMPAT_SECRET_KEY}
        S3FS_ENDPOINT=${ENDPOINT:-S3COMPAT_ENDPOINT}

    fi
    s3fs_install
    s3fs_add_creds $S3FS_ACCESS $S3FS_SECRET $S3FS_TYPE
    mkdir -p "$MOUNT_PATH"
    if [[ "$(ls -A $MOUNT_PATH)" ]]; then
        echo "S3 Bucket is already mounted at $MOUNT_PATH"
    else
        if [[ "$S3FS_TYPE" == "aws" ]]; then 
            echo "Mounting S3 Bucket $BUCKET to $MOUNT_PATH"
            s3fs "$BUCKET" "$MOUNT_PATH" -o passwd_file="/etc/passwd-s3fs-$S3FS_TYPE"
        else
            echo "Mounting $S3FS_TYPE Bucket $BUCKET to $MOUNT_PATH"
            s3fs "$BUCKET" "$MOUNT_PATH" -o passwd_file="/etc/passwd-s3fs-$S3FS_TYPE" -o url="$S3FS_ENDPOINT/" -o use_path_request_style
        fi
    fi
}


