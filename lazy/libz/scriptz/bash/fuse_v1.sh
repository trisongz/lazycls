#!/bin/bash

################################################################
#####           Installers for FUSE                      #######
################################################################

fuse_install_prereqs() {
    if [[ "$(which fusermount)" == "" ]]; then
        apt update -qq && apt install -y -qq curl fuse
    fi
}

gcsfuse_install() {
    fuse_install_prereqs
    if [[ "$(which gcsfuse)" == "" ]]; then
        export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s`
        echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list
        curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
        sudo apt-get -qq update && sudo apt-get install -y -qq gcsfuse
    fi
}

s3fs_install() {
    fuse_install_prereqs
    if [[ "$(which s3fs)" == "" ]]; then
        sudo apt-get -qq update && sudo apt-get install -y -qq s3fs
    fi
}


s3fs_add_creds() {
    S3FS_ACCESS=$1
    S3FS_SECRET=$2
    S3FS_FS=$3
    S3FS_FILENAME="/etc/passwd-s3fs-${S3FS_FS:-aws}"
    echo "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" > $S3FS_FILENAME
    chmod 600 $S3FS_FILENAME
}


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


