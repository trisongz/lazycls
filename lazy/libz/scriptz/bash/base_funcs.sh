#!/bin/bash

SCRIPTS_DIR=$(dirname "$(realpath $0)")

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


