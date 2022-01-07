#!/bin/bash

GS_BUCKET=$1
GS_MOUNT_PATH=$2

SCRIPTS_DIR=$(dirname "$(realpath $0)")
#FUSE_FUNCS="$SCRIPTS_DIR/fuse_v1.sh"

#source "$FUSE_FUNCS"
echo "Mounting $GS_BUCKET -> $GS_MOUNT_PATH"

fuse_install_prereqs() {
    if [[ "$(which fuse)" == "" ]]; then
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

gcsfuse_mount $GS_BUCKET $GS_MOUNT_PATH