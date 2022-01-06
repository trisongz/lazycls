#!/bin/bash

### Script to install fuse3 in ubuntu 18.04 ###

install_preqs() {
    if [[ "$(which meson)" == "" ]]; then
        apt update -qq && apt install -y -qq curl meson fuse git
    fi
}

install_fuse3() {
    if [[ "$(which fuse3)" == "" ]]; then
        mkdir -p /tmp/fuse3 && cd /tmp/fuse3 && git clone https://github.com/libfuse/libfuse .
        mkdir build; cd build
        meson build ..
        ninja
        sudo ninja install
    fi
}

install_preqs
install_fuse3