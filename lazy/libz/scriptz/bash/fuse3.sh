#!/bin/bash

### Script to install fuse3 in ubuntu 18.04 ###

install_preqs() {
    if [[ "$(which meson)" == "" ]]; then
        apt update -qq && apt install -y -qq curl meson fuse git
    fi
}

install_fuse3() {
    if [[ "$(which fusermount3)" == "" ]]; then
        #mkdir -p /tmp/fuse3 && cd /tmp/fuse3 && git clone https://github.com/libfuse/libfuse .
        mkdir -p /tmp/fuse3 && cd /tmp/fuse3 && wget https://github.com/libfuse/libfuse/releases/download/fuse-3.10.5/fuse-3.10.5.tar.xz
        tar -xf fuse-3.10.5.tar.xz && cd fuse-3.10.5
        meson build
        #mkdir build; cd build
        #meson build ..
        cd build
        ninja
        sudo ninja install
        pip install pyfuse3
        cd /content
        rm -r /tmp/fuse3
    fi
}

install_preqs
install_fuse3