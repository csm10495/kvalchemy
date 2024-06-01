#!/bin/bash

# cheap mac detection
if which brew; then
    brew install openssl
    brew link --force openssl

    # https://github.com/pymssql/pymssql/issues/372#issuecomment-309950321
    brew install freetds
    brew link --force freetds
fi
