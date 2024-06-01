#!/bin/bash

# cheap mac detection
if which brew; then
    xcode-select --install

    # https://github.com/pymssql/pymssql/issues/372#issuecomment-309950321
    brew install freetds
    brew link --force freetds
fi
