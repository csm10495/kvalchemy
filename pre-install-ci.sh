if which brew; then
    # https://github.com/pymssql/pymssql/issues/372#issuecomment-309950321
    brew install freetds@0.91
    brew link --force freetds@0.91
fi
