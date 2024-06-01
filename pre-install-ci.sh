if which brew; then
    # https://github.com/pymssql/pymssql/issues/372#issuecomment-309950321
    brew install freetds
    brew link --force freetds
fi
