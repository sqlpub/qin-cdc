#!/bin/bash

version="v0.3.0"
currentDir=$(cd $(dirname "$0") || exit; pwd)

path="github.com/go-demo/version"
buildTime=$(date +"%Y-%m-%d %H:%M:%S")
buildTimeFormat=$(date +"%Y%m%d%H%M%S")
newDir="../bin/qin-cdc-$version"
# flagsMac="-X $path.Version=$version -X '$path.GoVersion=$(go version)' -X '$path.BuildTime=$buildTime' -X $path.GitCommit=$(git rev-parse HEAD)"
flagsLinux="-X $path.Version=$version -X '$path.GoVersion=$(go version)' -X '$path.BuildTime=$buildTime' -X $path.GitCommit=$(git rev-parse HEAD)"

mkdir -p "$newDir"
echo start buid qin-cdc
cd "$currentDir"/cmd || exit
# go build -ldflags "$flagsMac" -o "$newDir"/go-"$dbType"-starrocks-mac-"$buildTimeFormat"
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 CC=x86_64-linux-musl-gcc CXX=x86_64-linux-musl-g++ \
 go build -tags musl -ldflags "-extldflags -static $flagsLinux" -o "$newDir"/qin-cdc-$version-"$buildTimeFormat"
echo end buid qin-cdc