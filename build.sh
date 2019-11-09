#!/bin/bash

GOOS=linux GOARCH=amd64 go build -o es-hit
upx es-hit
mv es-hit{,.new}

