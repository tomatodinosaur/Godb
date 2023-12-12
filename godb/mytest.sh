#!/bin/bash
go test -v $1 $(ls *.go | grep -v $1)
