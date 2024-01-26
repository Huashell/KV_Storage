#!/bin/bash
go build -buildmode=plugin -gcflags="all=-N -l" -o wc-debug.so ../mrapps/wc.go
