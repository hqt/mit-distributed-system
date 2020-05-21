#!/bin/bash
cd src/raft || exit
for i in {1..10}
do
  echo "start stress test $i"
	go clean -testcache;
	go test --race -run 2A || exit;
done