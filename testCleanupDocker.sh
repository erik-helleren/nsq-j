#!/bin/zsh

docker rm -f $(docker ps --all --filter name=nsqd-cluster -q)  $(docker ps --all --filter name=nsq-lookup -q)
