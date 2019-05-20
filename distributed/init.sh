#!/usr/bin/env bash

# Use this by calling:
# bash -c "$(curl -sL https://raw.githubusercontent.com/lawben/scotty-window-processor/master/distributed/init.sh)"

sudo apt update
sudo apt install -y git oracle-java8-jdk maven

cd ~
HOME_DIR=`PWD`
git clone https://github.com/lawben/scotty-window-processor.git distributed-scotty
cd distributed-scotty

mvn clean install

export CLASSPATH="$HOME_DIR/distributed-scotty/distributed/target/classes:$HOME_DIR/distributed-scotty/core/target/classes:$HOME_DIR/distributed-scotty/state/target/classes:$HOME_DIR/distributed-scotty/slicing/target/classes:$HOME_DIR/.m2/repository/org/jetbrains/annotations/17.0.0/annotations-17.0.0.jar:$HOME_DIR/.m2/repository/org/zeromq/jeromq/0.5.1/jeromq-0.5.1.jar:$HOME_DIR/.m2/repository/eu/neilalexander/jnacl/1.0.0/jnacl-1.0.0.jar:$HOME_DIR/.m2/repository/org/slf4j/slf4j-api/1.7.16/slf4j-api-1.7.16.jar"
echo ${CLASSPATH} >> ${HOME_DIR}/.bashrc
