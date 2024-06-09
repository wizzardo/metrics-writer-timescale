#!/usr/bin/env bash
set -e

#export JAVA_HOME=/Users/wizzardo/Library/Java/liberica-11.0.14
#export JAVA_HOME=/Users/wizzardo/Library/Java/liberica-17.0.4
export JAVA_HOME=/Users/wizzardo/Library/Java/liberica-21.0.2
export PATH=$JAVA_HOME/bin:$PATH

#./gradlew clean fatJar --refresh-dependencies
#./gradlew clean fatJar
./gradlew  fatJar

