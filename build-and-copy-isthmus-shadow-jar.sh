#!/bin/bash

echo "Enter the absolute path of the substrait-java repo"
read substrait_java_path
cd ${substrait_java_path}/isthmus; ../gradlew shadowJar
cd -
mkdir -p jars
cp ${substrait_java_path}/isthmus/build/libs/*all.jar ./jars/
