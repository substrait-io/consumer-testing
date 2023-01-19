#!/bin/bash

set -euo pipefail

echo "Building Isthmus"
cd substrait-java/isthmus; ../gradlew shadowJar
cd -
mkdir -p jars
cp substrait-java/isthmus/build/libs/*all.jar ./jars/
echo "Build Succeed!!!"
