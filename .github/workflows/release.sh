#!/usr/bin/env bash

# Be extra strict
## If you want to change these, make sure they are not set later in the code. We
## assume they are always on and we disable/enable them on a per need basis

set -o xtrace
set -o errexit
set -o nounset
set -o pipefail

SCALA_VER="${1}"
BRANCH_OR_TAG="${2}"

## Format: ${componentOwner}-${componentName}-${componentVersion}.zip
EMS_FOLDER="celonis-kafka-connect-ems-${BRANCH_OR_TAG}"


rm -rf "${EMS_FOLDER}"

mkdir -p "${EMS_FOLDER}/lib"
mkdir -p "${EMS_FOLDER}/docs"
mkdir -p "${EMS_FOLDER}/etc"

# Copy build artifacts
cp -r connector/target/scala-${SCALA_VER}/*.jar "${EMS_FOLDER}/lib"
cp -r connector/target/manifest.json "${EMS_FOLDER}"

# Copy configuration
cp "config/quickstart-EMSSinkConnector.properties" "${EMS_FOLDER}/etc"

# Copy docs and images
cp -r "release/assets" "${EMS_FOLDER}"
cp README.md LICENSE NOTICE "${EMS_FOLDER}/docs"
cp -r licenses/ "${EMS_FOLDER}/docs"

mkdir -p ${CONNECTOR_FOLDER}
zip -r "${CONNECTOR_FOLDER}/${EMS_FOLDER}.zip" "${EMS_FOLDER}/"

rm -rf "${EMS_FOLDER}"
