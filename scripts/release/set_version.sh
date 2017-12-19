#!/bin/sh
set -eu -o pipefail
IFS=$'\n\t'

if [[ $# -ne 1 ]] ; then
    >&2 echo "Usage: $0 <new_version>"
    exit 1
fi

INPUT_VERSION=$1; shift

MAJOR_VERSION=${INPUT_VERSION%%.*}
WITHOUT_MAJOR_VERSION=${INPUT_VERSION#${MAJOR_VERSION}.}
MINOR_VERSION=${WITHOUT_MAJOR_VERSION%%.*}
WITHOUT_MINOR_VERSION=${INPUT_VERSION#${MAJOR_VERSION}.${MINOR_VERSION}.}
PATCH_VERSION=${WITHOUT_MINOR_VERSION%%.*}

XYZ_VERSION="${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}"

cd $(dirname -- $0)
cd ${PWD}/../..

# quasardbVersion=2.0.0
sed -i -e 's/\s*quasardbVersion\s*=\s*[0-9.]\+/quasardbVersion='"${XYZ_VERSION}"'/' gradle.properties
# <quasardbVersion>2.0.0</quasardbVersion>
sed -i -e 's|<quasardbVersion>.*</quasardbVersion>|<quasardbVersion>'"${XYZ_VERSION}"'</quasardbVersion>|' pom.xml