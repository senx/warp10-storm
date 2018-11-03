#!/bin/sh
#
#   Copyright 2018  SenX S.A.S.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#
# Script used to run a topology
#
# Usage: toporun.sh file1.mc2 file2.mc2 ...
#

TMPDIR=""

die() {
  echo "$@" 1>&2
  #rm -rf "${TMPDIR}"
  #rm -rf "${DYNJAR}"
  exit 1
}

checkpath() {
  for path in $@
  do
    if [ ! -e $path ]
    then
      die "Missing path $path"
    fi
  done
}

WARP10_STORM_JAR=build/libs/warp10-storm.jar

JAR=jar
JAVA_OPTS="-Ddebug"

CLASSPATH=`grep ' path=' .classpath|grep 'kind="lib"'|sed -e 's/.* path="//' -e 's/".*//'|while read f
do
  /bin/echo -n $f:
done`

##
## Parse arguments
##

MACROS=''
JARS=''
NODES=''
CONFIG=''
LOCAL=''
KEEPJAR=''

OPTIND=1
while getopts ":klt:j:m:c:" opt; do
  case $opt in
    k)
      KEEPJAR="true"
      ;;
    l)
      LOCAL="-Dstorm.local"
      ;;
    t)
      TOPOLOGY="${OPTARG}"
      ;;
    j)
      JARS="${JARS} ${OPTARG}"
      ;;
    m)
      MACROS="${MACROS} ${OPTARG}"
      ;;
    c)
      CONFIG=${OPTARG}
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

if [ "x" != "x${CONFIG}" ]
then
  JAVA_OPTS="${JAVA_OPTS} -Dwarp10.config=${CONFIG}"
fi

if [ "x" == "x${TOPOLOGY}" ]
then
  die "You MUST specify the topology name using -t xxx."
fi

shift $((OPTIND - 1))

NODES=$@

checkpath ${JARS} ${MACROS} ${CONFIG} ${NODES}

DYNJAR="`pwd`/warp10-storm-`date +%s`-$$.jar"

if [ "x" == "x${LOCAL}" ]
then

  ##
  ## Merge the jars
  ##

  echo "[Merging jars ${JARS} into ${DYNJAR}]"

  cp ${WARP10_STORM_JAR} ${DYNJAR} || die "Error copying base jar ${WARP10_STORM_JAR} into ${DYNJAR}"

  if [ "x" != "x${JARS}" ]
  then
    TMPDIR="${DYNJAR}.tmp"

    for jar in ${JARS}
    do
      mkdir "${TMPDIR}"
      ln -s "${jar}" $$.jar
      cd "${TMPDIR}"
      ${JAR} xf ../$$.jar || die "Error unpacking jar ${jar}"
      cd ..
      ${JAR} uf ${DYNJAR} -C ${TMPDIR} '*' || die "Error merging ${jar} into ${DYNJAR}"
      rm -rf "${TMPDIR}"
      rm -rf $$.jar
    done
  fi

  JAVA_OPTS="${JAVA_OPTS} -Dstorm.jar=${DYNJAR}"

  ##
  ## Add nodes
  ##

  echo "[Merging nodes ${NODES} into ${DYNJAR}]"
  ${JAR} uf ${DYNJAR} ${NODES} || die "Error merging nodes"

else

  CLASSPATH="${WARP10_STORM_JAR}:${CLASSPATH}"

  if [ "x" != "x${JARS}" ]
  then
    for jar in ${JARS}
    do
      CLASSPATH="${CLASSPATH}:${jar}"
      echo ${CLASSPATH}
    done
  fi

  JAVA_OPTS="${JAVA_OPTS} -Dstorm.jar=${WARP10_STORM_JAR}"

  ##
  ## Add nodes
  ##

  echo "[Merging configuration and nodes ${NODES} into ${DYNJAR}]"
  ${JAR} cf ${DYNJAR} ${CONFIG} ${NODES} || die "Error merging nodes"
fi

##
## Merge macros
##

if [ "x" != "x${MACROS}" ]
then
  echo "[Merging macros ${MACROS} into ${DYNJAR}]"
  ${JAR} uf ${DYNJAR} ${MACROS} || die "Error merging macros"
fi

CLASSPATH="${DYNJAR}:${CLASSPATH}:."

JAVA_OPTS="${JAVA_OPTS} ${LOCAL}"

java ${JAVA_OPTS} -cp ${CLASSPATH} io.warp10.storm.WarpScriptTopology "${TOPOLOGY}" ${NODES}

if [ "x" != "x${DYNJAR}" ]
then
  if [ "x" == "x${KEEPJAR}" ]
  then
    rm "${DYNJAR}"
  fi
fi
