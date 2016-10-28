#!/bin/sh
#
# Script used to run a topology
#
# Usage: toporun.sh file1.mc2 file2.mc2 ...
#
# You can set the WARP10_CONFIG environment variable to the
# path of the Warp 10 config file.
#
# If the config file is not specified, WarpScript will be
# configured with microseconds as the time unit and REXEC
# will be enabled
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

if [ "x" != "x${WARP10_CONFIG}" ]
then
  JAVA_OPTS="${JAVA_OPTS} -Dwarp10.config=${WARP10_CONFIG}"
fi

CLASSPATH=`grep ' path=' .classpath|grep 'kind="lib"'|sed -e 's/.* path="//' -e 's/".*//'|while read f
do
  /bin/echo -n $f:
done`

CLASSPATH="${CLASSPATH}:."

##
## Parse arguments
##

MACROS=''
JARS=''
NODES=''
CONFIG=''

OPTIND=1
while getopts ":t:j:m:c:" opt; do
  case $opt in
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

if [ "x" == "x${TOPOLOGY}" ]
then
  die "You MUST specify the topology name using -t xxx."
fi

shift $((OPTIND - 1))

NODES=$@

checkpath ${JARS} ${MACROS} ${CONFIG} ${NODES}

DYNJAR="`pwd`/warp10-storm-`date +%s`-$$.jar"

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

##
## Add nodes and configuration
##

echo "[Merging nodes ${NODES} into ${DYNJAR}]"
${JAR} uf ${DYNJAR} ${CONFIG} ${NODES} || die "Error merging nodes"

##
## Merge macros
##

if [ "x" != "x${MACROS}" ]
then
  echo "[Merging macros ${MACROS} into ${DYNJAR}]"
  ${JAR} uf ${DYNJAR} ${MACROS} || die "Error merging macros"
fi

JAVA_OPTS="${JAVA_OPTS} -Dstorm.jar=${DYNJAR}"
CLASSPATH="${CLASSPATH}:${DYNJAR}"

java ${JAVA_OPTS} -cp ${CLASSPATH} io.warp10.storm.WarpScriptTopology "${TOPOLOGY}" ${NODES}

rm ${DYNJAR}
