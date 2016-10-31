## Storm integration of WarpScript

The `warp10-storm` repository contains code which enables the creation of Storm topologies entirely in WarpScript.

### Defining topologies

Topologies are defined by WarpScript code which leaves on the stack a set of maps, each map being a node (spout or bolt).

Have a look at the commented examples in `src/main/warpscript`.

### Running topologies

Topologies are run using the `src/main/sh/toporun.sh` script.

This script will pack together the required jars and the various mc2 files which form your topology (node files and optional macros) and then submit this jar to a local or remote Storm cluster.

The script has the following options:

```
-l  Indicate that the topology should be run on a local cluster
-m xxxx.mc2  Add the specified file in the jar so it can be called as a macro
-t topology  Set the name of the topology
-j xxxx.jar  Add the specified jar to the compound jar
-c xxxx.conf  Use the specified file as the Warp 10 platform configuration

Any argument following the options will be used to define nodes.
```
