#!/bin/sh
set -e
case "$1" in
  namenode)
    if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
      hdfs namenode -format -nonInteractive -force
    fi
    exec hdfs namenode
    ;;
  datanode)
    exec hdfs datanode
    ;;
  *)
    exec "$@"
    ;;
esac
