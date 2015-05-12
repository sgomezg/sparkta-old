#!/bin/bash

CONF_DIR=config
CLASSPATH_DIR=lib

PRG="$0"

while [ -h "$PRG" ]; do
      ls=`ls -ld "$PRG"`
        link=`expr "$ls" : '.*-> \(.*\)$'`
          if expr "$link" : '/.*' > /dev/null; then
                  PRG="$link"
                    else
                            PRG=`dirname "$PRG"`/"$link"
                              fi
                          done

                          PRGDIR=`dirname "$PRG"`
                          BASEDIR=`cd "$PRGDIR/.." >/dev/null; pwd`

                          export SPARKTA_HOME=$BASEDIR

                          # If a specific java binary isn't specified search for the standard 'java' binary
                          if [ -z "$JAVACMD" ] ; then
                                if [ -n "$JAVA_HOME"  ] ; then
                                        if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
                                                  # IBM's JDK on AIX uses strange locations for the executables
                                                        JAVACMD="$JAVA_HOME/jre/sh/java"
                                                            else
                                                                      JAVACMD="$JAVA_HOME/bin/java"
                                                                          fi
                                                                            else
                                                                                    JAVACMD=`which java`
                                                                                      fi
                                                                                  fi

                                                                                  exec "$JAVACMD" -Xms128m -Xmx128m\
                                                                                        -classpath "$BASEDIR/$CLASSPATH_DIR/*:$BASEDIR/$CONF_DIR" \
                                                                                          -Dapp.name="sparkta-driver" \
                                                                                            -Dapp.pid="$$" \
                                                                                              -Dapp.repo="$BASEDIR/$CLASSPATH_DIR" \
                                                                                                -Dapp.home="$BASEDIR" \
                                                                                                  -Dbasedir="$BASEDIR" \
                                                                                                    com.stratio.sparkta.driver.Sparkta \
                                                                                                      "$@"
