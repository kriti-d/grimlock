#!/bin/bash
#
# Copyright 2019,2020 Commonwealth Bank of Australia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -vx

JAR=grimlock.jar
NUM_TEST=34
DO_BUILD=${1-true}
DO_CLEANUP=true
DO_INIT=true
DO_LOCAL=true
DO_CLUSTER=false
DO_DEMO=true
DO_TEST=true
BASE_DIR="../../../../../../.."

if [ ${DO_BUILD} = "true" ]
then
  grep '[a-z]\s*++\s*shadeSha' ${BASE_DIR}/build.sbt > /dev/null 2>&1
  IS_SHADED=$?

  if [ ${IS_SHADED} -eq 0 -a ${DO_LOCAL} = "true" ]
  then
    set +x
    echo "!!                                           !!"
    echo "!!                                           !!"
    echo "!! Local and shaded don't work well together !!"
    echo "!!                                           !!"
    echo "!!                                           !!"
    exit
  fi

  rm -f ${JAR}
  cd ${BASE_DIR}; ./sbt clean assembly; cd -
  cp ${BASE_DIR}/grimlock-examples/target/scala-2.11/grimlock*.jar ${JAR}
fi

if [ ${DO_DEMO} = "true" ]
then
  if [ ${DO_LOCAL} = "true" ]
  then
    if [ ${DO_INIT} = "true" ]
    then
      mkdir -p demo.scala
    fi

    if [ ${DO_CLEANUP} = "true" ]
    then
      rm -rf demo.scala/*
    fi

    java -cp $JAR commbank.grimlock.scala.examples.BasicOperations ../data
    java -cp $JAR commbank.grimlock.scala.examples.Conditional ../data
    java -cp $JAR commbank.grimlock.scala.examples.PipelineDataPreparation ../data
    java -cp $JAR commbank.grimlock.scala.examples.Scoring ../data
    java -cp $JAR commbank.grimlock.scala.examples.DataAnalysis ../data
    java -cp $JAR commbank.grimlock.scala.examples.LabelWeighting ../data
    java -cp $JAR commbank.grimlock.scala.examples.InstanceCentricTfIdf ../data
    java -cp $JAR commbank.grimlock.scala.examples.MutualInformation ../data
    java -cp $JAR commbank.grimlock.scala.examples.DerivedData ../data
    cp ../data/gbm.R ../data/rf.R ../data/lr.R .
    java -cp $JAR commbank.grimlock.scala.examples.Ensemble ../data

    if [ -d "demo.scala.old" ]
    then
      diff -r demo.scala demo.scala.old
    fi
  fi
fi

if [ ${DO_TEST} = "true" ]
then
  if [ ${DO_LOCAL} = "true" ]
  then
    if [ ${DO_INIT} = "true" ]
    then
      mkdir -p tmp.scala
    fi

    if [ ${DO_CLEANUP} = "true" ]
    then
      rm -rf tmp.scala/*
    fi

    for i in $(seq 1 ${NUM_TEST})
    do
      java -cp $JAR commbank.grimlock.test.TestScala${i} .
    done

    if [ -d "tmp.scala.old" ]
    then
      diff -r tmp.scala tmp.scala.old
    fi
  fi
fi

rm -rf gbm.R rf.R lr.R

