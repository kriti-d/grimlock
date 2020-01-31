#!/bin/bash
#
# Copyright 2014,2015,2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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
DO_INIT=false
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
      mkdir -p demo.spark
    fi

    if [ ${DO_CLEANUP} = "true" ]
    then
      rm -rf demo.spark/*
    fi

    $BASE_DIR/../spark-2.3.2-bin-hadoop2.7/bin/spark-submit --master local \
      --class commbank.grimlock.spark.examples.BasicOperations $JAR local ../data
    $BASE_DIR/../spark-2.3.2-bin-hadoop2.7/bin/spark-submit --master local \
      --class commbank.grimlock.spark.examples.Conditional $JAR local ../data
    $BASE_DIR/../spark-2.3.2-bin-hadoop2.7/bin/spark-submit --master local \
      --class commbank.grimlock.spark.examples.PipelineDataPreparation $JAR local ../data
    $BASE_DIR/../spark-2.3.2-bin-hadoop2.7/bin/spark-submit --master local \
      --class commbank.grimlock.spark.examples.Scoring $JAR local ../data
    $BASE_DIR/../spark-2.3.2-bin-hadoop2.7/bin/spark-submit --master local \
      --class commbank.grimlock.spark.examples.DataAnalysis $JAR local ../data
    $BASE_DIR/../spark-2.3.2-bin-hadoop2.7/bin/spark-submit --master local \
      --class commbank.grimlock.spark.examples.LabelWeighting $JAR local ../data
    $BASE_DIR/../spark-2.3.2-bin-hadoop2.7/bin/spark-submit --master local \
      --class commbank.grimlock.spark.examples.InstanceCentricTfIdf $JAR local ../data
    $BASE_DIR/../spark-2.3.2-bin-hadoop2.7/bin/spark-submit --master local \
      --class commbank.grimlock.spark.examples.MutualInformation $JAR local ../data
    $BASE_DIR/../spark-2.3.2-bin-hadoop2.7/bin/spark-submit --master local \
      --class commbank.grimlock.spark.examples.DerivedData $JAR local ../data
    cp ../data/gbm.R ../data/rf.R ../data/lr.R .
    $BASE_DIR/../spark-2.3.2-bin-hadoop2.7/bin/spark-submit --master local \
      --class commbank.grimlock.spark.examples.Ensemble $JAR local ../data

    if [ -d "demo.spark.old" ]
    then
      diff -r demo.spark demo.spark.old
    fi
  fi
fi

if [ ${DO_TEST} = "true" ]
then
  if [ ${DO_LOCAL} = "true" ]
  then
    if [ ${DO_INIT} = "true" ]
    then
      mkdir -p tmp.spark
    fi

    if [ ${DO_CLEANUP} = "true" ]
    then
      rm -rf tmp.spark/*
    fi

    for i in $(seq 1 ${NUM_TEST})
    do
      $BASE_DIR/../spark-2.3.2-bin-hadoop2.7/bin/spark-submit --master local --driver-class-path $JAR \
        --class commbank.grimlock.test.TestSpark${i} $JAR local .
    done

    if [ -d "tmp.spark.old" ]
    then
      diff -r tmp.spark tmp.spark.old
    fi
  fi
fi

rm -rf gbm.R rf.R lr.R

