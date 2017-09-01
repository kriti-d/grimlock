// Copyright 2014,2015,2016,2017 Commonwealth Bank of Australia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commbank.grimlock.scalding.examples

import commbank.grimlock.framework._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._

import commbank.grimlock.scalding.environment._

import com.twitter.scalding.{ Args, Job }

import shapeless.nat.{ _1, _2 }

class DataAnalysis(args: Args) extends Job(args) {

  // Define implicit context.
  implicit val ctx = Context()

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read the data (ignoring errors). This returns a 2D matrix (instance x feature).
  val (data, _) = ctx.loadText(s"${path}/exampleInput.txt", Cell.parse2D())

  // For the instances:
  //  1/ Compute the number of features for each instance;
  //  2/ Save the counts;
  //  3/ Compute the moments of the counts;
  //  4/ Save the moments.
  data
    .summarise(Over(_1))(Counts())
    .saveAsText(ctx, s"./demo.${output}/feature_count.out")
    .summarise(Along(_1))(
      Moments(
        _.append("mean").toOption,
        _.append("sd").toOption,
        _.append("skewness").toOption,
        _.append("kurtosis").toOption
      )
    )
    .saveAsText(ctx, s"./demo.${output}/feature_density.out")
    .toUnit

  // For the features:
  //  1/ Compute the number of instance that have a value for each features;
  //  2/ Save the counts;
  //  3/ Compute the moments of the counts;
  //  4/ Save the moments.
  data
    .summarise(Over(_2))(Counts())
    .saveAsText(ctx, s"./demo.${output}/instance_count.out")
    .summarise(Along(_1))(
      Moments(
        _.append("mean").toOption,
        _.append("sd").toOption,
        _.append("skewness").toOption,
        _.append("kurtosis").toOption
      )
    )
    .saveAsText(ctx, s"./demo.${output}/instance_density.out")
    .toUnit
}

