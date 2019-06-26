// Copyright 2016,2017,2018,2019 Commonwealth Bank of Australia
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

package commbank.grimlock.test

import commbank.grimlock.framework.distribution._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._

import shapeless.nat._0

class TestScalaTDigestQuantile extends TestApproximateQuantile with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "An approximate quantile" should "return reasonably close t-digest aggregates" in {
    val ref = toU(data2)
      .summarise(Along(_0), Default())(
        CountMapQuantiles(probs, Quantiles.Type1, TestApproximateQuantile.name, false, true)
      )
      .toList
      .map(_.content.value.as[Double].get)
      .sorted

    val res = toU(data2)
      .summarise(Along(_0), Default())(TDigestQuantiles(probs, 100, TestApproximateQuantile.name, false, true))
      .toList
      .map(_.content.value.as[Double].get)
      .sorted

    res.zip(ref).foreach { case (t, c) => t shouldBe c +- 5e-3 }
  }

  it should "return reasonably close t-digest quantiles" in {
    val ref = toU(data2)
      .summarise(Along(_0), Default())(
        CountMapQuantiles(probs, Quantiles.Type1, TestApproximateQuantile.name, false, true)
      )
      .toList
      .map(_.content.value.as[Double].get)
      .sorted

    val res = toU(data2)
      .tDigestQuantiles(Along(_0), Default())(probs, 100, TestApproximateQuantile.name, false, true)
      .toList
      .map(_.content.value.as[Double].get)
      .sorted

    res.zip(ref).foreach { case (t, c) => t shouldBe c +- 5e-3 }
  }
}

class TestScaldingTDigestQuantile extends TestApproximateQuantile with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "An approximate quantile" should "return reasonably close t-digest aggregates" in {
    val ref = toU(data2)
      .summarise(Along(_0), Default(12))(
        CountMapQuantiles(probs, Quantiles.Type1, TestApproximateQuantile.name, false, true)
      )
      .toList
      .map(_.content.value.as[Double].get)
      .sorted

    val res = toU(data2)
      .summarise(Along(_0), Default())(TDigestQuantiles(probs, 100, TestApproximateQuantile.name, false, true))
      .toList
      .map(_.content.value.as[Double].get)
      .sorted

    res.zip(ref).foreach { case (t, c) => t shouldBe c +- 5e-3 }
  }

  it should "return reasonably close t-digest quantiles" in {
    val ref = toU(data2)
      .summarise(Along(_0), Default(12))(
        CountMapQuantiles(probs, Quantiles.Type1, TestApproximateQuantile.name, false, true)
      )
      .toList
      .map(_.content.value.as[Double].get)
      .sorted

    val res = toU(data2)
      .tDigestQuantiles(Along(_0), Default())(probs, 100, TestApproximateQuantile.name, false, true)
      .toList
      .map(_.content.value.as[Double].get)
      .sorted

    res.zip(ref).foreach { case (t, c) => t shouldBe c +- 5e-3 }
  }
}

class TestSparkTDigestQuantile extends TestApproximateQuantile with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "An approximate quantile" should "return reasonably close t-digest aggregates" in {
    val ref = toU(data2)
      .summarise(Along(_0), Default(12))(
        CountMapQuantiles(probs, Quantiles.Type1, TestApproximateQuantile.name, false, true)
      )
      .toList
      .map(_.content.value.as[Double].get)
      .sorted

    val res = toU(data2)
      .summarise(Along(_0), Default())(TDigestQuantiles(probs, 100, TestApproximateQuantile.name, false, true))
      .toList
      .map(_.content.value.as[Double].get)
      .sorted

    res.zip(ref).foreach { case (t, c) => t shouldBe c +- 5e-3 }
  }

  it should "return reasonably close t-digest quantiles" in {
    val ref = toU(data2)
      .summarise(Along(_0), Default(12))(
        CountMapQuantiles(probs, Quantiles.Type1, TestApproximateQuantile.name, false, true)
      )
      .toList
      .map(_.content.value.as[Double].get)
      .sorted

    val res = toU(data2)
      .tDigestQuantiles(Along(_0), Default())(probs, 100, TestApproximateQuantile.name, false, true)
      .toList
      .map(_.content.value.as[Double].get)
      .sorted

    res.zip(ref).foreach { case (t, c) => t shouldBe c +- 5e-3 }
  }
}

