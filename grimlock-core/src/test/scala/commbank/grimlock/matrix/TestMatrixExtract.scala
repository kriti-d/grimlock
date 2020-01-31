// Copyright 2015,2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.sample._

import com.twitter.scalding.typed.ValuePipe

import shapeless.HList

trait TestMatrixExtract extends TestMatrix {
  val ext = "foo"

  val result1 = List(Cell(Position("foo"), Content(OrdinalSchema[String](), "3.14")))

  val result2 = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result3 = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result4 = List(Cell(Position("foo"), Content(OrdinalSchema[String](), "3.14")))

  val result5 = List(
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result6 = List(
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )
}

object TestMatrixExtract {
  case class TestSampler[P <: HList]() extends Sampler[P] {
    def select(cell: Cell[P]): Boolean =
      cell.position.asList.contains(StringValue("foo")) || cell.position.asList.contains(IntValue(2))
  }

  case class TestSamplerWithValue[P <: HList]() extends SamplerWithValue[P] {
    type V = String
    def selectWithValue(cell: Cell[P], ext: V): Boolean = cell.position.asList.contains(StringValue(ext))
  }
}

class TestScalaMatrixExtract extends TestMatrixExtract with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.subset" should "return its sampled data in 1D" in {
    toU(data1)
      .extract(TestMatrixExtract.TestSampler())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its sampled data in 2D" in {
    toU(data2)
      .extract(TestMatrixExtract.TestSampler())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its sampled data in 3D" in {
    toU(data3)
      .extract(TestMatrixExtract.TestSampler())
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.subsetWithValue" should "return its sampled data in 1D" in {
    toU(data1)
      .extractWithValue(ext, TestMatrixExtract.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its sampled data in 2D" in {
    toU(data2)
      .extractWithValue(ext, TestMatrixExtract.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its sampled data in 3D" in {
    toU(data3)
      .extractWithValue(ext, TestMatrixExtract.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result6
  }
}

class TestScaldingMatrixExtract extends TestMatrixExtract with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.subset" should "return its sampled data in 1D" in {
    toU(data1)
      .extract(TestMatrixExtract.TestSampler())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its sampled data in 2D" in {
    toU(data2)
      .extract(TestMatrixExtract.TestSampler())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its sampled data in 3D" in {
    toU(data3)
      .extract(TestMatrixExtract.TestSampler())
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.subsetWithValue" should "return its sampled data in 1D" in {
    toU(data1)
      .extractWithValue(ValuePipe(ext), TestMatrixExtract.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its sampled data in 2D" in {
    toU(data2)
      .extractWithValue(ValuePipe(ext), TestMatrixExtract.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its sampled data in 3D" in {
    toU(data3)
      .extractWithValue(ValuePipe(ext), TestMatrixExtract.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result6
  }
}

class TestSparkMatrixExtract extends TestMatrixExtract with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.subset" should "return its sampled data in 1D" in {
    toU(data1)
      .extract(TestMatrixExtract.TestSampler())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its sampled data in 2D" in {
    toU(data2)
      .extract(TestMatrixExtract.TestSampler())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its sampled data in 3D" in {
    toU(data3)
      .extract(TestMatrixExtract.TestSampler())
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.subsetWithValue" should "return its sampled data in 1D" in {
    toU(data1)
      .extractWithValue(ext, TestMatrixExtract.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its sampled data in 2D" in {
    toU(data2)
      .extractWithValue(ext, TestMatrixExtract.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its sampled data in 3D" in {
    toU(data3)
      .extractWithValue(ext, TestMatrixExtract.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result6
  }
}

