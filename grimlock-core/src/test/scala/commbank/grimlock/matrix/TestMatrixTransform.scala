// Copyright 2015,2016,2017,2018,2019 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.transform._

import com.twitter.scalding.typed.ValuePipe

import shapeless.{ ::, HList, HNil }
import shapeless.nat._0

trait TestMatrixTransform extends TestMatrix {
  val ext = Map(
    Position("foo") -> Map(
      Position("max.abs") -> Content(ContinuousSchema[Double](), 3.14),
      Position("mean") -> Content(ContinuousSchema[Double](), 3.14),
      Position("sd") -> Content(ContinuousSchema[Double](), 1.0)
    ),
    Position("bar") -> Map(
      Position("max.abs") -> Content(ContinuousSchema[Double](), 6.28),
      Position("mean") -> Content(ContinuousSchema[Double](), 3.14),
      Position("sd") -> Content(ContinuousSchema[Double](), 2.0)
    ),
    Position("baz") -> Map(
      Position("max.abs") -> Content(ContinuousSchema[Double](), 9.42),
      Position("mean") -> Content(ContinuousSchema[Double](), 3.14),
      Position("sd") -> Content(ContinuousSchema[Double](), 3.0)
    ),
    Position("qux") -> Map(
      Position("max.abs") -> Content(ContinuousSchema[Double](), 12.56),
      Position("mean") -> Content(ContinuousSchema[Double](), 3.14),
      Position("sd") -> Content(ContinuousSchema[Double](), 4.0)
    )
  )

  val result1 = List(
    Cell(Position("bar.ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz.ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux.ind"), Content(DiscreteSchema[Long](), 1L))
  )

  val result2 = List(
    Cell(Position("bar.ind", 1), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar.ind", 2), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar.ind", 3), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar=19", 3), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar=6.28", 1), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz.ind", 1), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz.ind", 2), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz=9.42", 1), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 1), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 2), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 3), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 4), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo=3.14", 1), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo=9.42", 3), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux.ind", 1), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux=12.56", 1), Content(DiscreteSchema[Long](), 1L))
  )

  val result3 = List(
    Cell(Position("bar.ind", 1, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar.ind", 2, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar.ind", 3, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar=19", 3, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar=6.28", 1, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz.ind", 1, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz.ind", 2, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz=9.42", 1, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 1, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 2, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 3, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 4, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo=3.14", 1, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo=9.42", 3, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux.ind", 1, "xyz"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux=12.56", 1, "xyz"), Content(DiscreteSchema[Long](), 1L))
  )

  val result4 = List(
    Cell(Position("bar.n"), Content(ContinuousSchema[Double](), 6.28 / 6.28)),
    Cell(Position("bar.s"), Content(ContinuousSchema[Double](), (6.28 - 3.14) / 2)),
    Cell(Position("baz.n"), Content(ContinuousSchema[Double](), 9.42 / 9.42)),
    Cell(Position("baz.s"), Content(ContinuousSchema[Double](), (9.42 - 3.14) / 3)),
    Cell(Position("foo.n"), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo.s"), Content(ContinuousSchema[Double](), (3.14 - 3.14) / 1)),
    Cell(Position("qux.n"), Content(ContinuousSchema[Double](), 12.56 / 12.56)),
    Cell(Position("qux.s"), Content(ContinuousSchema[Double](), (12.56 - 3.14) / 4))
  )

  val result5 = List(
    Cell(Position("bar.n", 1), Content(ContinuousSchema[Double](), 6.28 / 6.28)),
    Cell(Position("bar.n", 2), Content(ContinuousSchema[Double](), 12.56 / 6.28)),
    Cell(Position("bar.n", 3), Content(ContinuousSchema[Double](), 18.84 / 6.28)),
    Cell(Position("baz.n", 1), Content(ContinuousSchema[Double](), 9.42 / 9.42)),
    Cell(Position("baz.n", 2), Content(ContinuousSchema[Double](), 18.84 / 9.42)),
    Cell(Position("foo.n", 1), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo.n", 2), Content(ContinuousSchema[Double](), 6.28 / 3.14)),
    Cell(Position("foo.n", 3), Content(ContinuousSchema[Double](), 9.42 / 3.14)),
    Cell(Position("foo.n", 4), Content(ContinuousSchema[Double](), 12.56 / 3.14)),
    Cell(Position("qux.n", 1), Content(ContinuousSchema[Double](), 12.56 / 12.56))
  )

  val result6 = List(
    Cell(Position("bar.n", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28 / 6.28)),
    Cell(Position("bar.n", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56 / 6.28)),
    Cell(Position("bar.n", 3, "xyz"), Content(ContinuousSchema[Double](), 18.84 / 6.28)),
    Cell(Position("baz.n", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42 / 9.42)),
    Cell(Position("baz.n", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84 / 9.42)),
    Cell(Position("foo.n", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo.n", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28 / 3.14)),
    Cell(Position("foo.n", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42 / 3.14)),
    Cell(Position("foo.n", 4, "xyz"), Content(ContinuousSchema[Double](), 12.56 / 3.14)),
    Cell(Position("qux.n", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56 / 12.56))
  )

  val result7 = List(
    Cell(Position("bar.ind", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz.ind", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux.ind", "ind"), Content(DiscreteSchema[Long](), 1L))
  )

  val result8 = List(
    Cell(Position("bar.ind", 1, "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar.ind", 2, "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar.ind", 3, "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar=19", 3, "bin"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar=6.28", 1, "bin"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz.ind", 1, "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz.ind", 2, "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz=9.42", 1, "bin"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 1, "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 2, "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 3, "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 4, "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo=3.14", 1, "bin"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo=9.42", 3, "bin"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux.ind", 1, "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux=12.56", 1, "bin"), Content(DiscreteSchema[Long](), 1L))
  )

  val result9 = List(
    Cell(Position("bar.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar.ind", 2, "xyz", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar.ind", 3, "xyz", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar=19", 3, "xyz", "bin"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar=6.28", 1, "xyz", "bin"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz.ind", 2, "xyz", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("baz=9.42", 1, "xyz", "bin"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 2, "xyz", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 3, "xyz", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo.ind", 4, "xyz", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo=3.14", 1, "xyz", "bin"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("foo=9.42", 3, "xyz", "bin"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux=12.56", 1, "xyz", "bin"), Content(DiscreteSchema[Long](), 1L))
  )

  val result10 = List(
    Cell(Position("bar.n", "nrm"), Content(ContinuousSchema[Double](), 6.28 / 6.28)),
    Cell(Position("bar.s", "std"), Content(ContinuousSchema[Double](), (6.28 - 3.14) / 2)),
    Cell(Position("baz.n", "nrm"), Content(ContinuousSchema[Double](), 9.42 / 9.42)),
    Cell(Position("baz.s", "std"), Content(ContinuousSchema[Double](), (9.42 - 3.14) / 3)),
    Cell(Position("foo.n", "nrm"), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo.s", "std"), Content(ContinuousSchema[Double](), (3.14 - 3.14) / 1)),
    Cell(Position("qux.n", "nrm"), Content(ContinuousSchema[Double](), 12.56 / 12.56)),
    Cell(Position("qux.s", "std"), Content(ContinuousSchema[Double](), (12.56 - 3.14) / 4))
  )

  val result11 = List(
    Cell(Position("bar.n", 1, "nrm"), Content(ContinuousSchema[Double](), 6.28 / 6.28)),
    Cell(Position("bar.n", 2, "nrm"), Content(ContinuousSchema[Double](), 12.56 / 6.28)),
    Cell(Position("bar.n", 3, "nrm"), Content(ContinuousSchema[Double](), 18.84 / 6.28)),
    Cell(Position("baz.n", 1, "nrm"), Content(ContinuousSchema[Double](), 9.42 / 9.42)),
    Cell(Position("baz.n", 2, "nrm"), Content(ContinuousSchema[Double](), 18.84 / 9.42)),
    Cell(Position("foo.n", 1, "nrm"), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo.n", 2, "nrm"), Content(ContinuousSchema[Double](), 6.28 / 3.14)),
    Cell(Position("foo.n", 3, "nrm"), Content(ContinuousSchema[Double](), 9.42 / 3.14)),
    Cell(Position("foo.n", 4, "nrm"), Content(ContinuousSchema[Double](), 12.56 / 3.14)),
    Cell(Position("qux.n", 1, "nrm"), Content(ContinuousSchema[Double](), 12.56 / 12.56))
  )

  val result12 = List(
    Cell(Position("bar.n", 1, "xyz", "nrm"), Content(ContinuousSchema[Double](), 6.28 / 6.28)),
    Cell(Position("bar.n", 2, "xyz", "nrm"), Content(ContinuousSchema[Double](), 12.56 / 6.28)),
    Cell(Position("bar.n", 3, "xyz", "nrm"), Content(ContinuousSchema[Double](), 18.84 / 6.28)),
    Cell(Position("baz.n", 1, "xyz", "nrm"), Content(ContinuousSchema[Double](), 9.42 / 9.42)),
    Cell(Position("baz.n", 2, "xyz", "nrm"), Content(ContinuousSchema[Double](), 18.84 / 9.42)),
    Cell(Position("foo.n", 1, "xyz", "nrm"), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo.n", 2, "xyz", "nrm"), Content(ContinuousSchema[Double](), 6.28 / 3.14)),
    Cell(Position("foo.n", 3, "xyz", "nrm"), Content(ContinuousSchema[Double](), 9.42 / 3.14)),
    Cell(Position("foo.n", 4, "xyz", "nrm"), Content(ContinuousSchema[Double](), 12.56 / 3.14)),
    Cell(Position("qux.n", 1, "xyz", "nrm"), Content(ContinuousSchema[Double](), 12.56 / 12.56))
  )

  type W = Map[Position[Value[String] :: HNil], Map[Position[Value[String] :: HNil], Content]]

  def extractor[
    P <: HList
  ](
    key: String
  )(implicit
    ev: Position.IndexConstraints.Aux[P, _0, Value[String]]
  ) = ExtractWithDimensionAndKey[P, _0, Value[String], String, Content](_0, key)
    .andThenPresent(_.value.as[Double])

  def locate1[
    P <: HList
  ](
    postfix: String
  )(implicit
    ev1: Position.IndexConstraints.Aux[P, _0, Value[String]],
    ev2: Position.UpdateConstraints[P, _0, Value[String]]
  ) = Locate.RenameDimension[P, _0, Value[String], String](_0, _.toShortString + postfix)

  def locate2[
    P <: HList
  ](
    separator: String
  )(implicit
    ev1: Position.IndexConstraints.Aux[P, _0, Value[String]],
    ev2: Position.UpdateConstraints[P, _0, Value[String]]
  ) = Locate.RenameDimensionWithContent[P, _0, Value[String], String](
    _0,
    (v, c) => v.toShortString + separator + c.toShortString
  )
}

class TestScalaMatrixTransform extends TestMatrixTransform with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.transform" should "return its transformed data in 1D" in {
    toU(data1)
      .transform(Indicator[P1]().andThenRelocate(locate1(".ind")))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its transformed data in 2D" in {
    toU(data2)
      .transform(
        List(
          Indicator[P2]().andThenRelocate(locate1(".ind")),
          Binarise[P2, P2](locate2("="))
        )
      )
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its transformed data in 3D" in {
    toU(data3)
      .transform(
        Indicator[P3]().andThenRelocate(locate1(".ind")),
        Binarise[P3, P3](locate2("="))
      )
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.transformWithValue" should "return its transformed data in 1D" in {
    toU(num1)
      .transformWithValue(
        ext,
        List(
          Normalise[P1, W](extractor("max.abs")).andThenRelocate(locate1(".n")),
          Standardise[P1, W](extractor("mean"), extractor("sd")).andThenRelocate(locate1(".s"))
        )
      )
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its transformed data in 2D" in {
    toU(num2)
      .transformWithValue(
        ext,
        Normalise[P2, W](extractor("max.abs")).andThenRelocate(locate1(".n"))
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its transformed data in 3D" in {
    toU(num3)
      .transformWithValue(
        ext,
        Normalise[P3, W](extractor("max.abs")).andThenRelocate(locate1(".n"))
      )
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.transformAndExpand" should "return its transformed data in 1D" in {
    toU(data1)
      .transform(
        Indicator[P1]()
          .andThenRelocate(locate1(".ind"))
          .andThenRelocate(c => c.position.append("ind").toOption)
      )
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its transformed data in 2D" in {
    toU(data2)
      .transform(
        Indicator[P2]()
          .andThenRelocate(locate1(".ind"))
          .andThenRelocate(c => c.position.append("ind").toOption),
        Binarise[P2, P2](locate2("=")).andThenRelocate(c => c.position.append("bin").toOption)
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its transformed data in 3D" in {
    toU(data3)
      .transform(
        List(
          Indicator[P3]()
            .andThenRelocate(locate1(".ind"))
            .andThenRelocate(c => c.position.append("ind").toOption),
          Binarise[P3, P3](locate2("="))
            .andThenRelocate(c => c.position.append("bin").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.transformAndExpandWithValue" should "return its transformed data in 1D" in {
    toU(num1)
      .transformWithValue(
        ext,
        Normalise[P1, W](extractor("max.abs"))
          .andThenRelocate(locate1(".n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption),
        Standardise[P1, W](extractor("mean"), extractor("sd"))
          .andThenRelocate(locate1(".s"))
          .andThenRelocateWithValue((c, _) => c.position.append("std").toOption)
      )
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its transformed data in 2D" in {
    toU(num2)
      .transformWithValue(
        ext,
        Normalise[P2, W](extractor("max.abs"))
          .andThenRelocate(locate1(".n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption)
      )
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its transformed data in 3D" in {
    toU(num3)
      .transformWithValue(
        ext,
        Normalise[P3, W](extractor("max.abs"))
          .andThenRelocate(locate1(".n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption)
      )
      .toList.sortBy(_.position) shouldBe result12
  }
}

class TestScaldingMatrixTransform extends TestMatrixTransform with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.transform" should "return its transformed data in 1D" in {
    toU(data1)
      .transform(Indicator[P1]().andThenRelocate(locate1(".ind")))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its transformed data in 2D" in {
    toU(data2)
      .transform(
        List(
          Indicator[P2]().andThenRelocate(locate1(".ind")),
          Binarise[P2, P2](locate2("="))
        )
      )
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its transformed data in 3D" in {
    toU(data3)
      .transform(
        Indicator[P3]().andThenRelocate(locate1(".ind")),
        Binarise[P3, P3](locate2("="))
      )
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.transformWithValue" should "return its transformed data in 1D" in {
    toU(num1)
      .transformWithValue(
        ValuePipe(ext),
        List(
          Normalise[P1, W](extractor("max.abs")).andThenRelocate(locate1(".n")),
          Standardise[P1, W](extractor("mean"), extractor("sd")).andThenRelocate(locate1(".s"))
        )
      )
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its transformed data in 2D" in {
    toU(num2)
      .transformWithValue(
        ValuePipe(ext),
        Normalise[P2, W](extractor("max.abs")).andThenRelocate(locate1(".n"))
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its transformed data in 3D" in {
    toU(num3)
      .transformWithValue(
        ValuePipe(ext),
        Normalise[P3, W](extractor("max.abs")).andThenRelocate(locate1(".n"))
      )
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.transformAndExpand" should "return its transformed data in 1D" in {
    toU(data1)
      .transform(
        Indicator[P1]()
          .andThenRelocate(locate1(".ind"))
          .andThenRelocate(c => c.position.append("ind").toOption)
      )
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its transformed data in 2D" in {
    toU(data2)
      .transform(
        Indicator[P2]()
          .andThenRelocate(locate1(".ind"))
          .andThenRelocate(c => c.position.append("ind").toOption),
        Binarise[P2, P2](locate2("=")).andThenRelocate(c => c.position.append("bin").toOption)
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its transformed data in 3D" in {
    toU(data3)
      .transform(
        List(
          Indicator[P3]()
            .andThenRelocate(locate1(".ind"))
            .andThenRelocate(c => c.position.append("ind").toOption),
          Binarise[P3, P3](locate2("="))
            .andThenRelocate(c => c.position.append("bin").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.transformAndExpandWithValue" should "return its transformed data in 1D" in {
    toU(num1)
      .transformWithValue(
        ValuePipe(ext),
        Normalise[P1, W](extractor("max.abs"))
          .andThenRelocate(locate1(".n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption),
        Standardise[P1, W](extractor("mean"), extractor("sd"))
          .andThenRelocate(locate1(".s"))
          .andThenRelocateWithValue((c, _) => c.position.append("std").toOption)
      )
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its transformed data in 2D" in {
    toU(num2)
      .transformWithValue(
        ValuePipe(ext),
        Normalise[P2, W](extractor("max.abs"))
          .andThenRelocate(locate1(".n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption)
      )
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its transformed data in 3D" in {
    toU(num3)
      .transformWithValue(
        ValuePipe(ext),
        Normalise[P3, W](extractor("max.abs"))
          .andThenRelocate(locate1(".n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption)
      )
      .toList.sortBy(_.position) shouldBe result12
  }
}

class TestSparkMatrixTransform extends TestMatrixTransform with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.transform" should "return its transformed data in 1D" in {
    toU(data1)
      .transform(Indicator[P1]().andThenRelocate(locate1(".ind")))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its transformed data in 2D" in {
    toU(data2)
      .transform(
        List(
          Indicator[P2]().andThenRelocate(locate1(".ind")),
          Binarise[P2, P2](locate2("="))
        )
      )
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its transformed data in 3D" in {
    toU(data3)
      .transform(
        Indicator[P3]().andThenRelocate(locate1(".ind")),
        Binarise[P3, P3](locate2("="))
      )
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.transformWithValue" should "return its transformed data in 1D" in {
    toU(num1)
      .transformWithValue(
        ext,
        List(
          Normalise[P1, W](extractor("max.abs")).andThenRelocate(locate1(".n")),
          Standardise[P1, W](extractor("mean"), extractor("sd")).andThenRelocate(locate1(".s"))
        )
      )
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its transformed data in 2D" in {
    toU(num2)
      .transformWithValue(
        ext,
        Normalise[P2, W](extractor("max.abs")).andThenRelocate(locate1(".n"))
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its transformed data in 3D" in {
    toU(num3)
      .transformWithValue(
        ext,
        Normalise[P3, W](extractor("max.abs")).andThenRelocate(locate1(".n"))
      )
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.transformAndExpand" should "return its transformed data in 1D" in {
    toU(data1)
      .transform(
        Indicator[P1]()
          .andThenRelocate(locate1(".ind"))
          .andThenRelocate(c => c.position.append("ind").toOption)
      )
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its transformed data in 2D" in {
    toU(data2)
      .transform(
        Indicator[P2]()
          .andThenRelocate(locate1(".ind"))
          .andThenRelocate(c => c.position.append("ind").toOption),
        Binarise[P2, P2](locate2("=")).andThenRelocate(c => c.position.append("bin").toOption)
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its transformed data in 3D" in {
    toU(data3)
      .transform(
        List(
          Indicator[P3]()
            .andThenRelocate(locate1(".ind"))
            .andThenRelocate(c => c.position.append("ind").toOption),
          Binarise[P3, P3](locate2("="))
            .andThenRelocate(c => c.position.append("bin").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.transformAndExpandWithValue" should "return its transformed data in 1D" in {
    toU(num1)
      .transformWithValue(
        ext,
        Normalise[P1, W](extractor("max.abs"))
          .andThenRelocate(locate1(".n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption),
        Standardise[P1, W](extractor("mean"), extractor("sd"))
          .andThenRelocate(locate1(".s"))
          .andThenRelocateWithValue((c, _) => c.position.append("std").toOption)
      )
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its transformed data in 2D" in {
    toU(num2)
      .transformWithValue(
        ext,
        Normalise[P2, W](extractor("max.abs"))
          .andThenRelocate(locate1(".n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption)
      )
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its transformed data in 3D" in {
    toU(num3)
      .transformWithValue(
        ext,
        Normalise[P3, W](extractor("max.abs"))
          .andThenRelocate(locate1(".n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption)
      )
      .toList.sortBy(_.position) shouldBe result12
  }
}

