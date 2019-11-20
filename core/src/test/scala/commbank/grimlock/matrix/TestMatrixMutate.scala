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
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _0, _1, _2 }

trait TestMatrixMutate extends TestMatrix {
  val result1 = List(
    Cell(Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result2 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 19.0)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result4 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 19.0)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result5 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result6 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result7 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 19.0)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result8 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 19.0)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result9 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result10 = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 19.0)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 19.0)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result11 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )
}

class TestScalaMatrixMutate extends TestMatrixMutate with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.mutate" should "return its first over data in 1D" in {
    toU(data1)
      .mutate(Over(_0), Default())(
        "foo",
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over data in 2D" in {
    toU(data2)
      .mutate(Over(_0), Default())(
        "foo",
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along data in 2D" in {
    toU(data2)
      .mutate(Along(_0), Default())(
        List(3, 4),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over data in 2D" in {
    toU(data2)
      .mutate(Over(_1), Default())(
        List(3, 4),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along data in 2D" in {
    toU(data2)
      .mutate(Along(_1), Default())(
        "foo",
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over data in 3D" in {
    toU(data3)
      .mutate(Over(_0), Default())(
        "foo",
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along data in 3D" in {
    toU(data3)
      .mutate(Along(_0), Default())(
        List(Position(3, "xyz"), Position(4, "xyz")),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over data in 3D" in {
    toU(data3)
      .mutate(Over(_1), Default())(
        List(3, 4),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along data in 3D" in {
    toU(data3)
      .mutate(Along(_1), Default())(
        Position("foo", "xyz"),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over data in 3D" in {
    toU(data3)
      .mutate(Over(_2), Default())(
        List("xyz"),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along data in 3D" in {
    toU(data3)
      .mutate(Along(_2), Default())(
        Position("foo", 1),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return with empty data - Default" in {
    toU(data3)
      .mutate(Over(_0), Default())(
        List.empty[Position[S31]],
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }
}

class TestScaldingMatrixMutate extends TestMatrixMutate with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.mutate" should "return its first over data in 1D" in {
    toU(data1)
      .mutate(Over(_0), InMemory())(
        "foo",
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over data in 2D" in {
    toU(data2)
      .mutate(Over(_0), Default())(
        "foo",
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along data in 2D" in {
    toU(data2)
      .mutate(Along(_0), Default(12))(
        List(3, 4),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over data in 2D" in {
    toU(data2)
      .mutate(Over(_1), Unbalanced(12))(
        List(3, 4),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along data in 2D" in {
    toU(data2)
      .mutate(Along(_1), InMemory())(
        "foo",
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over data in 3D" in {
    toU(data3)
      .mutate(Over(_0), Default())(
        "foo",
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along data in 3D" in {
    toU(data3)
      .mutate(Along(_0), Default(12))(
        List(Position(3, "xyz"), Position(4, "xyz")),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over data in 3D" in {
    toU(data3)
      .mutate(Over(_1), Unbalanced(12))(
        List(3, 4),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along data in 3D" in {
    toU(data3)
      .mutate(Along(_1), InMemory())(
        Position("foo", "xyz"),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over data in 3D" in {
    toU(data3)
      .mutate(Over(_2), Default())(
        List("xyz"),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along data in 3D" in {
    toU(data3)
      .mutate(Along(_2), Default(12))(
        Position("foo", 1),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return with empty data - InMemory" in {
    toU(data3)
      .mutate(Over(_0), InMemory())(
        List.empty[Position[S31]],
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }

  it should "return with empty data - Default" in {
    toU(data3)
      .mutate(Over(_0), Default())(
        List.empty[Position[S31]],
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }
}

class TestSparkMatrixMutate extends TestMatrixMutate with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.mutate" should "return its first over data in 1D" in {
    toU(data1)
      .mutate(Over(_0), InMemory())(
        "foo",
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over data in 2D" in {
    toU(data2)
      .mutate(Over(_0), Default())(
        "foo",
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along data in 2D" in {
    toU(data2)
      .mutate(Along(_0), Default(12))(
        List(3, 4),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over data in 2D" in {
    toU(data2)
      .mutate(Over(_1), InMemory())(
        List(3, 4),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along data in 2D" in {
    toU(data2)
      .mutate(Along(_1), Default())(
        "foo",
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over data in 3D" in {
    toU(data3)
      .mutate(Over(_0), Default(12))(
        "foo",
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along data in 3D" in {
    toU(data3)
      .mutate(Along(_0), InMemory())(
        List(Position(3, "xyz"), Position(4, "xyz")),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over data in 3D" in {
    toU(data3)
      .mutate(Over(_1), Default())(
        List(3, 4),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along data in 3D" in {
    toU(data3)
      .mutate(Along(_1), Default(12))(
        Position("foo", "xyz"),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over data in 3D" in {
    toU(data3)
      .mutate(Over(_2), InMemory())(
        List("xyz"),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along data in 3D" in {
    toU(data3)
      .mutate(Along(_2), Default())(
        Position("foo", 1),
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return with empty data - Default" in {
    toU(data3)
      .mutate(Over(_0), Default())(
        List.empty[Position[S31]],
        c => Content.decoder(DoubleCodec, ContinuousSchema[Double]())(c.content.value.toShortString)
      )
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }
}

