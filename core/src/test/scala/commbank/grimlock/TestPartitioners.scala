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

import commbank.grimlock.library.partition._

import com.twitter.scalding.typed.TypedPipe

import org.apache.spark.rdd.RDD

import shapeless.{ ::, HList, HNil }
import shapeless.nat.{ _0, _1 }

trait TestHashPartitioners extends TestGrimlock {
  // In scalding REPL:
  //
  // import commbank.grimlock.framework.encoding._
  //
  // List(4, 14, 35).map { case i => math.abs(IntValue(i).hashCode % 10) }
  // ->  List(8, 4, 7)
  //
  // List("p", "g", "h").map { case s => math.abs(StringValue(s).hashCode % 10) }
  // ->  List(6, 8, 0)

  type P = Value[Int] :: Value[String] :: HNil

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")

  val cell1 = Cell(Position(4, "p"), Content(ContinuousSchema[Double](), 3.14))
  val cell2 = Cell(Position(14, "g"), Content(ContinuousSchema[Double](), 3.14))
  val cell3 = Cell(Position(35, "h"), Content(ContinuousSchema[Double](), 3.14))
}

class TestBinaryHashSplit extends TestHashPartitioners {
  "A BinaryHashSplit" should "assign left on the first dimension" in {
    BinaryHashSplit[P, _0, String](_0, 9, "left", "right", 10).assign(cell1) shouldBe List("left")
  }

  it should "assign left on the first dimension when on boundary" in {
    BinaryHashSplit[P, _0, String](_0, 8, "left", "right", 10).assign(cell1) shouldBe List("left")
  }

  it should "assign right on the first dimension" in {
    BinaryHashSplit[P, _0, String](_0, 7, "left", "right", 10).assign(cell1) shouldBe List("right")
  }

  it should "assign left on the second dimension" in {
    BinaryHashSplit[P, _1, String](_1, 9, "left", "right", 10).assign(cell2) shouldBe List("left")
  }

  it should "assign left on the second dimension when on boundary" in {
    BinaryHashSplit[P, _1, String](_1, 8, "left", "right", 10).assign(cell2) shouldBe List("left")
  }

  it should "assign right on the second dimension" in {
    BinaryHashSplit[P, _1, String](_1, 7, "left", "right", 10).assign(cell2) shouldBe List("right")
  }
}

class TestTernaryHashSplit extends TestHashPartitioners {
  "A TernaryHashSplit" should "assign left on the first dimension" in {
    TernaryHashSplit[P, _0, String](_0, 5, 8, "left", "middle", "right", 10).assign(cell2) shouldBe List("left")
  }

  it should "assign left on the first dimension when on boundary" in {
    TernaryHashSplit[P, _0, String](_0, 4, 8, "left", "middle", "right", 10).assign(cell2) shouldBe List("left")
  }

  it should "assign middle on the first dimension" in {
    TernaryHashSplit[P, _0, String](_0, 5, 8, "left", "middle", "right", 10).assign(cell3) shouldBe List("middle")
  }

  it should "assign middle on the first dimension when on boundary" in {
    TernaryHashSplit[P, _0, String](_0, 5, 7, "left", "middle", "right", 10).assign(cell3) shouldBe List("middle")
  }

  it should "assign right on the first dimension" in {
    TernaryHashSplit[P, _0, String](_0, 5, 7, "left", "middle", "right", 10).assign(cell1) shouldBe List("right")
  }

  it should "assign left on the second dimension" in {
    TernaryHashSplit[P, _1, String](_1, 7, 9, "left", "middle", "right", 10).assign(cell1) shouldBe List("left")
  }

  it should "assign left on the second dimension when on boundary" in {
    TernaryHashSplit[P, _1, String](_1, 6, 9, "left", "middle", "right", 10).assign(cell1) shouldBe List("left")
  }

  it should "assign middle on the second dimension" in {
    TernaryHashSplit[P, _1, String](_1, 7, 9, "left", "middle", "right", 10).assign(cell2) shouldBe List("middle")
  }

  it should "assign middle on the second dimension when on boundary" in {
    TernaryHashSplit[P, _1, String](_1, 7, 8, "left", "middle", "right", 10).assign(cell2) shouldBe List("middle")
  }

  it should "assign right on the second dimension" in {
    TernaryHashSplit[P, _1, String](_1, 2, 7, "left", "middle", "right", 10).assign(cell2) shouldBe List("right")
  }
}

class TestHashSplit extends TestHashPartitioners {
  val map1 = Map("lower.left" -> ((0, 5)), "upper.left" -> ((1, 6)), "right" -> ((6, 7)))
  val map2 = Map("lower.left" -> ((1, 6)), "upper.left" -> ((2, 7)), "right" -> ((6, 10)))

  "A HashSplit" should "assign both left on the first dimension" in {
    HashSplit[P, _0, String](_0, map1, 10).assign(cell2) shouldBe List("lower.left", "upper.left")
  }

  it should "assign right on the first dimension" in {
    HashSplit[P, _0, String](_0, map1, 10).assign(cell3) shouldBe List("right")
  }

  it should "assign none on the first dimension" in {
    HashSplit[P, _0, String](_0, map1, 10).assign(cell1) shouldBe List()
  }

  it should "assign both left on the second dimension" in {
    HashSplit[P, _1, String](_1, map2, 10).assign(cell1) shouldBe List("lower.left", "upper.left")
  }

  it should "assign right on the second dimension" in {
    HashSplit[P, _1, String](_1, map2, 10).assign(cell2) shouldBe List("right")
  }

  it should "assign none on the second dimension" in {
    HashSplit[P, _1, String](_1, map2, 10).assign(cell3) shouldBe List()
  }
}

trait TestDatePartitioners extends TestGrimlock {
  type P = Value[Int] :: Value[java.util.Date] :: HNil

  implicit def toDate(date: java.util.Date): Value[java.util.Date] = DateValue(date, codec)

  val codec = DateCodec("yyyy-MM-dd")

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")

  val cell1 = Cell(Position(1, dfmt.parse("2004-01-01")), Content(ContinuousSchema[Double](), 3.14))
  val cell2 = Cell(Position(2, dfmt.parse("2006-01-01")), Content(ContinuousSchema[Double](), 3.14))
  val cell3 = Cell(Position(3, dfmt.parse("2007-01-01")), Content(ContinuousSchema[Double](), 3.14))
}

class TestBinaryDateSplit extends TestDatePartitioners {
  "A BinaryDateSplit" should "assign left on the second dimension" in {
    BinaryDateSplit[P, _1, String](_1, dfmt.parse("2005-01-01"), "left", "right", codec)
      .assign(cell1) shouldBe List("left")
  }

  it should "assign left on the second dimension when on boundary" in {
    BinaryDateSplit[P, _1, String](_1, dfmt.parse("2004-01-01"), "left", "right", codec)
      .assign(cell1) shouldBe List("left")
  }

  it should "assign right on the second dimension" in {
    BinaryDateSplit[P, _1, String](_1, dfmt.parse("2005-01-01"), "left", "right", codec)
      .assign(cell2) shouldBe List("right")
  }
}

class TestTernaryDateSplit extends TestDatePartitioners {
  "A TernaryDateSplit" should "assign left on the second dimension" in {
    TernaryDateSplit[P, _1, String](
      _1,
      dfmt.parse("2005-01-01"),
      dfmt.parse("2006-06-30"),
      "left",
      "middle",
      "right",
      codec
    ).assign(cell1) shouldBe List("left")
  }

  it should "assign left on the second dimension when on boundary" in {
    TernaryDateSplit[P, _1, String](
      _1,
      dfmt.parse("2004-01-01"),
      dfmt.parse("2006-06-30"),
      "left",
      "middle",
      "right",
      codec
    ).assign(cell1) shouldBe List("left")
  }

  it should "assign middle on the second dimension" in {
    TernaryDateSplit[P, _1, String](
      _1,
      dfmt.parse("2005-01-01"),
      dfmt.parse("2006-01-01"),
      "left",
      "middle",
      "right",
      codec
    ).assign(cell2) shouldBe List("middle")
  }

  it should "assign middle on the second dimension when on boundary" in {
    TernaryDateSplit[P, _1, String](
      _1,
      dfmt.parse("2005-01-01"),
      dfmt.parse("2006-01-01"),
      "left",
      "middle",
      "right",
      codec
    ).assign(cell2) shouldBe List("middle")
  }

  it should "assign right on the second dimension" in {
    TernaryDateSplit[P, _1, String](
      _1,
      dfmt.parse("2005-01-01"),
      dfmt.parse("2006-06-30"),
      "left",
      "middle",
      "right",
      codec
    ).assign(cell3) shouldBe List("right")
  }
}

class TestDateSplit extends TestDatePartitioners {
  val map1 = Map(
    "lower.left" -> ((dfmt.parse("2003-01-01"), dfmt.parse("2005-01-01"))),
    "upper.left" -> ((dfmt.parse("2003-06-30"), dfmt.parse("2005-06-30"))),
    "right" -> ((dfmt.parse("2006-06-30"), dfmt.parse("2008-01-01")))
  )

  "A DateSplit" should "assign both left on the second dimension" in {
    DateSplit[P, _1, String](_1, map1, codec).assign(cell1) shouldBe List("lower.left", "upper.left")
  }

  it should "assign right on the second dimension" in {
    DateSplit[P, _1, String](_1, map1, codec).assign(cell3) shouldBe List("right")
  }

  it should "assign none on the second dimension" in {
    DateSplit[P, _1, String](_1, map1, codec).assign(cell2) shouldBe List()
  }
}

trait TestPartitions extends TestGrimlock {
  val data = List(
    ("train", Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 1L))),
    ("train", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 2L))),
    ("train", Cell(Position("fid:C"), Content(ContinuousSchema[Long](), 3L))),
    ("test", Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 4L))),
    ("test", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 5L))),
    ("valid", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 6L)))
  )

  val data2 = List(
    Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 8L)),
    Cell(Position("fid:C"), Content(ContinuousSchema[Long](), 9L))
  )

  val result1 = List("test", "train", "valid")

  val result2 = List(
    Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 1L)),
    Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 2L)),
    Cell(Position("fid:C"), Content(ContinuousSchema[Long](), 3L))
  )

  val result3 = data ++ List(
    ("xyz", Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 8L))),
    ("xyz", Cell(Position("fid:C"), Content(ContinuousSchema[Long](), 9L)))
  )

  val result4 = List(
    ("test", Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 4L))),
    ("test", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 5L))),
    ("valid", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 6L)))
  )

  val result5 = List(
    ("test", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 10L))),
    ("valid", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 12L))),
    ("test", Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 8L)))
  )
}

object TestPartitions {
  def double[P <: HList](cell: Cell[P]): Option[Cell[P]] = cell.content.value.as[Long].map(v =>
    Cell(cell.position, Content(ContinuousSchema[Long](), 2 * v))
  )

  def doubleS[P <: HList](key: String, list: List[Cell[P]]): List[Cell[P]] = list.flatMap(double(_))
  def doubleT[P <: HList](key: String, pipe: TypedPipe[Cell[P]]): TypedPipe[Cell[P]] = pipe.flatMap(double(_))
  def doubleR[P <: HList](key: String, rdd: RDD[Cell[P]]): RDD[Cell[P]] = rdd.flatMap(double(_))
}

class TestScalaPartitions extends TestPartitions with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Partitions" should "return its ids" in {
    toU(data)
      .ids(Default())
      .toList.sorted shouldBe result1
  }

  it should "get a partition's data" in {
    toU(data)
      .get("train")
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "add new data" in {
    toU(data)
      .add("xyz", toU(data2))
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result3
  }

  it should "remove a partition's data" in {
    toU(data)
      .remove("train")
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result4
  }

  it should "foreach should apply to selected partitions" in {
    toU(data)
      .forEach(List("test", "valid", "not.there"), TestPartitions.doubleS)
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }

  it should "forall should apply to selected partitions" in {
    toU(data)
      .forAll(ctx, TestPartitions.doubleS, List("train", "not.there"), Default())
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }

  it should "forall should apply to selected partitions with reducers" in {
    toU(data)
      .forAll(ctx, TestPartitions.doubleS, List("train", "not.there"), Default())
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }
}

class TestScaldingPartitions extends TestPartitions with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Partitions" should "return its ids" in {
    toU(data)
      .ids(Default())
      .toList.sorted shouldBe result1
  }

  it should "get a partition's data" in {
    toU(data)
      .get("train")
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "add new data" in {
    toU(data)
      .add("xyz", toU(data2))
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result3
  }

  it should "remove a partition's data" in {
    toU(data)
      .remove("train")
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result4
  }

  it should "foreach should apply to selected partitions" in {
    toU(data)
      .forEach(List("test", "valid", "not.there"), TestPartitions.doubleT)
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }

  it should "forall should apply to selected partitions" in {
    toU(data)
      .forAll(ctx, TestPartitions.doubleT, List("train", "not.there"), Default())
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }

  it should "forall should apply to selected partitions with reducers" in {
    toU(data)
      .forAll(ctx, TestPartitions.doubleT, List("train", "not.there"), Default(10))
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }
}

class TestSparkPartitions extends TestPartitions with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Partitions" should "return its ids" in {
    toU(data)
      .ids(Default(12))
      .toList.sorted shouldBe result1
  }

  it should "get a partition's data" in {
    toU(data)
      .get("train")
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "add new data" in {
    toU(data)
      .add("xyz", toU(data2))
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result3
  }

  it should "remove a partition's data" in {
    toU(data)
      .remove("train")
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result4
  }

  it should "foreach should apply to selected partitions" in {
    toU(data)
      .forEach(List("test", "valid", "not.there"), TestPartitions.doubleR)
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }

  it should "forall should apply to selected partitions" in {
    toU(data)
      .forAll(ctx, TestPartitions.doubleR, List("train", "not.there"), Default())
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }

  it should "forall should apply to selected partitions with reducers" in {
    toU(data)
      .forAll(ctx, TestPartitions.doubleR, List("train", "not.there"), Default(10))
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }
}

