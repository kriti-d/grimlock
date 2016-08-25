// Copyright 2015,2016 Commonwealth Bank of Australia
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

package commbank.grimlock

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.partition._

import commbank.grimlock.scalding._
import commbank.grimlock.scalding.environment.Context._

import commbank.grimlock.spark.environment.Context._

import com.twitter.scalding.typed.TypedPipe

import org.apache.spark.rdd._

import shapeless.nat.{ _1, _2 }

trait TestHashPartitioners extends TestGrimlock {

  // In scalding REPL:
  //
  // import commbank.grimlock.framework.encoding._
  //
  // List(4, 14, 35).map { case i => math.abs(LongValue(i).hashCode % 10) }
  // ->  List(4, 9, 0)
  //
  // List("p", "g", "h").map { case s => math.abs(StringValue(s).hashCode % 10) }
  // ->  List(6, 8, 0)

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")

  val cell1 = Cell(Position(4, "p"), Content(ContinuousSchema[Double](), 3.14))
  val cell2 = Cell(Position(14, "g"), Content(ContinuousSchema[Double](), 3.14))
  val cell3 = Cell(Position(35, "h"), Content(ContinuousSchema[Double](), 3.14))
}

class TestBinaryHashSplit extends TestHashPartitioners {

  "A BinaryHashSplit" should "assign left on the first dimension" in {
    BinaryHashSplit[_1, _2, String](_1, 5, "left", "right", 10).assign(cell1) shouldBe List("left")
  }

  it should "assign left on the first dimension when on boundary" in {
    BinaryHashSplit[_1, _2, String](_1, 4, "left", "right", 10).assign(cell1) shouldBe List("left")
  }

  it should "assign right on the first dimension" in {
    BinaryHashSplit[_1, _2, String](_1, 5, "left", "right", 10).assign(cell2) shouldBe List("right")
  }

  it should "assign left on the second dimension" in {
    BinaryHashSplit[_2, _2, String](_2, 7, "left", "right", 10).assign(cell1) shouldBe List("left")
  }

  it should "assign left on the second dimension when on boundary" in {
    BinaryHashSplit[_2, _2, String](_2, 6, "left", "right", 10).assign(cell1) shouldBe List("left")
  }

  it should "assign right on the second dimension" in {
    BinaryHashSplit[_2, _2, String](_2, 7, "left", "right", 10).assign(cell2) shouldBe List("right")
  }
}

class TestTernaryHashSplit extends TestHashPartitioners {

  "A TernaryHashSplit" should "assign left on the first dimension" in {
    TernaryHashSplit[_1, _2, String](_1, 3, 7, "left", "middle", "right", 10).assign(cell3) shouldBe List("left")
  }

  it should "assign left on the first dimension when on boundary" in {
    TernaryHashSplit[_1, _2, String](_1, 4, 7, "left", "middle", "right", 10).assign(cell3) shouldBe List("left")
  }

  it should "assign middle on the first dimension" in {
    TernaryHashSplit[_1, _2, String](_1, 3, 7, "left", "middle", "right", 10).assign(cell1) shouldBe List("middle")
  }

  it should "assign middle on the first dimension when on boundary" in {
    TernaryHashSplit[_1, _2, String](_1, 3, 4, "left", "middle", "right", 10).assign(cell1) shouldBe List("middle")
  }

  it should "assign right on the first dimension" in {
    TernaryHashSplit[_1, _2, String](_1, 4, 7, "left", "middle", "right", 10).assign(cell2) shouldBe List("right")
  }

  it should "assign le2t on the second dimension" in {
    TernaryHashSplit[_2, _2, String](_2, 3, 7, "left", "middle", "right", 10).assign(cell3) shouldBe List("left")
  }

  it should "assign left on the second dimension when on boundary" in {
    TernaryHashSplit[_2, _2, String](_2, 6, 7, "left", "middle", "right", 10).assign(cell3) shouldBe List("left")
  }

  it should "assign middle on the second dimension" in {
    TernaryHashSplit[_2, _2, String](_2, 3, 7, "left", "middle", "right", 10).assign(cell1) shouldBe List("middle")
  }

  it should "assign middle on the second dimension when on boundary" in {
    TernaryHashSplit[_2, _2, String](_2, 3, 8, "left", "middle", "right", 10).assign(cell1) shouldBe List("middle")
  }

  it should "assign right on the second dimension" in {
    TernaryHashSplit[_2, _2, String](_2, 3, 7, "left", "middle", "right", 10).assign(cell2) shouldBe List("right")
  }
}

class TestHashSplit extends TestHashPartitioners {

  val map1 = Map("lower.left" -> ((0, 4)), "upper.left" -> ((1, 5)), "right" -> ((6, 10)))
  val map2 = Map("lower.left" -> ((0, 6)), "upper.left" -> ((1, 7)), "right" -> ((7, 10)))

  "A HashSplit" should "assign both left on the first dimension" in {
    HashSplit[_1, _2, String](_1, map1, 10).assign(cell1) shouldBe List("lower.left", "upper.left")
  }

  it should "assign right on the first dimension" in {
    HashSplit[_1, _2, String](_1, map1, 10).assign(cell2) shouldBe List("right")
  }

  it should "assign none on the first dimension" in {
    HashSplit[_1, _2, String](_1, map1, 10).assign(cell3) shouldBe List()
  }

  it should "assign both left on the second dimension" in {
    HashSplit[_2, _2, String](_2, map2, 10).assign(cell1) shouldBe List("lower.left", "upper.left")
  }

  it should "assign right on the second dimension" in {
    HashSplit[_2, _2, String](_2, map2, 10).assign(cell2) shouldBe List("right")
  }

  it should "assign none on the second dimension" in {
    HashSplit[_2, _2, String](_2, map2, 10).assign(cell3) shouldBe List()
  }
}

trait TestDatePartitioners extends TestGrimlock {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")

  val cell1 = Cell(
    Position(1, DateValue(dfmt.parse("2004-01-01"), DateCodec("yyyy-MM-dd"))),
    Content(ContinuousSchema[Double](), 3.14)
  )
  val cell2 = Cell(
    Position(2, DateValue(dfmt.parse("2006-01-01"), DateCodec("yyyy-MM-dd"))),
    Content(ContinuousSchema[Double](), 3.14)
  )
  val cell3 = Cell(
    Position(3, DateValue(dfmt.parse("2007-01-01"), DateCodec("yyyy-MM-dd"))),
    Content(ContinuousSchema[Double](), 3.14)
  )
}

class TestBinaryDateSplit extends TestDatePartitioners {

  "A BinaryDateSplit" should "assign none on the first dimension" in {
    BinaryDateSplit[_1, _2, String](_1, dfmt.parse("2005-01-01"), "left", "right", DateCodec("yyyy-MM-dd"))
      .assign(cell1) shouldBe List()
  }

  it should "assign left on the second dimension" in {
    BinaryDateSplit[_2, _2, String](_2, dfmt.parse("2005-01-01"), "left", "right", DateCodec("yyyy-MM-dd"))
      .assign(cell1) shouldBe List("left")
  }

  it should "assign left on the second dimension when on boundary" in {
    BinaryDateSplit[_2, _2, String](_2, dfmt.parse("2004-01-01"), "left", "right", DateCodec("yyyy-MM-dd"))
      .assign(cell1) shouldBe List("left")
  }

  it should "assign right on the second dimension" in {
    BinaryDateSplit[_2, _2, String](_2, dfmt.parse("2005-01-01"), "left", "right", DateCodec("yyyy-MM-dd"))
      .assign(cell2) shouldBe List("right")
  }
}

class TestTernaryDateSplit extends TestDatePartitioners {

  "A TernaryDateSplit" should "assign none on the first dimension" in {
    TernaryDateSplit[_1, _2, String](
      _1,
      dfmt.parse("2005-01-01"),
      dfmt.parse("2006-06-30"),
      "left",
      "middle",
      "right",
      DateCodec("yyyy-MM-dd")
    ).assign(cell1) shouldBe List()
  }

  it should "assign left on the second dimension" in {
    TernaryDateSplit[_2, _2, String](
      _2,
      dfmt.parse("2005-01-01"),
      dfmt.parse("2006-06-30"),
      "left",
      "middle",
      "right",
      DateCodec("yyyy-MM-dd")
    ).assign(cell1) shouldBe List("left")
  }

  it should "assign left on the second dimension when on boundary" in {
    TernaryDateSplit[_2, _2, String](
      _2,
      dfmt.parse("2004-01-01"),
      dfmt.parse("2006-06-30"),
      "left",
      "middle",
      "right",
      DateCodec("yyyy-MM-dd")
    ).assign(cell1) shouldBe List("left")
  }

  it should "assign middle on the second dimension" in {
    TernaryDateSplit[_2, _2, String](
      _2,
      dfmt.parse("2005-01-01"),
      dfmt.parse("2006-01-01"),
      "left",
      "middle",
      "right",
      DateCodec("yyyy-MM-dd")
    ).assign(cell2) shouldBe List("middle")
  }

  it should "assign middle on the second dimension when on boundary" in {
    TernaryDateSplit[_2, _2, String](
      _2,
      dfmt.parse("2005-01-01"),
      dfmt.parse("2006-01-01"),
      "left",
      "middle",
      "right",
      DateCodec("yyyy-MM-dd")
    ).assign(cell2) shouldBe List("middle")
  }

  it should "assign right on the second dimension" in {
    TernaryDateSplit[_2, _2, String](
      _2,
      dfmt.parse("2005-01-01"),
      dfmt.parse("2006-06-30"),
      "left",
      "middle",
      "right",
      DateCodec("yyyy-MM-dd")
    ).assign(cell3) shouldBe List("right")
  }
}

class TestDateSplit extends TestDatePartitioners {

  val map1 = Map(
    "lower.left" -> ((dfmt.parse("2003-01-01"), dfmt.parse("2005-01-01"))),
    "upper.left" -> ((dfmt.parse("2003-06-30"), dfmt.parse("2005-06-30"))),
    "right" -> ((dfmt.parse("2006-06-30"), dfmt.parse("2008-01-01")))
  )

  "A DateSplit" should "assign none on the first dimension" in {
    DateSplit[_1, _2, String](_1, map1, DateCodec("yyyy-MM-dd")).assign(cell1) shouldBe List()
  }

  it should "assign both left on the second dimension" in {
    DateSplit[_2, _2, String](_2, map1, DateCodec("yyyy-MM-dd")).assign(cell1) shouldBe List("lower.left", "upper.left")
  }

  it should "assign right on the second dimension" in {
    DateSplit[_2, _2, String](_2, map1, DateCodec("yyyy-MM-dd")).assign(cell3) shouldBe List("right")
  }

  it should "assign none on the second dimension" in {
    DateSplit[_2, _2, String](_2, map1, DateCodec("yyyy-MM-dd")).assign(cell2) shouldBe List()
  }
}

trait TestPartitions extends TestGrimlock {

  val data = List(
    ("train", Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 1))),
    ("train", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 2))),
    ("train", Cell(Position("fid:C"), Content(ContinuousSchema[Long](), 3))),
    ("test", Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 4))),
    ("test", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 5))),
    ("valid", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 6)))
  )

  val data2 = List(
    Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 8)),
    Cell(Position("fid:C"), Content(ContinuousSchema[Long](), 9))
  )

  val result1 = List("test", "train", "valid")

  val result2 = List(
    Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 1)),
    Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 2)),
    Cell(Position("fid:C"), Content(ContinuousSchema[Long](), 3))
  )

  val result3 = data ++ List(
    ("xyz", Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 8))),
    ("xyz", Cell(Position("fid:C"), Content(ContinuousSchema[Long](), 9)))
  )

  val result4 = List(
    ("test", Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 4))),
    ("test", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 5))),
    ("valid", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 6)))
  )

  val result5 = List(
    ("test", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 10))),
    ("valid", Cell(Position("fid:B"), Content(ContinuousSchema[Long](), 12))),
    ("test", Cell(Position("fid:A"), Content(ContinuousSchema[Long](), 8)))
  )
}

object TestPartitions {

  def double(cell: Cell[_1]): Option[Cell[_1]] = cell.content.value.asLong.map(v =>
    Cell(cell.position, Content(ContinuousSchema[Long](), 2 * v))
  )

  def doubleT(key: String, pipe: TypedPipe[Cell[_1]]): TypedPipe[Cell[_1]] = pipe.flatMap(double(_))
  def doubleR(key: String, pipe: RDD[Cell[_1]]): RDD[Cell[_1]] = pipe.flatMap(double(_))
}

class TestScaldingPartitions extends TestPartitions {

  "A Partitions" should "return its ids" in {
    toPipe(data)
      .ids(Default())
      .toList.sorted shouldBe result1
  }

  it should "get a partition's data" in {
    toPipe(data)
      .get("train")
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "add new data" in {
    toPipe(data)
      .add("xyz", toPipe(data2))
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result3
  }

  it should "remove a partition's data" in {
    toPipe(data)
      .remove("train")
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result4
  }

  it should "foreach should apply to selected partitions" in {
    toPipe(data)
      .forEach(List("test", "valid", "not.there"), TestPartitions.doubleT)
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }

  it should "forall should apply to selected partitions" in {
    toPipe(data)
      .forAll(TestPartitions.doubleT, List("train", "not.there"), Default(Execution(scaldingCtx)))
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }

  it should "forall should apply to selected partitions with reducers" in {
    toPipe(data)
      .forAll(
        TestPartitions.doubleT,
        List("train", "not.there"),
        Default(Sequence(Reducers(123), Execution(scaldingCtx)))
      )
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }
}

class TestSparkPartitions extends TestPartitions {

  "A Partitions" should "return its ids" in {
    toRDD(data)
      .ids(Default(Reducers(12)))
      .toList.sorted shouldBe result1
  }

  it should "get a partition's data" in {
    toRDD(data)
      .get("train")
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "add new data" in {
    toRDD(data)
      .add("xyz", toRDD(data2))
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result3
  }

  it should "remove a partition's data" in {
    toRDD(data)
      .remove("train")
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result4
  }

  it should "foreach should apply to selected partitions" in {
    toRDD(data)
      .forEach(List("test", "valid", "not.there"), TestPartitions.doubleR)
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }

  it should "forall should apply to selected partitions" in {
    toRDD(data)
      .forAll(TestPartitions.doubleR, List("train", "not.there"), Default())
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }

  it should "forall should apply to selected partitions with reducers" in {
    toRDD(data)
      .forAll(TestPartitions.doubleR, List("train", "not.there"), Default(Reducers(10)))
      .toList.sortBy(_._2.content.value.toShortString) shouldBe result5
  }
}

