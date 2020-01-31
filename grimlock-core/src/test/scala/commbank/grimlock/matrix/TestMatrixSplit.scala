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
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.partition._
import commbank.grimlock.framework.position._

import com.twitter.scalding.typed.ValuePipe

import shapeless.{ HList, Nat }
import shapeless.nat.{ _0, _1, _2 }

trait TestMatrixSplit extends TestMatrix {
  implicit val TO1 = TestMatrixSplit.TupleOrdering[P1]
  implicit val TO2 = TestMatrixSplit.TupleOrdering[P2]
  implicit val TO3 = TestMatrixSplit.TupleOrdering[P3]

  val result1 = data1.map(c => (c.position(_0).toShortString, c)).sorted

  val result2 = data2.map(c => (c.position(_0).toShortString, c)).sorted

  val result3 = data2.map(c => (c.position(_1).toShortString, c)).sorted

  val result4 = data3.map(c => (c.position(_0).toShortString, c)).sorted

  val result5 = data3.map(c => (c.position(_1).toShortString, c)).sorted

  val result6 = data3.map(c => (c.position(_2).toShortString, c)).sorted

  val result7 = data1.map(c => (c.position(_0).toShortString, c)).sorted

  val result8 = data2.map(c => (c.position(_0).toShortString, c)).sorted

  val result9 = data2.map(c => (c.position(_1).toShortString, c)).sorted

  val result10 = data3.map(c => (c.position(_0).toShortString, c)).sorted

  val result11 = data3.map(c => (c.position(_1).toShortString, c)).sorted

  val result12 = data3.map(c => (c.position(_2).toShortString, c)).sorted
}

object TestMatrixSplit {
  case class TestPartitioner[
    P <: HList,
    D <: Nat
  ](
    dim: D
  )(implicit
    ev: Position.IndexConstraints[P, D] { type V <: Value[_] }
  ) extends Partitioner[P, String] {
    def assign(cell: Cell[P]): TraversableOnce[String] = List(cell.position(dim).toShortString)
  }

  case class TestPartitionerWithValue[
    P <: HList,
    D <: Nat
  ](implicit
    ev: Position.IndexConstraints[P, D] { type V <: Value[_] }
  ) extends PartitionerWithValue[P, String] {
    type V = D
    def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[String] = List(cell.position(ext).toShortString)
  }

  def TupleOrdering[P <: HList](): Ordering[(String, Cell[P])] = new Ordering[(String, Cell[P])] {
    def compare(x: (String, Cell[P]), y: (String, Cell[P])): Int = x._1.compare(y._1) match {
      case cmp if (cmp == 0) => x._2.position.compare(y._2.position)
      case cmp => cmp
    }
  }
}

class TestScalaMatrixSplit extends TestMatrixSplit with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.split" should "return its first partitions in 1D" in {
    toU(data1)
      .split(TestMatrixSplit.TestPartitioner(_0))
      .toList.sorted shouldBe result1
  }

  it should "return its first partitions in 2D" in {
    toU(data2)
      .split(TestMatrixSplit.TestPartitioner(_0))
      .toList.sorted shouldBe result2
  }

  it should "return its second partitions in 2D" in {
    toU(data2)
      .split(TestMatrixSplit.TestPartitioner(_1))
      .toList.sorted shouldBe result3
  }

  it should "return its first partitions in 3D" in {
    toU(data3)
      .split(TestMatrixSplit.TestPartitioner(_0))
      .toList.sorted shouldBe result4
  }

  it should "return its second partitions in 3D" in {
    toU(data3)
      .split(TestMatrixSplit.TestPartitioner(_1))
      .toList.sorted shouldBe result5
  }

  it should "return its third partitions in 3D" in {
    toU(data3)
      .split(TestMatrixSplit.TestPartitioner(_2))
      .toList.sorted shouldBe result6
  }

  "A Matrix.splitWithValue" should "return its first partitions in 1D" in {
    toU(data1)
      .splitWithValue(_0, TestMatrixSplit.TestPartitionerWithValue[P1, _0])
      .toList.sorted shouldBe result7
  }

  it should "return its first partitions in 2D" in {
    toU(data2)
      .splitWithValue(_0, TestMatrixSplit.TestPartitionerWithValue[P2, _0])
      .toList.sorted shouldBe result8
  }

  it should "return its second partitions in 2D" in {
    toU(data2)
      .splitWithValue(_1, TestMatrixSplit.TestPartitionerWithValue[P2, _1])
      .toList.sorted shouldBe result9
  }

  it should "return its first partitions in 3D" in {
    toU(data3)
      .splitWithValue(_0, TestMatrixSplit.TestPartitionerWithValue[P3, _0])
      .toList.sorted shouldBe result10
  }

  it should "return its second partitions in 3D" in {
    toU(data3)
      .splitWithValue(_1, TestMatrixSplit.TestPartitionerWithValue[P3, _1])
      .toList.sorted shouldBe result11
  }

  it should "return its third partitions in 3D" in {
    toU(data3)
      .splitWithValue(_2, TestMatrixSplit.TestPartitionerWithValue[P3, _2])
      .toList.sorted shouldBe result12
  }
}

class TestScaldingMatrixSplit extends TestMatrixSplit with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.split" should "return its first partitions in 1D" in {
    toU(data1)
      .split(TestMatrixSplit.TestPartitioner(_0))
      .toList.sorted shouldBe result1
  }

  it should "return its first partitions in 2D" in {
    toU(data2)
      .split(TestMatrixSplit.TestPartitioner(_0))
      .toList.sorted shouldBe result2
  }

  it should "return its second partitions in 2D" in {
    toU(data2)
      .split(TestMatrixSplit.TestPartitioner(_1))
      .toList.sorted shouldBe result3
  }

  it should "return its first partitions in 3D" in {
    toU(data3)
      .split(TestMatrixSplit.TestPartitioner(_0))
      .toList.sorted shouldBe result4
  }

  it should "return its second partitions in 3D" in {
    toU(data3)
      .split(TestMatrixSplit.TestPartitioner(_1))
      .toList.sorted shouldBe result5
  }

  it should "return its third partitions in 3D" in {
    toU(data3)
      .split(TestMatrixSplit.TestPartitioner(_2))
      .toList.sorted shouldBe result6
  }

  "A Matrix.splitWithValue" should "return its first partitions in 1D" in {
    toU(data1)
      .splitWithValue(ValuePipe(_0), TestMatrixSplit.TestPartitionerWithValue[P1, _0])
      .toList.sorted shouldBe result7
  }

  it should "return its first partitions in 2D" in {
    toU(data2)
      .splitWithValue(ValuePipe(_0), TestMatrixSplit.TestPartitionerWithValue[P2, _0])
      .toList.sorted shouldBe result8
  }

  it should "return its second partitions in 2D" in {
    toU(data2)
      .splitWithValue(ValuePipe(_1), TestMatrixSplit.TestPartitionerWithValue[P2, _1])
      .toList.sorted shouldBe result9
  }

  it should "return its first partitions in 3D" in {
    toU(data3)
      .splitWithValue(ValuePipe(_0), TestMatrixSplit.TestPartitionerWithValue[P3, _0])
      .toList.sorted shouldBe result10
  }

  it should "return its second partitions in 3D" in {
    toU(data3)
      .splitWithValue(ValuePipe(_1), TestMatrixSplit.TestPartitionerWithValue[P3, _1])
      .toList.sorted shouldBe result11
  }

  it should "return its third partitions in 3D" in {
    toU(data3)
      .splitWithValue(ValuePipe(_2), TestMatrixSplit.TestPartitionerWithValue[P3, _2])
      .toList.sorted shouldBe result12
  }
}

class TestSparkMatrixSplit extends TestMatrixSplit with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.split" should "return its first partitions in 1D" in {
    toU(data1)
      .split(TestMatrixSplit.TestPartitioner(_0))
      .toList.sorted shouldBe result1
  }

  it should "return its first partitions in 2D" in {
    toU(data2)
      .split(TestMatrixSplit.TestPartitioner(_0))
      .toList.sorted shouldBe result2
  }

  it should "return its second partitions in 2D" in {
    toU(data2)
      .split(TestMatrixSplit.TestPartitioner(_1))
      .toList.sorted shouldBe result3
  }

  it should "return its first partitions in 3D" in {
    toU(data3)
      .split(TestMatrixSplit.TestPartitioner(_0))
      .toList.sorted shouldBe result4
  }

  it should "return its second partitions in 3D" in {
    toU(data3)
      .split(TestMatrixSplit.TestPartitioner(_1))
      .toList.sorted shouldBe result5
  }

  it should "return its third partitions in 3D" in {
    toU(data3)
      .split(TestMatrixSplit.TestPartitioner(_2))
      .toList.sorted shouldBe result6
  }

  "A Matrix.splitWithValue" should "return its first partitions in 1D" in {
    toU(data1)
      .splitWithValue(_0, TestMatrixSplit.TestPartitionerWithValue[P1, _0])
      .toList.sorted shouldBe result7
  }

  it should "return its first partitions in 2D" in {
    toU(data2)
      .splitWithValue(_0, TestMatrixSplit.TestPartitionerWithValue[P2, _0])
      .toList.sorted shouldBe result8
  }

  it should "return its second partitions in 2D" in {
    toU(data2)
      .splitWithValue(_1, TestMatrixSplit.TestPartitionerWithValue[P2, _1])
      .toList.sorted shouldBe result9
  }

  it should "return its first partitions in 3D" in {
    toU(data3)
      .splitWithValue(_0, TestMatrixSplit.TestPartitionerWithValue[P3, _0])
      .toList.sorted shouldBe result10
  }

  it should "return its second partitions in 3D" in {
    toU(data3)
      .splitWithValue(_1, TestMatrixSplit.TestPartitionerWithValue[P3, _1])
      .toList.sorted shouldBe result11
  }

  it should "return its third partitions in 3D" in {
    toU(data3)
      .splitWithValue(_2, TestMatrixSplit.TestPartitionerWithValue[P3, _2])
      .toList.sorted shouldBe result12
  }
}

