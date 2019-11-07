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
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import shapeless.HList
import shapeless.nat.{ _0, _1, _2 }

trait TestMatrixWhich extends TestMatrix {
  val result1 = List(Position("qux"))

  val result2 = List(Position("qux"))

  val result3 = List(Position("foo"), Position("qux"))

  val result4 = List(Position("foo", 3), Position("foo", 4), Position("qux", 1))

  val result5 = List(Position("qux", 1))

  val result6 = List(Position("foo", 4))

  val result7 = List(Position("foo", 4))

  val result8 = List(Position("qux", 1))

  val result9 = List(Position("foo", 1), Position("foo", 2), Position("qux", 1))

  val result10 = List(Position("bar", 2), Position("baz", 2), Position("foo", 2), Position("foo", 4))

  val result11 = List(Position("bar", 2), Position("baz", 2), Position("foo", 2), Position("foo", 4))

  val result12 = List(Position("foo", 1), Position("foo", 2), Position("qux", 1))

  val result13 = List(Position("foo", 3, "xyz"), Position("foo", 4, "xyz"), Position("qux", 1, "xyz"))

  val result14 = List(Position("qux", 1, "xyz"))

  val result15 = List(Position("foo", 4, "xyz"))

  val result16 = List(Position("foo", 4, "xyz"))

  val result17 = List(Position("qux", 1, "xyz"))

  val result18 = List(Position("foo", 3, "xyz"), Position("foo", 4, "xyz"), Position("qux", 1, "xyz"))

  val result19 = List(Position("qux", 1, "xyz"))

  val result20 = List(Position("foo", 1, "xyz"), Position("foo", 2, "xyz"), Position("qux", 1, "xyz"))

  val result21 = List(
    Position("bar", 2, "xyz"),
    Position("baz", 2, "xyz"),
    Position("foo", 2, "xyz"),
    Position("foo", 4, "xyz")
  )

  val result22 = List(
    Position("bar", 2, "xyz"),
    Position("baz", 2, "xyz"),
    Position("foo", 2, "xyz"),
    Position("foo", 4, "xyz")
  )

  val result23 = List(Position("foo", 1, "xyz"), Position("foo", 2, "xyz"), Position("qux", 1, "xyz"))

  val result24 = data3.map(_.position).sorted

  val result25 = List(Position("foo", 2, "xyz"), Position("qux", 1, "xyz"))
}

object TestMatrixWhich {
  def predicate[P <: HList](cell: Cell[P]): Boolean =
    (cell.content.classification == NominalType) ||
    (cell.content.value.codec.isInstanceOf[DateCodec]) ||
    (cell.content.value equ "12.56")
}

class TestScalaMatrixWhich extends TestMatrixWhich with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.which" should "return its coordinates in 1D" in {
    toU(data1)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result1
  }

  it should "return its first over coordinates in 1D" in {
    toU(data1)
      .whichByPosition(Over(_0), Default())((List("bar", "qux"), (c: Cell[P1]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result2
  }

  it should "return its first over multiple coordinates in 1D" in {
    toU(data1)
      .whichByPosition(Over(_0), Default())(
        List(
          (List("bar", "qux"), (c: Cell[P1]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[P1]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result3
  }

  it should "return its coordinates in 2D" in {
    toU(data2)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result4
  }

  it should "return its first over coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Over(_0), Default())((List("bar", "qux"), (c: Cell[P2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result5
  }

  it should "return its first along coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Along(_0), Default())((List(2, 4), (c: Cell[P2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result6
  }

  it should "return its second over coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Over(_1), Default())((List(2, 4), (c: Cell[P2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result7
  }

  it should "return its second along coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Along(_1), Default())((List("bar", "qux"), (c: Cell[P2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result8
  }

  it should "return its first over multiple coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Over(_0), Default())(
        List(
          (List("bar", "qux"), (c: Cell[P2]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[P2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result9
  }

  it should "return its first along multiple coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Along(_0), Default())(
        List(
          (List(2, 4), (c: Cell[P2]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[P2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result10
  }

  it should "return its second over multiple coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Over(_1), Default())(
        List(
          (List(2, 4), (c: Cell[P2]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[P2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result11
  }

  it should "return its second along multiple coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Along(_1), Default())(
        List(
          (List("bar", "qux"), (c: Cell[P2]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[P2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result12
  }

  it should "return its coordinates in 3D" in {
    toU(data3)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result13
  }

  it should "return its first over coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_0), Default())((List("bar", "qux"), (c: Cell[P3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result14
  }

  it should "return its first along coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_0), Default())(
        (List(Position(2, "xyz"), Position(4, "xyz")), (c: Cell[P3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result15
  }

  it should "return its second over coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_1), Default())((List(2, 4), (c: Cell[P3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result16
  }

  it should "return its second along coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_1), Default())(
        (List(Position("bar", "xyz"), Position("qux", "xyz")), (c: Cell[P3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result17
  }

  it should "return its third over coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_2), Default())(("xyz", (c: Cell[P3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result18
  }

  it should "return its third along coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_2), Default())(
        (List(Position("bar", 2), Position("qux", 1)), (c: Cell[P3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result19
  }

  it should "return its first over multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_0), Default())(
        List(
          (List("bar", "qux"), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result20
  }

  it should "return its first along multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_0), Default())(
        List(
          (List(Position(2, "xyz"), Position(4, "xyz")), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List(Position(2, "xyz")), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result21
  }

  it should "return its second over multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_1), Default())(
        List(
          (List(2, 4), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result22
  }

  it should "return its second along multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_1), Default())(
        List(
          (List(Position("bar", "xyz"), Position("qux", "xyz")), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List(Position("foo", "xyz")), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result23
  }

  it should "return its third over multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_2), Default())(
        List(
          ("xyz", (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          ("xyz", (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result24
  }

  it should "return its third along multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_2), Default())(
        List(
          (List(Position("foo", 1), Position("qux", 1)), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List(Position("foo", 2)), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result25
  }

  it should "return empty data - Default" in {
    toU(data3)
      .whichByPosition(Along(_2), Default())(
        List((List(), (c: Cell[P3]) => !TestMatrixWhich.predicate(c)))
      )
      .toList.sorted shouldBe List()
  }
}

class TestScaldingMatrixWhich extends TestMatrixWhich with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.which" should "return its coordinates in 1D" in {
    toU(data1)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result1
  }

  it should "return its first over coordinates in 1D" in {
    toU(data1)
      .whichByPosition(Over(_0), InMemory())((List("bar", "qux"), (c: Cell[P1]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result2
  }

  it should "return its first over multiple coordinates in 1D" in {
    toU(data1)
      .whichByPosition(Over(_0), Default())(
        List(
          (List("bar", "qux"), (c: Cell[P1]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[P1]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result3
  }

  it should "return its coordinates in 2D" in {
    toU(data2)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result4
  }

  it should "return its first over coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Over(_0), Default(12))((List("bar", "qux"), (c: Cell[P2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result5
  }

  it should "return its first along coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Along(_0), Unbalanced(12))((List(2, 4), (c: Cell[P2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result6
  }

  it should "return its second over coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Over(_1), InMemory())((List(2, 4), (c: Cell[P2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result7
  }

  it should "return its second along coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Along(_1), Default())((List("bar", "qux"), (c: Cell[P2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result8
  }

  it should "return its first over multiple coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Over(_0), Default(12))(
        List(
          (List("bar", "qux"), (c: Cell[P2]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[P2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result9
  }

  it should "return its first along multiple coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Along(_0), Unbalanced(12))(
        List(
          (List(2, 4), (c: Cell[P2]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[P2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result10
  }

  it should "return its second over multiple coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Over(_1), InMemory())(
        List(
          (List(2, 4), (c: Cell[P2]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[P2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result11
  }

  it should "return its second along multiple coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Along(_1), Default())(
        List(
          (List("bar", "qux"), (c: Cell[P2]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[P2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result12
  }

  it should "return its coordinates in 3D" in {
    toU(data3)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result13
  }

  it should "return its first over coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_0), Default(12))((List("bar", "qux"), (c: Cell[P3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result14
  }

  it should "return its first along coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_0), Unbalanced(12))(
        (List(Position(2, "xyz"), Position(4, "xyz")), (c: Cell[P3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result15
  }

  it should "return its second over coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_1), InMemory())((List(2, 4), (c: Cell[P3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result16
  }

  it should "return its second along coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_1), Default())(
        (List(Position("bar", "xyz"), Position("qux", "xyz")), (c: Cell[P3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result17
  }

  it should "return its third over coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_2), Default(12))(("xyz", (c: Cell[P3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result18
  }

  it should "return its third along coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_2), Unbalanced(12))(
        (List(Position("bar", 2), Position("qux", 1)), (c: Cell[P3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result19
  }

  it should "return its first over multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_0), InMemory())(
        List(
          (List("bar", "qux"), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result20
  }

  it should "return its first along multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_0), Default())(
        List(
          (List(Position(2, "xyz"), Position(4, "xyz")), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List(Position(2, "xyz")), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result21
  }

  it should "return its second over multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_1), Default(12))(
        List(
          (List(2, 4), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result22
  }

  it should "return its second along multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_1), Unbalanced(12))(
        List(
          (List(Position("bar", "xyz"), Position("qux", "xyz")), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List(Position("foo", "xyz")), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result23
  }

  it should "return its third over multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_2), InMemory())(
        List(
          ("xyz", (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          ("xyz", (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result24
  }

  it should "return its third along multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_2), Default())(
        List(
          (List(Position("foo", 1), Position("qux", 1)), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List(Position("foo", 2)), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result25
  }

  it should "return empty data - InMemory" in {
    toU(data3)
      .whichByPosition(Along(_2), InMemory())(
        List((List(), (c: Cell[P3]) => !TestMatrixWhich.predicate(c)))
      )
      .toList.sorted shouldBe List()
  }

  it should "return empty data - Default" in {
    toU(data3)
      .whichByPosition(Along(_2), Default())(
        List((List(), (c: Cell[P3]) => !TestMatrixWhich.predicate(c)))
      )
      .toList.sorted shouldBe List()
  }
}

class TestSparkMatrixWhich extends TestMatrixWhich with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.which" should "return its coordinates in 1D" in {
    toU(data1)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result1
  }

  it should "return its first over coordinates in 1D" in {
    toU(data1)
      .whichByPosition(Over(_0), InMemory())((List("bar", "qux"), (c: Cell[P1]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result2
  }

  it should "return its first over multiple coordinates in 1D" in {
    toU(data1)
      .whichByPosition(Over(_0), Default())(
        List(
          (List("bar", "qux"), (c: Cell[P1]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[P1]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result3
  }

  it should "return its coordinates in 2D" in {
    toU(data2)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result4
  }

  it should "return its first over coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Over(_0), Default(12))((List("bar", "qux"), (c: Cell[P2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result5
  }

  it should "return its first along coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Along(_0), InMemory())((List(2, 4), (c: Cell[P2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result6
  }

  it should "return its second over coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Over(_1), Default())((List(2, 4), (c: Cell[P2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result7
  }

  it should "return its second along coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Along(_1), Default(12))((List("bar", "qux"), (c: Cell[P2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result8
  }

  it should "return its first over multiple coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Over(_0), InMemory())(
        List(
          (List("bar", "qux"), (c: Cell[P2]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[P2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result9
  }

  it should "return its first along multiple coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Along(_0), Default())(
        List(
          (List(2, 4), (c: Cell[P2]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[P2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result10
  }

  it should "return its second over multiple coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Over(_1), Default(12))(
        List(
          (List(2, 4), (c: Cell[P2]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[P2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result11
  }

  it should "return its second along multiple coordinates in 2D" in {
    toU(data2)
      .whichByPosition(Along(_1), InMemory())(
        List(
          (List("bar", "qux"), (c: Cell[P2]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[P2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result12
  }

  it should "return its coordinates in 3D" in {
    toU(data3)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result13
  }

  it should "return its first over coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_0), Default())((List("bar", "qux"), (c: Cell[P3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result14
  }

  it should "return its first along coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_0), Default(12))(
        (List(Position(2, "xyz"), Position(4, "xyz")), (c: Cell[P3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result15
  }

  it should "return its second over coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_1), InMemory())((List(2, 4), (c: Cell[P3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result16
  }

  it should "return its second along coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_1), Default())(
        (List(Position("bar", "xyz"), Position("qux", "xyz")), (c: Cell[P3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result17
  }

  it should "return its third over coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_2), Default(12))(("xyz", (c: Cell[P3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result18
  }

  it should "return its third along coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_2), InMemory())(
        (List(Position("bar", 2), Position("qux", 1)), (c: Cell[P3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result19
  }

  it should "return its first over multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_0), Default())(
        List(
          (List("bar", "qux"), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result20
  }

  it should "return its first along multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_0), Default(12))(
        List(
          (List(Position(2, "xyz"), Position(4, "xyz")), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List(Position(2, "xyz")), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result21
  }

  it should "return its second over multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_1), InMemory())(
        List(
          (List(2, 4), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result22
  }

  it should "return its second along multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_1), Default())(
        List(
          (List(Position("bar", "xyz"), Position("qux", "xyz")), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List(Position("foo", "xyz")), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result23
  }

  it should "return its third over multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Over(_2), Default(12))(
        List(
          ("xyz", (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          ("xyz", (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result24
  }

  it should "return its third along multiple coordinates in 3D" in {
    toU(data3)
      .whichByPosition(Along(_2), InMemory())(
        List(
          (List(Position("foo", 1), Position("qux", 1)), (c: Cell[P3]) => TestMatrixWhich.predicate(c)),
          (List(Position("foo", 2)), (c: Cell[P3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result25
  }

  it should "return empty data - Default" in {
    toU(data3)
      .whichByPosition(Along(_2), Default())(
        List((List(), (c: Cell[P3]) => !TestMatrixWhich.predicate(c)))
      )
      .toList.sorted shouldBe List()
  }
}

