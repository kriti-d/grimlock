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
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.position._

import shapeless.{ ::, HList, HNil }

trait TestMatrixToVector extends TestMatrix {
  val result1 = data2.map { case Cell(Position(f :: s :: HNil), c) =>
      Cell(Position(f.toShortString + TestMatrixToVector.separator + s.toShortString), c)
    }
    .sortBy(_.position)

  val result2 = data3.map { case Cell(Position(f :: s :: t :: HNil), c) =>
     Cell(
       Position(
         f.toShortString + TestMatrixToVector.separator +
         s.toShortString + TestMatrixToVector.separator +
         t.toShortString
       ),
       c
     )
    }
    .sortBy(_.position)
}

object TestMatrixToVector {
  val separator = ":"

  def melt[P <: HList](pos: Position[P]): String = pos.asList.map(_.toShortString).mkString(separator)
}

class TestScalaMatrixToVector extends TestMatrixToVector with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.vectorise" should "return its vector for 2D" in {
    toU(data2)
      .vectorise(TestMatrixToVector.melt)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation vector for 3D" in {
    toU(data3)
      .vectorise(TestMatrixToVector.melt)
      .toList.sortBy(_.position) shouldBe result2
  }
}

class TestScaldingMatrixToVector extends TestMatrixToVector with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.vectorise" should "return its vector for 2D" in {
    toU(data2)
      .vectorise(TestMatrixToVector.melt)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation vector for 3D" in {
    toU(data3)
      .vectorise(TestMatrixToVector.melt)
      .toList.sortBy(_.position) shouldBe result2
  }
}

class TestSparkMatrixToVector extends TestMatrixToVector with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.vectorise" should "return its vector for 2D" in {
    toU(data2)
      .vectorise(TestMatrixToVector.melt)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation vector for 3D" in {
    toU(data3)
      .vectorise(TestMatrixToVector.melt)
      .toList.sortBy(_.position) shouldBe result2
  }
}

