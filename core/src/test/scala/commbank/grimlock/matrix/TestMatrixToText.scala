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

trait TestMatrixToText extends TestMatrix {
  val result1 = data1.map(_.toString).sorted

  val result2 = data2.map(_.toShortString(false, "|")).sorted

  val result3 = data3.map(_.toShortString(true, "/")).sorted
}

class TestScalaMatrixToText extends TestMatrixToText with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.toText" should "return its strings for 1D" in {
    toU(data1)
      .toText(c => List(c.toString))
      .toList.sorted shouldBe result1
  }

  "A Matrix.toText" should "return its strings for 2D" in {
    toU(data2)
      .toText(Cell.toShortString(false, "|"))
      .toList.sorted shouldBe result2
  }

  "A Matrix.toText" should "return its strings for 3D" in {
    toU(data3)
      .toText(Cell.toShortString(true, "/"))
      .toList.sorted shouldBe result3
  }
}

class TestScaldingMatrixToText extends TestMatrixToText with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.toText" should "return its strings for 1D" in {
    toU(data1)
      .toText(c => List(c.toString))
      .toList.sorted shouldBe result1
  }

  "A Matrix.toText" should "return its strings for 2D" in {
    toU(data2)
      .toText(Cell.toShortString(false, "|"))
      .toList.sorted shouldBe result2
  }

  "A Matrix.toText" should "return its strings for 3D" in {
    toU(data3)
      .toText(Cell.toShortString(true, "/"))
      .toList.sorted shouldBe result3
  }
}

class TestSparkMatrixToText extends TestMatrixToText with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.toText" should "return its strings for 1D" in {
    toU(data1)
      .toText(c => List(c.toString))
      .toList.sorted shouldBe result1
  }

  "A Matrix.toText" should "return its strings for 2D" in {
    toU(data2)
      .toText(Cell.toShortString(false, "|"))
      .toList.sorted shouldBe result2
  }

  "A Matrix.toText" should "return its strings for 3D" in {
    toU(data3)
      .toText(Cell.toShortString(true, "/"))
      .toList.sorted shouldBe result3
  }
}

