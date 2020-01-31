// Copyright 2017,2018,2019,2020 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

trait TestStatistics extends TestGrimlock {
  val num1 = List(
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux"), Content(ContinuousSchema[Double](), 12.56))
  )

  val num2 = List(
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 12.56))
  )

  val num3 = List(
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 12.56))
  )

  val num4 = List(
    Cell(Position("foo", 1, "xyz", true), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar", 1, "xyz", false), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", 1, "xyz", true), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz", true), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("foo", 2, "xyz", false), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz", true), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz", false), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 3, "xyz", false), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("bar", 3, "xyz", false), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 4, "xyz", true), Content(ContinuousSchema[Double](), 12.56))
  )
}

