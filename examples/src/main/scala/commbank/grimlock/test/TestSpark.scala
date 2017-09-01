// Copyright 2014,2015,2016,2017 Commonwealth Bank of Australia
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

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.{ DateCodec, DateValue, DoubleCodec, LongCodec, StringCodec }
import commbank.grimlock.framework.metadata.{ ContinuousSchema, NominalSchema }
import commbank.grimlock.framework.position.Position

import commbank.grimlock.spark.environment._
import commbank.grimlock.spark.transform.CutRules

import commbank.grimlock.test.TestSparkReader._

import org.apache.spark.{ SparkContext, SparkConf }

import shapeless.nat._3

object TestSparkReader {
  def load4TupleDataAddDate(ctx: Context, file: String): ctx.U[Cell[_3]] = {
    def hashDate(v: String) = {
      val cal = java.util.Calendar.getInstance()

      cal.setTime((new java.text.SimpleDateFormat("yyyy-MM-dd")).parse("2014-05-14"))
      cal.add(java.util.Calendar.DATE, -(v.hashCode % 21)) // Generate 3 week window prior to date

      DateValue(cal.getTime(), DateCodec())
    }

    ctx.spark.textFile(file)
      .flatMap {
        _.trim.split(java.util.regex.Pattern.quote("|"), 4) match {
          case Array(i, f, e, v) =>
            val content = e match {
              case "string" => StringCodec.decode(v).map(c => Content(NominalSchema[String](), c))
              case _ => scala.util.Try(v.toLong).toOption match {
                case Some(_) => LongCodec.decode(v).map(c => Content(ContinuousSchema[Long](), c))
                case None => DoubleCodec.decode(v).map(c => Content(ContinuousSchema[Double](), c))
              }
            }

            content.map(c => Cell(Position(i, f, hashDate(v)), c))
          case _ => None
        }
      }
  }
}

object TestSpark1 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test1(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark2 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test2(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark3 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test3(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark4 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test4(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark5 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test5(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark6 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test6(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark7 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test7(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark8 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test8(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark9 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test9(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark10 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test10(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark11 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test11(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark12 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test12(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark13 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test13(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark14 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test14(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark15 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test15(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark16 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test16(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark17 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test17(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark18 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test18(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark19 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test19(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark20 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test20(ctx, args(1), "spark")
  }
}

object TestSpark21 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val path = args(1)

    Shared.test21(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark22 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test22(ctx, args(1), "spark")
  }
}

object TestSpark23 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test23(ctx, args(1), "spark")
  }
}

object TestSpark24 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test24(ctx, args(1), "spark")
  }
}

object TestSpark25 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test25(ctx, args(1), "spark")
  }
}

object TestSpark26 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test26(ctx, args(1), "spark")
  }
}

object TestSpark27 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test27(ctx, args(1), "spark")
  }
}

object TestSpark28 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test28[Context](ctx, CutRules, "spark")
  }
}

object TestSpark29 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test29(ctx, "spark")
  }
}

object TestSpark30 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test30(ctx, args(1), "spark")
  }
}

object TestSpark31 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test31(ctx, "spark")
  }
}

object TestSpark32 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test32(ctx, "spark")
  }
}

object TestSpark33 {
  def main(args: Array[String]) {
    val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))

    Shared.test33(ctx, "spark")
  }
}

