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
import commbank.grimlock.framework.encoding.{ DateCodec, DateValue, DoubleCodec, LongCodec, StringCodec, Value }
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata.{ ContinuousSchema, NominalSchema }
import commbank.grimlock.framework.position.{ Coordinates3, Position }

import commbank.grimlock.spark.environment.Context
import commbank.grimlock.spark.environment.implicits._

import java.util.Date

import org.apache.spark.sql.SparkSession

object TestSparkReader {
  def load4TupleDataAddDate(
    ctx: Context,
    file: String
  ): ctx.U[Cell[Coordinates3[String, String, Date]]] = ctx.session.sparkContext.textFile(file)
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

  implicit def toDate(date: Date): Value[Date] = DateValue(date, DateCodec())

  private def hashDate(v: String): Date = {
    val cal = java.util.Calendar.getInstance()

    cal.setTime((new java.text.SimpleDateFormat("yyyy-MM-dd")).parse("2014-05-14"))
    cal.add(java.util.Calendar.DATE, -(v.hashCode % 21)) // Generate 3 week window prior to date

    cal.getTime()
  }
}

object TestSpark1 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test1(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark2 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test2(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark3 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test3(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark4 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test4(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark5 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test5(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark6 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test6(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark7 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test7(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark8 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test8(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark9 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test9(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark10 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test10(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark11 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test11(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark12 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test12(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark13 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test13(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark14 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test14(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark15 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test15(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark16 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test16(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark17 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test17(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark18 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test18(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark19 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test19(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark20 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test20(ctx, args(1), "spark")
  }
}

object TestSpark21 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    Shared.test21(ctx, TestSparkReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "spark")
  }
}

object TestSpark22 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test22(ctx, args(1), "spark")
  }
}

object TestSpark23 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test23(ctx, args(1), "spark")
  }
}

object TestSpark24 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test24(ctx, args(1), "spark")
  }
}

object TestSpark25 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test25(ctx, args(1), "spark")
  }
}

object TestSpark26 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test26(ctx, args(1), "spark")
  }
}

object TestSpark27 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test27(ctx, args(1), "spark")
  }
}

object TestSpark28 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test28[Context](ctx, "spark")
  }
}

object TestSpark29 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test29(ctx, "spark")
  }
}

object TestSpark30 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test30(ctx, args(1), "spark")
  }
}

object TestSpark31 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test31(ctx, "spark")
  }
}

object TestSpark32 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test32(ctx, "spark")
  }
}

object TestSpark33 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())

    Shared.test33(ctx, "spark", Redistribute(1))
  }
}

object TestSpark34 {
  def main(args: Array[String]) {
    val ctx = Context(SparkSession.builder().master(args(0)).appName("Test Spark").getOrCreate())
    val path = args(1)

    import ctx.session.sqlContext.sparkSession.implicits._

    Shared.test34(ctx, path, "spark")
  }
}

