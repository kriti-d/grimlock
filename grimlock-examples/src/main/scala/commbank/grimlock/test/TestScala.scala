// Copyright 2019,2020 Commonwealth Bank of Australia
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

import commbank.grimlock.scala.Persist
import commbank.grimlock.scala.environment.Context
import commbank.grimlock.scala.environment.implicits._

import java.util.Date

import org.apache.avro.generic.GenericRecord

import scala.io.Source

object TestScalaReader {
  def load4TupleDataAddDate(
    ctx: Context,
    file: String
  ): ctx.U[Cell[Coordinates3[String, String, Date]]] = Source.fromFile(file).getLines.toList
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

object TestScala1 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    import ctx.encoder

    Shared.test1(
      ctx,
      TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"),
      path,
      "scala",
      Persist.textLoader
    )
  }
}

object TestScala2 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    import ctx.encoder

    Shared.test2(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala3 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    import ctx.encoder

    Shared.test3(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala4 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test4(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala5 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test5(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala6 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test6(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala7 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test7(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala8 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    import ctx.encoder

    Shared.test8(
      ctx,
      TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"),
      path,
      "scala",
      Persist.textLoader
    )
  }
}

object TestScala9 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test9(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala10 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test10(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala11 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test11(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala12 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test12(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala13 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test13(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala14 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test14(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala15 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test15(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala16 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test16(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala17 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test17(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala18 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test18(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala19 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test19(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala20 {
  def main(args: Array[String]) {
    val ctx = Context()

    import ctx.encoder

    Shared.test20(ctx, args(0), "scala", Persist.textLoader)
  }
}

object TestScala21 {
  def main(args: Array[String]) {
    val ctx = Context()
    val path = args(0)

    Shared.test21(ctx, TestScalaReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scala")
  }
}

object TestScala22 {
  def main(args: Array[String]) {
    val ctx = Context()

    import ctx.encoder

    Shared.test22(ctx, args(0), "scala", Persist.textLoader)
  }
}

object TestScala23 {
  def main(args: Array[String]) {
    val ctx = Context()

    import ctx.encoder

    Shared.test23(ctx, args(0), "scala", Persist.textLoader)
  }
}

object TestScala24 {
  def main(args: Array[String]) {
    val ctx = Context()

    import ctx.encoder

    Shared.test24(ctx, args(0), "scala", Persist.textLoader)
  }
}

object TestScala25 {
  def main(args: Array[String]) {
    val ctx = Context()

    import ctx.encoder

    Shared.test25(ctx, args(0), "scala", Persist.textLoader)
  }
}

object TestScala26 {
  def main(args: Array[String]) {
    val ctx = Context()

    import ctx.encoder

    Shared.test26(ctx, args(0), "scala", Persist.textLoader)
  }
}

object TestScala27 {
  def main(args: Array[String]) {
    val ctx = Context()

    import ctx.encoder

    Shared.test27(ctx, args(0), "scala", Persist.textLoader)
  }
}

object TestScala28 {
  def main(args: Array[String]) {
    val ctx = Context()

    Shared.test28[Context](ctx, "scala")
  }
}

object TestScala29 {
  def main(args: Array[String]) {
    val ctx = Context()

    Shared.test29(ctx, "scala")
  }
}

object TestScala30 {
  def main(args: Array[String]) {
    val ctx = Context()

    import ctx.encoder

    Shared.test30(ctx, args(0), "scala", Persist.textLoader)
  }
}

object TestScala31 {
  def main(args: Array[String]) {
    val ctx = Context()

    Shared.test31(ctx, "scala")
  }
}

object TestScala32 {
  def main(args: Array[String]) {
    val ctx = Context()

    Shared.test32(ctx, "scala")
  }
}

object TestScala33 {
  def main(args: Array[String]) {
    val ctx = Context()

    import ctx.encoder

    Shared.test33(ctx, "scala", Default())
  }
}

object TestScala34 {
  def main(args: Array[String]) {
    val ctx = Context()

    import ctx.encoder

    implicit def genericRecordToParquetSample(record: GenericRecord): Shared.ParquetSample = Shared.ParquetSample(
      Option(record.get("a")).map(_.toString),
      record.get("b").asInstanceOf[Int],
      record.get("c").asInstanceOf[Boolean]
    )

    Shared.test34(ctx, args(0), "scala", Persist.parquetLoader)
  }
}

