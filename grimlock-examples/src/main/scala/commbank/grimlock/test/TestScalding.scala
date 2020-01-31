// Copyright 2014,2015,2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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

import com.twitter.scalding.{ Args, Job, TypedPsv }
import com.twitter.scalding.TDsl.sourceToTypedPipe

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.{ DateCodec, DateValue, DoubleCodec, LongCodec, StringCodec, Value }
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata.{ ContinuousSchema, NominalSchema }
import commbank.grimlock.framework.position.{ Coordinates3, Position }

import commbank.grimlock.scalding.Persist
import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.environment.implicits._

import java.util.Date

object TestScaldingReader {
  def load4TupleDataAddDate(
    ctx: Context,
    file: String
  ): ctx.U[Cell[Coordinates3[String, String, Date]]] = TypedPsv[(String, String, String, String)](file)
    .flatMap { case (i, f, e, v) =>
      val content = e match {
        case "string" => StringCodec.decode(v).map(c => Content(NominalSchema[String](), c))
        case _ => scala.util.Try(v.toLong).toOption match {
          case Some(_) => LongCodec.decode(v).map(c => Content(ContinuousSchema[Long](), c))
          case None => DoubleCodec.decode(v).map(c => Content(ContinuousSchema[Double](), c))
        }
      }

      content.map(c => Cell(Position(i, f, hashDate(v)), c))
    }

  implicit def toDate(date: Date): Value[Date] = DateValue(date, DateCodec())

  private def hashDate(v: String): Date = {
    val cal = java.util.Calendar.getInstance()

    cal.setTime((new java.text.SimpleDateFormat("yyyy-MM-dd")).parse("2014-05-14"))
    cal.add(java.util.Calendar.DATE, -(v.hashCode % 21)) // Generate 3 week window prior to date

    cal.getTime()
  }
}

class TestScalding1(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  import ctx.encoder

  Shared.test1(
    ctx,
    TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"),
    path,
    "scalding",
    Persist.textLoader
  )
}

class TestScalding2(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  import ctx.encoder

  Shared.test2(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding3(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  import ctx.encoder

  Shared.test3(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding4(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test4(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding5(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test5(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding6(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test6(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding7(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test7(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding8(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  import ctx.encoder

  Shared.test8(
    ctx,
    TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"),
    path,
    "scalding",
    Persist.textLoader
  )
}

class TestScalding9(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test9(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding10(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test10(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding11(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test11(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding12(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test12(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding13(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test13(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding14(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test14(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding15(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test15(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding16(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test16(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding17(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test17(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding18(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test18(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding19(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test19(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding20(args : Args) extends Job(args) {
  val ctx = Context()

  import ctx.encoder

  Shared.test20(ctx, args("path"), "scalding", Persist.textLoader)
}

class TestScalding21(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test21(ctx, TestScaldingReader.load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding22(args : Args) extends Job(args) {
  val ctx = Context()

  import ctx.encoder

  Shared.test22(ctx, args("path"), "scalding", Persist.textLoader)
}

class TestScalding23(args : Args) extends Job(args) {
  val ctx = Context()

  import ctx.encoder

  Shared.test23(ctx, args("path"), "scalding", Persist.textLoader)
}

class TestScalding24(args: Args) extends Job(args) {
  val ctx = Context()

  import ctx.encoder

  Shared.test24(ctx, args("path"), "scalding", Persist.textLoader)
}

class TestScalding25(args: Args) extends Job(args) {
  val ctx = Context()

  import ctx.encoder

  Shared.test25(ctx, args("path"), "scalding", Persist.textLoader)
}

class TestScalding26(args: Args) extends Job(args) {
  val ctx = Context()

  import ctx.encoder

  Shared.test26(ctx, args("path"), "scalding", Persist.textLoader)
}

class TestScalding27(args: Args) extends Job(args) {
  val ctx = Context()

  import ctx.encoder

  Shared.test27(ctx, args("path"), "scalding", Persist.textLoader)
}

class TestScalding28(args: Args) extends Job(args) {
  val ctx = Context()

  Shared.test28[Context](ctx, "scalding")
}

class TestScalding29(args: Args) extends Job(args) {
  val ctx = Context()

  Shared.test29(ctx, "scalding")
}

class TestScalding30(args: Args) extends Job(args) {
  val ctx = Context()

  import ctx.encoder

  Shared.test30(ctx, args("path"), "scalding", Persist.textLoader)
}

class TestScalding31(args: Args) extends Job(args) {
  val ctx = Context()

  Shared.test31(ctx, "scalding")
}

class TestScalding32(args: Args) extends Job(args) {
  val ctx = Context()

  Shared.test32(ctx, "scalding")
}

class TestScalding33(args: Args) extends Job(args) {
  val ctx = Context()

  import ctx.encoder

  Shared.test33(ctx, "scalding", Redistribute(1))
}

class TestScalding34(args: Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  import com.twitter.scalding.parquet.tuple.macros.Macros._
  import ctx.encoder

  Shared.test34(ctx, path, "scalding", Persist.typedParquetLoader)
}

