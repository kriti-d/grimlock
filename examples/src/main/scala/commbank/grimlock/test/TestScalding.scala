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

import commbank.grimlock.scalding.environment._
import commbank.grimlock.scalding.transform.CutRules

import commbank.grimlock.test.TestScaldingReader._

import com.twitter.scalding.{ Args, Job, TypedPsv }
import com.twitter.scalding.TDsl.sourceToTypedPipe

import shapeless.nat._3

object TestScaldingReader {
  def load4TupleDataAddDate(ctx: Context, file: String): Context.U[Cell[_3]] = {
    def hashDate(v: String) = {
      val cal = java.util.Calendar.getInstance()

      cal.setTime((new java.text.SimpleDateFormat("yyyy-MM-dd")).parse("2014-05-14"))
      cal.add(java.util.Calendar.DATE, -(v.hashCode % 21)) // Generate 3 week window prior to date

      DateValue(cal.getTime(), DateCodec())
    }

    (TypedPsv[(String, String, String, String)](file))
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
  }
}

class TestScalding1(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test1(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding2(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test2(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding3(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test3(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding4(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test4(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding5(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test5(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding6(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test6(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding7(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test7(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding8(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test8(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding9(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test9(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding10(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test10(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding11(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test11(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding12(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test12(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding13(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test13(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding14(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test14(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding15(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test15(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding16(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test16(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding17(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test17(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding18(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test18(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding19(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test19(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding20(args : Args) extends Job(args) {
  Shared.test20(Context(), args("path"), "scalding")
}

class TestScalding21(args : Args) extends Job(args) {
  val ctx = Context()
  val path = args("path")

  Shared.test21(ctx, load4TupleDataAddDate(ctx, path + "/someInputfile3.txt"), path, "scalding")
}

class TestScalding22(args : Args) extends Job(args) {
  Shared.test22(Context(), args("path"), "scalding")
}

class TestScalding23(args : Args) extends Job(args) {
  Shared.test23(Context(), args("path"), "scalding")
}

class TestScalding24(args: Args) extends Job(args) {
  Shared.test24(Context(), args("path"), "scalding")
}

class TestScalding25(args: Args) extends Job(args) {
  Shared.test25(Context(), args("path"), "scalding")
}

class TestScalding26(args: Args) extends Job(args) {
  Shared.test26(Context(), args("path"), "scalding")
}

class TestScalding27(args: Args) extends Job(args) {
  Shared.test27(Context(), args("path"), "scalding")
}

class TestScalding28(args: Args) extends Job(args) {
  Shared.test28(Context(), CutRules, "scalding")
}

class TestScalding29(args: Args) extends Job(args) {
  Shared.test29(Context(), "scalding")
}

class TestScalding30(args: Args) extends Job(args) {
  Shared.test30(Context(), args("path"), "scalding")
}

class TestScalding31(args: Args) extends Job(args) {
  Shared.test31(Context(), "scalding")
}

class TestScalding32(args: Args) extends Job(args) {
  Shared.test32(Context(), "scalding")
}

class TestScalding33(args: Args) extends Job(args) {
  Shared.test33(Context(), "scalding")
}

