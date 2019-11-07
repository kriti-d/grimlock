// Copyright 2016,2017,2018,2019 Commonwealth Bank of Australia
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

package commbank.grimlock.framework

import java.io.{ File, OutputStreamWriter, PrintWriter }
import java.lang.{ ProcessBuilder, Thread }
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{ Files, Paths }

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source

private[grimlock] object Stream {
  def delegate[K] = (command: String, files: List[String]) => {
    val contents = files.map(f => (new File(f).getName, Files.readAllBytes(Paths.get(f))))

    (key: K, itr: Iterator[String]) => {
      val tmp = Files.createTempDirectory(s"grimlock-${key}-")
      tmp.toFile.deleteOnExit

      contents.foreach { case (file, content) =>
        val path = Paths.get(tmp.toString, file)

        Files.write(path, content)
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxr-x---"))
      }

      val process = new ProcessBuilder(command.split(' ').toList.asJava).directory(tmp.toFile).start

      new Thread() {
        override def run() {
          Source.fromInputStream(process.getErrorStream, "ISO-8859-1").getLines.foreach(System.err.println)
        }
      }.start

      new Thread() {
        override def run() {
          val out = new PrintWriter(new OutputStreamWriter(process.getOutputStream, UTF_8))

          itr.foreach(out.println)
          out.close
        }
      }.start

      val result = Source.fromInputStream(process.getInputStream, "UTF-8").getLines

      new Iterator[String] {
        def next(): String = result.next

        def hasNext: Boolean = {
          if (result.hasNext)
            true
          else {
            val status = process.waitFor

            if (status != 0)
              throw new Exception(s"Subprocess '${command}' exited with status ${status}")

            false
          }
        }
      }
    }
  }
}

