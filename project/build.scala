// Copyright 2015,2016,2017 Commonwealth Bank of Australia
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

import sbt._
import sbt.Keys._
import sbtassembly.AssemblyKeys.assembly

import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin.uniformAssemblySettings
import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin.uniform
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin.{
  depend,
  noHadoop,
  uniformPublicDependencySettings,
  strictDependencySettings
}
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin.uniformThriftSettings

object build extends Build {

  lazy val standardSettings = Defaults.coreDefaultSettings ++
    uniformPublicDependencySettings ++
    strictDependencySettings ++
    Seq(
      test in assembly := {},
      parallelExecution in Test := false,
      scalacOptions += "-Xfatal-warnings",
      scalacOptions in (Compile, console) ~= (_.filterNot(Set("-Xfatal-warnings"))),
      scalacOptions in (Compile, doc) ~= (_.filterNot(Set("-Xfatal-warnings"))),
      scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value
    )

  lazy val all = Project(
    id = "all",
    base = file("."),
    settings = standardSettings ++
      uniform.project("grimlock-all", "commbank.grimlock.all") ++
      uniform.ghsettings ++
      Seq(assembly := file(""), publishArtifact := false),
    aggregate = Seq(core, examples)
  )

  lazy val core = Project(
    id = "core",
    base = file("core"),
    settings = standardSettings ++
      uniform.project("grimlock-core", "commbank.grimlock") ++
      uniformAssemblySettings ++
      uniform.docSettings("https://github.com/CommBank/grimlock") ++
      Seq(
        libraryDependencies ++= noDerby(
          depend.hadoopClasspath ++
          depend.scalding() ++
          depend.parquet() ++
          depend.shapeless("2.3.0") ++
          depend.omnia("ebenezer", "0.23.6-20170119005115-2ac29d0") ++
          Seq(
            noHadoop("org.apache.spark" %% "spark-core" % "2.1.0")
              exclude("com.twitter", "chill-java")
              exclude("com.twitter", "chill_2.11"),
            "com.typesafe.play"         %% "play-json"  % "2.3.9"
              exclude("com.fasterxml.jackson.core", "jackson-annotations")
              exclude("com.fasterxml.jackson.core", "jackson-core")
              exclude("com.fasterxml.jackson.core", "jackson-databind"),
            "com.tdunning"              %  "t-digest"   % "3.2-20160726-OMNIA",
            "com.esotericsoftware"      %  "kryo"       % "3.0.3" % "test", // needed by unit tests for Spark 2.1.0
            "org.scalatest"             %% "scalatest"  % "2.2.6" % "test"
          )
        )
      ) ++
      overrides
   )

  lazy val examples = Project(
    id = "examples",
    base = file("examples"),
    settings = standardSettings ++
      uniform.project("grimlock-examples", "commbank.grimlock.examples") ++
      uniformAssemblySettings ++
      Seq(libraryDependencies ++= depend.hadoopClasspath) ++
      overrides
  ).dependsOn(core % "test->test;compile->compile")

  lazy val overrides = Seq(
    dependencyOverrides ++= Set(
      "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
      "org.scala-lang.modules" %% "scala-xml"                % "1.0.4",
      "org.apache.commons"     %  "commons-lang3"            % "3.3.2"
    )
  )

  def noDerby(deps: Seq[ModuleID]) = deps.map(_ exclude("org.apache.derby", "derby")) // Exclude for Spark 2.1.0 REPL
}

