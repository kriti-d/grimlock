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

import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin.uniformAssemblySettings
import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin.uniform

lazy val standardSettings = Defaults.coreDefaultSettings ++ Seq(
  conflictManager := ConflictManager.strict,
  test in assembly := {},
  parallelExecution in Test := false,
  scalacOptions ++= Seq("-Xfatal-warnings", "-deprecation", "-unchecked", "-Xlint", "-feature", "-language:_"),
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
      resolvers ++= Seq(
        "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",                 // *-cdh jars
        "commbank-releases" at "http://commbank.artifactoryonline.com/commbank/ext-releases-local",  // ebenezer jars
        "Concurrent Maven Repo" at "http://conjars.org/repo",                                        // cascading jars
        "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"                                  // scalaz jars
      ),
      libraryDependencies ++= noDerby(
        Seq(
          "org.apache.spark" %% "spark-core" % "2.1.1"
            exclude("com.twitter",                "chill-java")                     // 0.8.0   -> 0.8.4   [scalding]
            exclude("com.twitter",                "chill_2.11")                     // 0.8.0   -> 0.8.4   [scalding]
            exclude("com.google.guava",           "guava")                          // 11.0.1  -> 14.0.1  [scalding]
            ,
          "com.typesafe.play" %% "play-json" % "2.6.0-M1"
            exclude("com.fasterxml.jackson.core", "jackson-annotations") // TODO:   // 2.8.5   ->  2.6.5! [spark]
            exclude("com.fasterxml.jackson.core", "jackson-core")        // TODO:   // 2.8.5   ->  2.6.5! [spark]
            exclude("com.fasterxml.jackson.core", "jackson-databind")    // TODO:   // 2.8.5   ->  2.6.5! [spark]
            ,
          "com.tdunning" %  "t-digest" % "3.2-20160726-OMNIA"
            ,
          "com.chuusai" %% "shapeless" % "2.3.2"
            ,
          "com.twitter" %% "scalding-core" % "0.17.0"
            exclude("org.slf4j",                  "slf4j-api")                      // 1.6.6   -> 1.7.16  [spark]
            ,
          "com.twitter" %% "scrooge-core" % "4.12.0"
            ,
          "au.com.cba.omnia" %% "ebenezer" % "0.23.13-20170710073951-5e6af7a"       // TODO: Can something else be used?
            exclude("org.apache.commons",          "commons-lang3")                 // 3.1     -> 3.5     [spark]
            exclude("org.apache.hadoop",           "hadoop-client")      // TODO:   // 2.5.0   -> 2.2.0!  [spark]
            exclude("org.scala-lang.modules",      "scala-parser-combinators_2.11") // 1.0.2   -> 1.0.4   [spark]
            exclude("org.scala-lang.modules",      "scala-xml_2.11")                // 1.0.2   -> 1.0.4   [spark]
            exclude("com.twitter",                 "algebird-core_2.11")            // 0.12.0  -> 0.13.0  [scalding]
            exclude("com.twitter",                 "bijection-core_2.11")           // 0.9.1   -> 0.9.5   [scalding]
            exclude("com.twitter",                 "bijection-macros_2.11")         // 0.9.1   -> 0.95    [scalding]
            exclude("com.twitter",                 "scalding-core_2.11")            // 0.16.0  -> 0.17.0  [scalding]
            exclude("com.twitter",                 "scrooge-core_2.11")             // 3.20.0  -> 4.12.0  [scrooge]
            ,
          "org.joda" % "joda-convert" % "1.8.2"                                     // Needed by play-json
            ,
          "com.esotericsoftware"      %  "kryo"       % "3.0.3" % "test"            // Needed by Spark unit tests
            ,
          "org.scalatest"             %% "scalatest"  % "3.0.1" % "test"
            exclude("org.scala-lang.modules", "scala-xml_2.11")          // TODO:   // 1.0.5   -> 1.0.4!  [spark]
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
    overrides
).dependsOn(core % "test->test;compile->compile")

lazy val overrides = Seq(
  dependencyOverrides ++= Set(
    "com.twitter"                %% "util-core" % "6.42.0",              // TODO: Needed for interop with io.finch
    "org.apache.thrift"          %  "libthrift" % "0.9.0-cdh5-3",        // TODO:   // 0.7.0   -> 0.9.0   [ebenezer!]
    "com.thoughtworks.paranamer" %  "paranamer" % "2.6"                  // TODO:   // 2.3     -> 2.6     [spark!]
  )
)

def noDerby(deps: Seq[ModuleID]) = deps.map(_ exclude("org.apache.derby", "derby")) // Exclude for Spark REPL

