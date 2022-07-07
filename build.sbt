ThisBuild / version := "0.1.1-dev"
ThisBuild / scalaVersion := "2.13.8"

val root = (project in file("."))
  .settings(
    name := "data-copy-tool",
    assembly / mainClass := Some("dct.DataCopyTool"),
    libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.19",
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "3.0.4",
    libraryDependencies += "com.typesafe.slick" %% "slick-hikaricp" % "3.3.3",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.3.4",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided",
    libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.12",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.12" % "test",
    libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"
  )

Compile / resourceDirectory := baseDirectory.value / "src" / "main" / "resources"
Compile / unmanagedClasspath += baseDirectory.value / "src" / "main" / "resources"
