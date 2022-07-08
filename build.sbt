scalacOptions ++= Seq(
    "-encoding", "utf8", // Option and arguments on same line
    "-Xfatal-warnings",  // New lines for each options
    "-deprecation",
    "-unchecked",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-language:existentials"
)

ThisBuild / version := "0.1.1-dev"
ThisBuild / scalaVersion := "2.13.8"

val root = (project in file("."))
  .settings(
    name := "data-copy-tool",
    assembly / mainClass := Some("dct.DataCopyTool"),
    assembly / logLevel := Level.Info,
    libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.19"
      exclude("com.typesafe.akka", "akka-protobuf-v3_2.13"),
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "3.0.4"
      exclude("com.typesafe.akka", "akka-protobuf-v3_2.13"),
    libraryDependencies += "com.typesafe.slick" %% "slick-hikaricp" % "3.3.3",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.3.4",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
      exclude("org.apache.zookeeper", "zookeeper")
      exclude("commons-logging", "commons-logging")
      excludeAll ExclusionRule(organization = "org.glassfish.jersey.containers")
      excludeAll ExclusionRule(organization = "org.glassfish.jersey.core")
      excludeAll ExclusionRule(organization = "org.glassfish.jersey.inject")
      exclude("org.spark-project.spark", "unused"),
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
      exclude("org.apache.zookeeper", "zookeeper")
      exclude("commons-logging", "commons-logging")
      excludeAll ExclusionRule(organization = "org.glassfish.jersey.containers")
      excludeAll ExclusionRule(organization = "org.glassfish.jersey.core")
      excludeAll ExclusionRule(organization = "org.glassfish.jersey.inject")
      exclude("org.spark-project.spark", "unused"),
    libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.12" % "test",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.12" % "test",
    libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"
  )

Compile / resourceDirectory := baseDirectory.value / "src" / "main" / "resources"
Compile / unmanagedClasspath += baseDirectory.value / "src" / "main" / "resources"

/**
 * shell script to be prepend to uber jar.
 */
import sbtassembly.AssemblyPlugin.defaultShellScript
ThisBuild / assemblyPrependShellScript := Some(defaultShellScript)

/**
 * Removing unnecessary files.
 */
ThisBuild / assemblyMergeStrategy  := {
    case PathList("module-info.class") => MergeStrategy.discard
    case PathList("git.properties") => MergeStrategy.discard
    case x if x.endsWith("/module-info.class") => MergeStrategy.discard
    case x if x.endsWith("/git.properties") => MergeStrategy.discard
    case x =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(x)
}