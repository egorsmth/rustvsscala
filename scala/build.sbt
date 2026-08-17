ThisBuild / scalaVersion := "2.13.15"
ThisBuild / organization := "rustvsp4s"

lazy val root = (project in file("."))
  .settings(
    name := "parquet4s-seaweed-reader",
    Compile / mainClass := Some("rustvsp4s.Main"),
    libraryDependencies ++= Seq(
      "com.github.mjakubowski84" %% "parquet4s-core" % "2.20.0",
      "org.apache.hadoop"        %  "hadoop-client"  % "3.3.6",
      "org.apache.hadoop"        %  "hadoop-aws"     % "3.3.6",
      "org.slf4j"                %  "slf4j-simple"   % "2.0.13"
    ),
    // Fat jar so benchmarks time `java -jar reader.jar`, not sbt booting a JVM.
    assembly / mainClass       := Some("rustvsp4s.Main"),
    assembly / assemblyJarName := "reader.jar",
    assembly / assemblyMergeStrategy := {
      // Keep the s3a FileSystem service registration (concat + dedupe).
      case PathList("META-INF", "services", _*) => MergeStrategy.filterDistinctLines
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case "module-info.class"                  => MergeStrategy.discard
      case x if x.endsWith("/module-info.class") => MergeStrategy.discard
      // Hadoop bundles overlap heavily; first-wins is fine for a benchmark harness.
      case _                                    => MergeStrategy.first
    }
  )
