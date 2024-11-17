ThisBuild / version := "0.1.0-SNAPSHOT"

//lazy val scala2Version = "2.13.15"
lazy val scala3Version = "3.1.0"

ThisBuild / scalaVersion := scala3Version

lazy val sparkVersion = "3.5.1"
val targetJDK = "17"

ThisBuild / javacOptions ++= Seq("-source", targetJDK, "-target", targetJDK)
ThisBuild / scalacOptions ++= Seq("-unchecked", "-deprecation", "-Ytasty-reader")

lazy val common = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided"
  ).map(_.cross(CrossVersion.for3Use2_13)),
  assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false),
  Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated,
  organization := "stb"
)

lazy val coreVersion = "0.1.0"
lazy val coreAppName = "core"
lazy val core = (project in file(appDir(coreAppName)))
  .settings(
    name := coreAppName,
    version := coreVersion,
    common,
    //scalaVersion := scala3Version,
  )

lazy val otherVersion = "0.1.0"
lazy val otherAppName = "other"
lazy val other = (project in file(appDir(otherAppName)))
  .dependsOn(core)
  .settings(
    name := otherAppName,
    version := otherVersion,
    common,
    //scalacOptions += "-Ytasty-reader"
  )

lazy val root = (project in file("."))
  .aggregate(core)
  .settings(
    name := "spark-with-scala3",
    common,
  )

def appDir(appName: String) = s"apps/$appName"