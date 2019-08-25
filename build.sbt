name := """data-brewery"""


lazy val commonSettings = Seq(
  organization := "com.dataintoresults",
  version := "1.0.0-M1",
  scalaVersion := "2.12.9",
  maintainer := "contact@dataintoresults.com"
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .aggregate(core, baton)
  .dependsOn(core)


lazy val core = project
  .settings(commonSettings)

lazy val baton = project
  .settings(commonSettings)
  .enablePlugins(JavaAppPackaging, ClasspathJarPlugin)
  .dependsOn(core)
