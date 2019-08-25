name := """data-brewery"""


lazy val commonSettings = Seq(
  organization := "com.dataintoresults",
  version := "1.0.0-M2",
  scalaVersion := "2.12.9",
  maintainer := "contact@dataintoresults.com"
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .aggregate(core, ipa)
  .dependsOn(core)


lazy val core = project
  .settings(commonSettings)

lazy val ipa = project
  .settings(commonSettings)
  .enablePlugins(JavaAppPackaging, ClasspathJarPlugin)
  .dependsOn(core)
