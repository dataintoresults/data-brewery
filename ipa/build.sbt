mainClass in Compile := Some("com.dataintoresults.ipa.Ipa")


libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.7.0",
  "org.scala-lang.modules" %% "scala-xml" % "1.1.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.play" %% "play-logback" % "2.6.11",
  "com.typesafe.play" %% "play-ws" % "2.6.11",
  
)