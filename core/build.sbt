
libraryDependencies ++= Seq( 
  "com.typesafe.play" %% "play-json" % "2.7.3",
  "org.playframework.anorm" %% "anorm" % "2.6.4",
  "org.slf4j" % "slf4j-api" % "1.7.28",
  
  // Access to data sources
  "org.scalikejdbc" %% "scalikejdbc" % "2.5.1",
  "org.postgresql" % "postgresql" % "42.2.5",
  "mysql" % "mysql-connector-java" % "8.0.17",
  "com.h2database" % "h2" % "1.4.199",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "6.4.0.jre8",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.2.1",// https://mvnrepository.com/artifact/com.oracle/ojdbc8
  "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0",
  "com.sap.cloud.db.jdbc" % "ngdbc" % "2.4.67",

  
  "commons-io" % "commons-io" % "2.6",

  // Parser
  "com.lihaoyi" %% "fastparse" % "2.1.3",

  // CSV stuff 
  "com.univocity" %  "univocity-parsers" % "2.7.6",

  // JSON stuff
  "com.google.code.gson" % "gson" % "2.8.5",

  "com.google.apis" % "google-api-services-analyticsreporting" % "v4-rev170-1.25.0",
  "com.google.apis" % "google-api-services-sheets" % "v4-rev604-1.25.0",
  "com.google.apis" % "google-api-services-webmasters" % "v3-rev35-1.25.0",
  "com.google.oauth-client" % "google-oauth-client" % "1.30.5",
  "com.google.http-client" % "google-http-client-gson" % "1.34.0",
  
  "fr.janalyse"   %%  "janalyse-ssh" % "0.10.3" % "compile",
  
  "com.typesafe.play" %% "play-mailer" % "7.0.1",
  "org.mindrot" % "jbcrypt" % "0.3m",
  "org.ccil.cowan.tagsoup" % "tagsoup" % "1.2.1",
  "com.github.nscala-time" %% "nscala-time" % "2.18.0",
  "org.apache.commons" % "commons-lang3" % "3.6",
  "org.jsoup" %  "jsoup" % "1.6.1",
  "org.apache.xmlrpc" % "xmlrpc-client" % "3.1.3",
  "org.clapper" %% "grizzled-slf4j" % "1.3.2",
  "org.scalaz" %% "scalaz-core" % "7.2.19",
  "org.scalaz" %% "scalaz-concurrent" % "7.2.19",
  
  "org.scalamock" %% "scalamock" % "4.4.0" % Test,  // https://mvnrepository.com/artifact/com.icegreen/greenmail
  "com.icegreen" % "greenmail" % "1.5.11" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,

  "org.seleniumhq.selenium" % "selenium-server" % "3.141.59",
  "org.seleniumhq.selenium" % "htmlunit-driver" % "2.35.1",
  "com.google.guava" % "guava" % "25.0-jre",
  
  
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.play" %% "play-logback" % "2.7.3",
  
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.0.7",
  "com.typesafe.play" %% "play-ws-standalone-json" % "2.0.7",
  
  "org.apache.poi" % "poi" % "4.1.1",
  "org.apache.poi" % "poi-ooxml" % "4.1.1"
)
