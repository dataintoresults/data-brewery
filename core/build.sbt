
libraryDependencies ++= Seq( 
  "com.typesafe.play" %% "play-json" % "2.6.0",
  "com.typesafe.play" %% "anorm" % "2.5.3",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  
  // Access to data sources
  "org.scalikejdbc" %% "scalikejdbc" % "2.5.1",
  "org.postgresql" % "postgresql" % "42.2.5",
  "mysql" % "mysql-connector-java" % "5.1.40",
  "com.h2database" % "h2" % "1.4.197",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "6.4.0.jre8",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.2.1",
  
  "commons-io" % "commons-io" % "2.6",

  // CSV stuff 
  "com.univocity" %  "univocity-parsers" % "2.7.6",

  // JSON stuff
  "com.google.code.gson" % "gson" % "2.8.5",

  "com.google.apis" % "google-api-services-analyticsreporting" % "v4-rev109-1.22.0",  
  "com.google.apis" % "google-api-services-sheets" % "v4-rev468-1.22.0",
  "com.google.oauth-client" % "google-oauth-client" % "1.22.0",
  "com.google.oauth-client" % "google-oauth-client-java6" % "1.22.0",
  "com.google.oauth-client" % "google-oauth-client-jetty" % "1.22.0",
  "com.google.http-client" % "google-http-client-gson" % "1.19.0",

  "com.google.apis" % "google-api-services-webmasters" % "v3-rev34-1.25.0",
  
  "fr.janalyse"   %%  "janalyse-ssh" % "0.10.3" % "compile",
  
  "com.typesafe.play" %% "play-mailer" % "6.0.1",
  "org.mindrot" % "jbcrypt" % "0.3m",
  "org.ccil.cowan.tagsoup" % "tagsoup" % "1.2.1",
  "com.github.nscala-time" %% "nscala-time" % "2.18.0",
  "org.apache.commons" % "commons-lang3" % "3.6",
  "org.apache.poi" % "poi" % "3.15",
  "org.jsoup" %  "jsoup" % "1.6.1",
  "org.apache.xmlrpc" % "xmlrpc-client" % "3.1.3",
  "org.clapper" %% "grizzled-slf4j" % "1.3.2",
  "org.scalaz" %% "scalaz-core" % "7.2.19",
  "org.scalaz" %% "scalaz-concurrent" % "7.2.19",
  
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,

  "org.seleniumhq.selenium" % "selenium-server" % "3.141.59",
  "org.seleniumhq.selenium" % "htmlunit-driver" % "2.35.1",
  "com.google.guava" % "guava" % "25.0-jre",
  
  
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.play" %% "play-logback" % "2.6.11",
  
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.0.6",
  "com.typesafe.play" %% "play-ws-standalone-json" % "2.0.6"


)
