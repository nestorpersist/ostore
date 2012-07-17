name := "ostore"

version := "0.5"

scalaVersion := "2.9.1"

resolvers += "typesafe0" at "http://repo.typesafe.com/typesafe/releases"

libraryDependencies ++=Seq(
        "org.scalatest" %% "scalatest" % "1.7.2",
        "junit" % "junit" % "4.10" % "test",
        "com.typesafe.akka" % "akka-actor" %  "2.0",
        "com.typesafe.akka" % "akka-remote" % "2.0"
)

parallelExecution in Test := false
