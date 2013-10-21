import net.thunderklaus.GwtPlugin._

name := "ui"
 
scalaVersion := "2.9.1"
 
seq(webSettings: _*)

//seq(gwtSettings: _*)

//gwtVersion := "2.4.0"

resolvers += "Vaadin add-ons repository" at "http://maven.vaadin.com/vaadin-addons"

resolvers += "typesafe0" at "http://repo.typesafe.com/typesafe/releases"

// basic dependencies
libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor" %  "2.0.3",
  "org.apache.httpcomponents" % "httpclient" % "4.1.2",
  "com.vaadin" % "vaadin" % "6.7.6",
  "org.vaadin.addons" % "scaladin" % "1.0.0",
  "org.eclipse.jetty" % "jetty-webapp" % "8.0.4.v20111024" % "container"
)

libraryDependencies ++= Seq(
	//Add add-ons from the directory here. e.g.
	//"org.vaadin.addons" % "ratingstars" % "1.4"
)



// hack: sbt-gwt-plugin assumes that sources are in src/main/java
//javaSource in Compile <<= (scalaSource in Compile)

//gwtModules := List("com.persist.OStoreUIWidgetset")

// more correct place would be to compile widgetset under the target dir and configure jetty to find it from there 
//gwtTemporaryPath := file(".") / "src" / "main" / "webapp" / "VAADIN" / "widgetsets"