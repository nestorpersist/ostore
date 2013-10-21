// xsbt-web-plugin
//resolvers += "Web plugin repo" at "http://siasia.github.com/maven2"

//libraryDependencies <+= sbtVersion(v => "com.github.siasia" %% "xsbt-web-plugin" % "0.11.1-0.2.10")

//libraryDependencies <+= sbtVersion(v => "com.github.siasia" %% "xsbt-web-plugin" % (v+"-0.2.11"))

addSbtPlugin("com.earldouglas" % "xsbt-web-plugin" % "0.4.2")

// sbteclipse
resolvers += Classpaths.typesafeResolver

//addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.1.0-RC1")

// GWT compiler
resolvers += "GWT plugin repo" at "http://ripla.github.com/maven"

resolvers += "GWT plugin repo" at "http://thunderklaus.github.com/maven"

addSbtPlugin("net.thunderklaus" % "sbt-gwt-plugin" % "1.1-SNAPSHOT")