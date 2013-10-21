
resolvers += Classpaths.typesafeResolver

//addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.1.0-RC1")

resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.4")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.9.2")

