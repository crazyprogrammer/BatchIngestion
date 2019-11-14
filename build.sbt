name := "IngestionProcessingV2"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.1"
val scalatestVersion = "3.0.1"

headerLicense := Some(HeaderLicense.ALv2("2017", "Standard Chartered Bank"))

lazy val commonSettings = Seq(
  coverageEnabled := true,
  coverageReport := true,
  coverageOutputXML := true
)

packAutoSettings
test in assembly := {}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
  "org.apache.hadoop" % "hadoop-client" % "2.7.1" % Provided excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1" % Provided excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.rogach" %% "scallop" % "3.1.1",
  "com.squareup.okhttp" % "okhttp" % "2.7.5",
  ("com.databricks" %% "spark-avro" % "4.0.0") exclude ("org.apache.spark", "spark-core"),
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.6" % "test" withSources() withJavadoc(),
  "com.fortysevendeg" %% "scalacheck-datetime" % "0.2.0" %"test" exclude("org.scalacheck", "scalacheck") withSources() withJavadoc()
)

resolvers ++= Seq(
  "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
  "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Maven Central" at "https://repo1.maven.org/maven2/",
  Resolver.sonatypeRepo("releases")
)

val currentTime = System.currentTimeMillis()

assemblyJarName in assembly := s"${name.value}-${version.value}-$currentTime.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-target:jvm-1.8", "-Ywarn-unused-import")
scalacOptions in (Compile, doc) ++= Seq("-unchecked", "-deprecation", "-diagrams", "-implicits")

fork in Test := true
javaOptions ++= Seq("-Xms2g", "-Xmx3g", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := true
