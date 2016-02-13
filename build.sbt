lazy val commonSettings = Seq(
	organization:= "",
	version := "0.1.0", 
	scalaVersion := "2.11.7",
	crossScalaVersions := Seq("2.11.7","2.10.4") 
)

lazy val root = (project in file(".")).
	settings(commonSettings: _*).
	settings( 
		name := "Spark4DS",
		libraryDependencies ++= {Seq(
			"com.databricks" %% "spark-csv" % "1.3.0",
			"org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
			"org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
			"org.apache.spark" %% "spark-hive" % "1.6.0" % "provided"
		)}
	)
