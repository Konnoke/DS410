import java.io.PrintWriter
import java.io.File
import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions
import scala.io.Source
import scala.util.Try


object sprint1 {

	// Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "sprint1"

	// HDFS Configuration Files
	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")

	def main(args: Array[String]): Unit = {

		// Configure SparkContext
		val conf = new SparkConf()
			.setMaster(SPARK_MASTER)
			.setAppName(APPLICATION_NAME)
		val sc = new SparkContext(conf)

		// Configure HDFS
		val configuration = new Configuration();
		configuration.addResource(CORE_SITE_CONFIG_PATH);
		configuration.addResource(HDFS_SITE_CONFIG_PATH);

		// Print Usage Information
		System.out.println("\n----------------------------------------------------------------\n")
		System.out.println("Usage: spark-submit [spark options] demo.jar [exhibit]")
		System.out.println(" Exhibit \'hw\': HelloWorld")
		System.out.println("Exhibit \'sprint1': Sprint 1")
		System.out.println("\n----------------------------------------------------------------\n");

		// Exhibit: HelloWorld
		if(args(0) == "hw") {
			System.out.println("Running exhibit: HelloWorld")
			System.out.println("Hello, World!")
		}
		//exhibit:
		if(args(0) == "sprint1"){
			//hdfs dfs -put uberMerged.csv /user/yib5063
			val rawData = sc.textFile("/user/yib5063/uberMerged.csv")
			val lines = rawData.map(line => line.split(","))
			
			
			
			
			/*
			val string_item = item.filter( i => Try(i(1).toInt).isSuccess && Try(i(2).toInt).isSuccess)
			val int_item = string_item.map( cs => (cs(1).toInt, cs(2).toInt)).filter( cs => cs._1 != cs._2)
			//*/
			
			
			//val writer = new PrintWriter(new File("sprint1output.txt"))
			//results.foreach(x => writer.write(x._1 + "\t" + x._2._1 + "\t" + x._2._2 + "\n"))
			//writer.close()

		}

	}
}
