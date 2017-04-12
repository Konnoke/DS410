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
import org.apache.spark.sql.SQLContext


object Driver {

	// Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "Project"

	// HDFS Configuration Files
	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")
	final val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
	final val sc = new SparkContext(conf)
	sc.setLogLevel("WARN")

	def main(args: Array[String]): Unit = {

		// Configure SparkContext
		val conf = new SparkConf()
		.setMaster(SPARK_MASTER)
		.setAppName(APPLICATION_NAME)
		//val sc = new SparkContext(conf)

		// Configure HDFS
		val configuration = new Configuration();
		configuration.addResource(CORE_SITE_CONFIG_PATH);
		configuration.addResource(HDFS_SITE_CONFIG_PATH);

		// Print Usage Information
		System.out.println("\n----------------------------------------------------------------\n")
		System.out.println("Exhibit \'sprint1': Sprint 1")
		System.out.println("spark-submit --master yarn-client --driver memory 12g --executor-memory 12g --num-executors 3 --executor-cores 8 project_2.10-1.0.jar sprint1")
		System.out.println("\n----------------------------------------------------------------\n");

		//exhibit:
		if(args(0) == "sprint1"){
			//hdfs dfs -put uberMerged.csv /user/yib5063
			val rawData = sc.textFile("/user/yib5063/uberMerged.csv")
			val lines = rawData.map(line => line.split(","))

			val sqlContext = new SQLContext(sc) //careful, prob gonna have to create sc in spark-submit driver
			val df = sqlContext.read
			.format("com.databricks.spark.csv")
			.option("header", "true") //use first line of all files as header
			.option("inferSchema", "true") //infer data types automatically
			.load("/user/yib5063/uberMerged.csv")

			//df.show()

			//subset the data to get the coords
			val coords = df.drop("Base")
			coords.show()
			//val coords_rdd: RDD[Row] = coords.rdd



		}

	}
}
