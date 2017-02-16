package lab4

import java.io.PrintWriter
import java.io.File
import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions
import scala.io.Source
import scala.util.Try


object Lab4 {

	// Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "lab4"

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
		System.out.println(" Exhibit \'tweet\': Top Tweeters")
		System.out.println("\n----------------------------------------------------------------\n");

		// Exhibit: HelloWorld
		if(args(0) == "hw") {
			System.out.println("Running exhibit: HelloWorld")
			System.out.println("Hello, World!")
		}

		if(args(0) == "cc"){
			val lines = sc.textFile("hdfs:/ds410/lab4/CSNNetwork.csv")
			val item  = lines.map(line => line.split(","))
			val string_item = item.filter( i => Try(i(1).toInt).isSuccess && Try(i(2).toInt).isSuccess)

			val int_item = string_item.map( cs => (cs(1).toInt, cs(2).toInt)).filter( cs => cs._1 != cs._2)
			val edge_increase = int_item.map(cs => (if (cs._1 < cs._2) (cs._1, cs._2); else (cs._2, cs._1))).distinct()
			val edge_decrease = int_item.map(cs => (if (cs._1 < cs._2) (cs._2, cs._1); else (cs._1, cs._2))).distinct()
			val two_edge = edge_increase.join(edge_decrease).map( cs => (cs._2, cs._1))
			// ((larger, smaller), middle)
			val extended_edge_decrease = edge_decrease.map(cs => (cs, 1))
			val triangle = extended_edge_decrease.join(two_edge)
			val num_triangle = triangle.count()

			val nodes = string_item.map(cs => cs(1) +  "," + cs(2)).flatMap(_.split(",")).distinct()
			val num_nodes = nodes.count()

			val result = num_triangle / (num_nodes * (num_nodes - 1) * (num_nodes - 2) / 6.0).toDouble

			print(result, num_nodes, num_triangle)



			val writer = new PrintWriter(new File("lab4output.txt"))
			result.foreach(x => writer.write(x._1 + "\t" + x._2 + "\n"))
			writer.close()

		}

	}
}
