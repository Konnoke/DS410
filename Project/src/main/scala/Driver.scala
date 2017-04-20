import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import scala.io.Source

object Driver {

    // Application Specific Variables
    private final val SPARK_MASTER = "yarn-client"
    private final val APPLICATION_NAME = "Project"
    private final val DATASET_PATH_UBER = "/user/yib5063/uberMerged.csv"

    // HDFS Configuration Files
    private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
    private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")
    final val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
    final val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    def main(args: Array[String]): Unit = {
        // Configure HDFS
        val configuration = new Configuration();
        configuration.addResource(CORE_SITE_CONFIG_PATH);
        configuration.addResource(HDFS_SITE_CONFIG_PATH);

        // Print Usage Information
        System.out.println("\n----------------------------------------------------------------\n")
        System.out.println("Usage: spark-submit [spark options] Project.jar [exhibit]")
        System.out.println(" Exhibit \'kmeans\': KMeans Clustering")
        System.out.println("\n----------------------------------------------------------------\n");

        // Exhibit: KMeans Clustering
        if(args(0) == "kmeans") {

          //load the data
          val rdd = sc.textFile("/user/yib5063/uberMerged.csv")

          //clean the data, cache it in memory for kmeans
          val parsedData = rdd.map{ line => Vectors.dense(line.split(",").slice(2, 4).map(_.toDouble))}.cache()

          //run kmeans
          val iterationCount = 100
          val clusterCount = 20
          val start = System.nanoTime
          //cache data
          val model = KMeans.train(parsedData, clusterCount, iterationCount)
            //[id, lat, lon]
          val end = System.nanoTime
          println("KMeans Run-Time: " + (end - start) / 10e9 + "s")
          val clusterCenters = model.clusterCenters map (_.toArray)
          val cost = model.computeCost(parsedData)
          println("Cost: " + cost)
          model.clusterCenters.foreach(println)

        }

    }
}
