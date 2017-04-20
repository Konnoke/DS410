package uber

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
import scala.io.Source

object Demo {

    // Application Specific Variables
    private final val SPARK_MASTER = "yarn-client"
    private final val APPLICATION_NAME = "uber"
    private final val DATASET_PATH_UBER = "/user/vpt5014/uberMerged.csv"

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
        System.out.println("Usage: spark-submit [spark options] uber.jar [exhibit]")
        System.out.println(" Exhibit \'kmeans\': KMeans Clustering")
        System.out.println("\n----------------------------------------------------------------\n");

        // Exhibit: KMeans Clustering
        if(args(0) == "kmeans") {
            val sqlContext = new SQLContext(sc)
            val df = sqlContext.read
                .format("com.databricks.spark.csv")
                .option("header", "true") //use first line of all files as header
                .option("inferSchema", "true") //infer data types automatically
                .load(DATASET_PATH_UBER)
            val coords = df.drop("Date.Time", "Base").rdd
            //[id, lat, lon]

            val start = System.nanoTime
            val clustersOfLocations = new KMeansClustering(3, 100).//clusterPapers(featureVectors)

            val end = System.nanoTime

            println((end - start) / 10e9 + "s")
        }
    }
}
