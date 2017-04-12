import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File
import java.util.Arrays
import Array._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions
import scala.io.Source
import scala.util.Try

class KMeansClustering(numClusters: Int, numIterations: Int) extends Serializable {
    def clusterPapers(featureVectors: RDD[Vector]): RDD[Int] = {
        val kmModel = KMeans.train(featureVectors, numClusters, numIterations)

        return kmModel.predict(featureVectors)
    }
}
