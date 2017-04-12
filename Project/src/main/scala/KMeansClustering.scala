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
    def clusterLocation(featureVectors: RDD[Vector]): RDD[Int] = {
        val kmModel = KMeans.train(featureVectors, numClusters, numIterations)

        return kmModel.predict(featureVectors)
    }

    def Distance(a:Array[Double], b:List[Double]) : Double = {
	      assert(a.length == b.length, "Distance(): features dim does not match.")
	      var dist = 0.0
	      for (i <- 0 to a.length-1) {
	          dist = dist + math.pow(a(i) - b(i), 2)
	      }
	      return math.sqrt(dist)
	  }






}
