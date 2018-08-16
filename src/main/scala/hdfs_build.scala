/* hdfs_build.scala */
import scala.util.Try
import scala.io.Source
import scala.util.parsing.json._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object hdfs_build {
	def main(args: Array[String]){
	// Settings
	val conf = new SparkConf().setAppName("hdfs_build")
	val sc = new SparkContext(conf)
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	sc.setLogLevel("WARN")
	
	// Find the user's home directory 
	val root_dir = System.getProperty("user.home")
	println(root_dir)
	// Find the user's username
	val user_name = System.getProperty("user.name")

	// VERTICES
	// Read raw json.gz file
	val day = java.time.LocalDate.now
	val vertices_raw = sqlContext.read.json("path/to/vertices.json.gz")
	// Map to correct Vertex RDD type
	val vertices = vertices_raw.rdd.map(row=> ((row.getAs[String]("toid").stripPrefix("osgb").toLong),row.getAs[Long]("index")))
	
	println("Vertice RDD created")
	
	// Set type and create RDD
	val verticesRDD: RDD[(VertexId, Long)] = vertices
	
	// // Save to hadoop distributed file system 
	verticesRDD.saveAsObjectFile("path/to/save/vertices")
	println("Vertices RDD saved to HDFS")

	// EDGES
	// Read raw json.gz file
	val edges_raw = sqlContext.read.json("path/to/edges.json.gz")
	// Map to correct edge RDD type
	val edges = edges_raw.rdd.map(row=>(Edge(row.getAs[String]("positiveNode").stripPrefix("osgb").toLong, row.getAs[String]("negativeNode").stripPrefix("osgb").toLong, row.getAs[Double]("length"))))	
	// Set type and create RDD
	val edgesRDD : RDD[Edge[Double]] = edges
	println("Edges RDD created")
	// Save to hadoop distributed file system
	edgesRDD.saveAsObjectFile("path/to/save/edges")
	println("edges RDD saved to HDFS")

	// Build seeds
	val seed_raw = sqlContext.read.json("path/to/seeds.json.gz")
	val seedRDD = seed_raw.rdd.map(row=> ((row.getAs[String]("origin").stripPrefix("osgb").toLong), row.getAs[String]("destination_a").stripPrefix("osgb").toLong, row.getAs[String]("destination_b").stripPrefix("osgb").toLong))
	seed_raw.saveAsObjectFile("path/to/save/seeds.json.gz")
	println("seed RDD built")

	}
}


