package org.example
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
/**
 * Hello world!
 *
 */
object App extends App {

  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = new SparkContext(conf)

  println( "Hello World!" )
//  val logFile = "D:\\test.txt" // 应该是你系统上的某些文件
//
//  val logData = sc.textFile(logFile, 2).cache()
//  val numAs = logData.filter(line => line.contains("a")).count()
//  val numBs = logData.filter(line => line.contains("b")).count()
//  println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  val data = Array(1,2,3,4)
  val distData = sc.parallelize(data)
  distData.foreach(x => println(x))
  distData.foreach(println)
  val broadcastVar = sc.broadcast(Array(1,2.3))
  sc.stop()



}
