import org.apache.spark
import org.apache.spark._
import org.apache.spark.rdd.RDD

object Main {

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))

  def main(args: Array[String]): Unit ={

    val FILE_PATH = "datasets/iris.data"

    val data = readCSV(FILE_PATH)
    val lineSizes = data.map(_.length)
    val sum = lineSizes.sum()

    println(s"There are ${sum} words in the Iris dataset")
  }

  def readCSV(filePath: String): RDD[Array[String]] ={

    val lines = sc.textFile(filePath)
    lines.map(line => line.split(",").map(_.trim))
  }
}
