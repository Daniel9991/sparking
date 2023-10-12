import org.apache.spark._

object Main {
  def main(args: Array[String]): Unit ={

    val FILE_PATH = "datasets/iris.data"

    val conf = new SparkConf().setMaster("local").setAppName("Sparking2")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(FILE_PATH)
    val words = lines.flatMap(words => words.split(","))
    val wordCount = words.count()

    println(s"There are ${wordCount} words in the Iris dataset")
  }
}
