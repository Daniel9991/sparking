import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object Main {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))

    def readCSV(filePath: String): RDD[Array[String]] ={
        val lines = sc.textFile(filePath)
        lines.map(line => line.split(",").map(_.trim))
    }

    def countTokensInCSV(tokens: RDD[Array[String]]): Int={
        val lineSizes = tokens.map(_.length)
        val sum = lineSizes.sum()
        sum.toInt
    }

//    def findKNeighborsForAll(instances: RDD[Instance], k: Int): RDD[Instance]={
//
//        val instancesWithKNeighbors = instances.map(instance => {
//            val kNeighbors = new ArrayBuffer[KNeighbor]
//
//            instances.foreach(otherInstance => {
//
//                if(instance.id != otherInstance.id) {
//
//                    val distance = DistanceFunctions.euclidean(instance.attributes, otherInstance.attributes)
//
//                    if(kNeighbors.length < k){
//                        kNeighbors.addOne(new KNeighbor(otherInstance.id, distance))
//                        kNeighbors.sortWith((n1, n2) => n1.distance < n2.distance)
//                    }
//                    else if(distance < kNeighbors.last.distance){
//                        kNeighbors(kNeighbors.length - 1) = new KNeighbor(otherInstance.id, distance)
//                        kNeighbors.sortWith((n1, n2) => n1.distance < n2.distance)
//                    }
//                }
//            })
//
//            instance.kNeighbors = kNeighbors.toArray
//            instance
//        })
//
//        instancesWithKNeighbors
//    }

    def findKNeighborsForAll(instances: RDD[Instance], k: Int): RDD[Instance]={

        instances.foreach(instance => {
            val otherInstances = instances.filter(otherInstance => otherInstance.id != instance.id)
            val allKNeighbors = instances.map(otherInstance => new KNeighbor(otherInstance.id, DistanceFunctions.euclidean(instance.attributes, otherInstance.attributes)))
            val kNeighbors = allKNeighbors.aggregate(new ArrayBuffer[KNeighbor])(
                (acc, neighbor) => {
                    if(acc.length < k){
                        acc.addOne(neighbor)
                        acc.sortWith((n1, n2) => n1.distance < n2.distance)
                    }
                    else if(neighbor.distance < acc.last.distance){
                        acc(k - 1) = neighbor
                        acc.sortWith((n1, n2) => n1.distance < n2.distance)
                    }
                    else{
                        acc
                    }
                },
                (acc1, acc2) => {
                    acc1.addAll(acc2)
                    acc1.sortWith((n1, n2) => n1.distance < n2.distance)
                    acc1.slice(0, k)
                }
            )

            instance.kNeighbors = kNeighbors.toArray
        })

        instances
    }

    def main(args: Array[String]): Unit ={

        val FILE_PATH = "datasets/idiris.data"
        val k = 3
        val sortNeighbors = (n1: KNeighbor, n2: KNeighbor) => n1.distance < n2.distance

        val data = readCSV(FILE_PATH)
        val instances = data.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            new Instance(index.toString, line.slice(0, 4).map(_.toDouble))
        })

        val fullyMappedInstances = instances.cartesian(instances)
            .filter(ins_tuple => ins_tuple._1.id != ins_tuple._2.id)
            .map(ins_tuple => {
                val (ins1, ins2) = ins_tuple
                (ins1.id, new KNeighbor(ins2.id, DistanceFunctions.euclidean(ins1.attributes, ins2.attributes)))
            })

        val x = fullyMappedInstances.combineByKey(
            _ => new ArrayBuffer[KNeighbor],
            (acc: ArrayBuffer[KNeighbor], neighbor) => {
                if(acc.length < k){
                    acc.addOne(neighbor)
                    acc.sortWith(sortNeighbors)
                }
                else if(neighbor.distance < acc.last.distance){
                    acc(k - 1) = neighbor
                    acc.sortWith(sortNeighbors)
                }
                else{
                    acc
                }
            },
            (acc1: ArrayBuffer[KNeighbor], acc2: ArrayBuffer[KNeighbor]) => {
                acc1.addAll(acc2)
                acc1.sortWith(sortNeighbors).slice(0, k)
            }
        ).mapValues(neighbors => neighbors.toArray)

        val first = x.filter(inst_tuple => inst_tuple._1 == "0").first()
        println(s"Instance with id ${first._1} has neighbors ${first._2.map(neighbor => s"id: ${neighbor.id} - dist: ${neighbor.distance}").mkString("{\n", ",\n", "\n}")}")
    }
}
