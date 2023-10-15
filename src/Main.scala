import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark._
import org.apache.spark.rdd.RDD

import java.io.StringWriter
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

    def sortNeighbors(n1: KNeighbor, n2: KNeighbor): Boolean ={
        n1.distance < n2.distance
    }

    def main(args: Array[String]): Unit ={

        val FILE_PATH = "datasets/idiris.data"
        val k = 3

        val data = readCSV(FILE_PATH)
        val instances = data.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            new Instance(index.toString, line.slice(0, 4).map(_.toDouble))
        })

//        Inefficient
        val fullyMappedInstances = instances.cartesian(instances)
            .filter(ins_tuple => ins_tuple._1.id != ins_tuple._2.id)
            .map(ins_tuple => {
                val (ins1, ins2) = ins_tuple
                (ins1.id, new KNeighbor(ins2.id, DistanceFunctions.euclidean(ins1.attributes, ins2.attributes)))
            })

        val groupedCombinations = fullyMappedInstances.groupByKey()
        val x = groupedCombinations.map(tuple => {
            val (instanceId, neighbors) = tuple
            (instanceId, neighbors.toArray.sortWith((n1, n2) => n1.distance < n2.distance).slice(0, k))
        })

        val neighborReferences = x.flatMap(tuple => {
            val (instanceId, neighbors) = tuple
            neighbors.map(neighbor => {
                if(neighbor.id == "0") println("El vecino era 0")
                (neighbor.id, instanceId)}
            )
        })

        val y = neighborReferences.groupByKey()
            .mapValues(rNeighbors => rNeighbors.map(
                    rNeighbor => new Neighbor(rNeighbor)
            ).toArray)

    }
}
