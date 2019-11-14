import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object KMeans {

  type Point=(Double,Double)
  var centroids: Array[Point] = Array[Point]()



  def distance (p: Point, z: Point): Double ={

    var euclidean = Math.sqrt ((p._1 - z._1) * (p._1 - z._1) + (p._2 - z._2) * (p._2 - z._2) );
    euclidean
  }

  def main (args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)

    centroids = sc.textFile(args(1)).map( line => { val a = line.split(",")
      (a(0).toDouble,a(1).toDouble)}).collect
    var points=sc.textFile(args(0)).map(line=>{val b=line.split(",")
      (b(0).toDouble,b(1).toDouble)})

    for(i<- 1 to 5){
      val cs = sc.broadcast(centroids)
      centroids = points.map { p => (cs.value.minBy(distance(p,_)), p) }
        .groupByKey().map { case(c,pointf)=>
        var count=0
        var sumX=0.0
        var sumY=0.0

        for(pp <- pointf) {
           count += 1
           sumX+=pp._1
           sumY+=pp._2
        }
        var cen_x=sumX/count
        var cen_y=sumY/count
        (cen_x,cen_y)

      }.collect
    }

centroids.foreach(println)
    }




}
