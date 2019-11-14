import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer

object Partition
{
  val depth = 6
  type g = (Long,List[Long])
  def getNodes(id: Long, cluster: Long, adjacent: List[Long]) :List[(Long,(Either[g,Long]))]=
  {

      var nodes: List[(Long,(Either[g,Long]))] = List[(Long,(Either[g,Long]))]((id ,Left(cluster,adjacent)))

      if(cluster > -1)
      {
         nodes::: adjacent.map(x => ((x,Right(cluster))))
      }
      else
      {
        return nodes
      }

    }

  def clusterfunc(id : Long, eachp : Iterable[Either[(Long, List[Long]), Long]]) : (Long, Long, List[Long]) =
  {
    var adjacent : List[Long] = Nil
    var cluster : Long = -1

    for(p <- eachp)
    {

      p match
      {
        case Right(c) => {cluster = c}
        case Left((-1,a)) =>  {adjacent = a}
        case Left((c,a)) => {if(c > 0) {return (id,c,a)}}
      }
    }
    return ( id, cluster, adjacent )
  }
  def main(args: Array[ String ])
  {
    val conf = new SparkConf().setAppName("Partition")
    val sc = new SparkContext(conf)

    var i=0
    var pc : Long = -1

    var mygraph = sc.textFile(args(0)).map(line => { val a = line.split(",")
                                                    i=i+1
                                                    var k=0
                                                    var add = new ListBuffer[Long]()

                                                    for(j <- 1 to (a.length-1))
                                                      {
                                                        add+=a(j).toLong
                                                        k=k+1
                                                      }
                                                    var adj= add.toList
                                                    if(i<=5)
                                                    {
                                                      (a(0).toLong,a(0).toLong,adj)
                                                    }
                                                    else
                                                    {
                                                      (a(0).toLong,pc,adj)
                                                    }

                                                 }  )



   for (i <- 1 to depth)
    {
     mygraph = mygraph.flatMap{ t => getNodes(t._1,t._2,t._3) }.groupByKey.map{ t => clusterfunc(t._1,t._2) }
    }

    var graph = mygraph.map{t => (t._2,1)}.groupByKey
    var partition = graph.map(t => { var m = 0
                                      for(v <- t._2)
                                        {
                                          m = m + v
                                        }
                                        (t._1,m)
                                        })


     partition.foreach(t => println(t))
   }

}
