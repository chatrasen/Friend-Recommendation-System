import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
sc.setLogLevel("ERROR")

val test=GraphLoader.edgeListFile(sc,"facebook/0.edges")

val edge_file = sc.textFile("facebook/0.edges")

val eRDD: RDD[Edge[Int]] = edge_file.map(line => {val x = line.split("\\s+")
Edge(x(0).toLong, x(1).toLong, 1); })

val edge_Set = test.edges.take(test.edges.count.toInt).toSet

println("\n\n***\tNo of input graph vertices\t: " + test.vertices.count())

println("***\tNo of input graph edges   \t: " + test.edges.count() + "\n")

val verticesWithSuccessors: VertexRDD[Array[VertexId]] =
    test.ops.collectNeighborIds(EdgeDirection.Out)

val successorSetGraph = Graph(verticesWithSuccessors, test.edges)

val neighborvertices: VertexRDD[Set[VertexId]] = verticesWithSuccessors.mapValues[Set[VertexId]]((arr: Array[VertexId]) => arr.toSet)

val ngVertices: VertexRDD[Set[VertexId]] =
    successorSetGraph.aggregateMessages[Set[VertexId]] (
    triplet => triplet.sendToDst(triplet.srcAttr.toSet),
    (s1, s2) => s1 ++ s2
    ).mapValues[Set[VertexId]](
        (id: VertexId, neighbors: Set[VertexId]) => neighbors - id
    )

val ngEdges = ngVertices.flatMap[Edge[Int]](
  {
    case (source: VertexId, allDests: Set[VertexId]) => {
      allDests.map((dest: VertexId) => Edge(source, dest,
          1
          ))
    }
  }
)

val newEdges = ngEdges.subtract(eRDD)

val neighborGraph = Graph(neighborvertices, newEdges)

val param1temp: Graph[ Set[VertexId], Set[VertexId] ] = neighborGraph.mapTriplets[Set[VertexId]] (
    (abcd: (EdgeTriplet[Set[VertexId], Int])) => ((abcd.srcAttr).intersect(abcd.dstAttr)) )
    
val param1 = (param1temp.edges).mapValues[Int]((abc: Edge[Set[VertexId]]) => abc.attr.size)

val temp1 = param1.groupBy(x => x.srcId).mapValues(_.maxBy(_.attr)).values

val temp2 = temp1.sortBy(x => x.srcId)

println("\n***\tRecommended Friend Request (By highest number of mutual friends) :\n")
temp2.foreach(x => println("***\tUser : " + x.srcId + "\tRecommended Friend : " + x.dstId + "  \tNo. of Mutual Friends : " + x.attr + "\t***"))
println()

/*
val param2temp: Graph[ Set[VertexId], Set[Edge[Int]] ] = param1temp.mapTriplets[Set[Edge[Int]]] (
    (abcd: (EdgeTriplet[ Set[VertexId], Set[VertexId] ])) => edge_Set.filter(x => ( ( (abcd.attr) contains (x.srcId) ) && ( (abcd.attr) contains (x.dstId) ) ) ) )

println("\nEdges with their Attribute as the Set of Edges that are in the graph containing only the source & destination vertices' mutual friends :")
param2temp.edges.foreach(println)
*/

val param2temp: Graph[ Set[VertexId], Double ] = param1temp.mapTriplets[Double] (
    (abcd: (EdgeTriplet[ Set[VertexId], Set[VertexId] ])) =>
    if(abcd.attr.size == 1) { 0.0 }
    else {
    ((edge_Set.filter(x => ( ( (abcd.attr) contains (x.srcId) ) && ( (abcd.attr) contains (x.dstId) ) ) ) ).size.toDouble) /
    ( (abcd.attr.size.toDouble) * (abcd.attr.size.toDouble - 1.0) )
    } )

val temp3 = param2temp.edges.groupBy(x => x.srcId).mapValues(_.maxBy(_.attr)).values

val temp4 = temp3.sortBy(x => x.srcId)

println("\n***\tRecommended Friend Request (By highest density among mutual friends) :\n")
temp4.foreach(x => println("***\tUser : " + x.srcId + "\tRecommended Friend : " + x.dstId + "  \tDensity : " + x.attr))
println()

val param3temp1: Graph[ Set[VertexId], Set[VertexId] ] = neighborGraph.mapTriplets[Set[VertexId]] (
    (abcd: (EdgeTriplet[Set[VertexId], Int])) => ((abcd.srcAttr).union(abcd.dstAttr)) )

val param3temp2: Graph[ Set[VertexId], Double ] = param3temp1.mapTriplets[Double] (
    (abcd: (EdgeTriplet[ Set[VertexId], Set[VertexId] ])) =>
    if(abcd.attr.size == 1) { 0.0 }
    else {
    ((edge_Set.filter(x => ( ( (abcd.attr) contains (x.srcId) ) && ( (abcd.attr) contains (x.dstId) ) ) ) ).size.toDouble) /
    ( (abcd.attr.size.toDouble) * (abcd.attr.size.toDouble - 1.0) )
    } )

val temp5 = param3temp2.edges.groupBy(x => x.srcId).mapValues(_.maxBy(_.attr)).values

val temp6 = temp5.sortBy(x => x.srcId)

println("\n***\tRecommended Friend Request (By highest density among union of the sets of friends) :\n")
temp6.foreach(x => println("***\tUser : " + x.srcId + "\tRecommended Friend : " + x.dstId + "  \tDensity : " + x.attr))
println()
