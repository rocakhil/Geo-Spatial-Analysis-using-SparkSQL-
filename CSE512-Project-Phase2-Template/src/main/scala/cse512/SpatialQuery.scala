package cse512
import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

  // YOU NEED TO CHANGE THIS PART
  val point = pointString.split(",")
  val x = point(0).toDouble
  val y = point(1).toDouble
  val rectCoord =  queryRectangle.split(",")
  val maxX = Math.max(rectCoord(0).toDouble,rectCoord(2).toDouble)
  val minX = Math.min(rectCoord(0).toDouble,rectCoord(2).toDouble)
  val maxY = Math.max(rectCoord(1).toDouble,rectCoord(3).toDouble)
  val minY = Math.min(rectCoord(1).toDouble,rectCoord(3).toDouble)
  return x>=minX && x<=maxX && y<=maxY && y>=minY

}
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>ST_Contains(queryRectangle,pointString))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()
    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

      // YOU NEED TO CHANGE THIS PART
      val point = pointString.split(",")
      val x = point(0).toDouble
      val y = point(1).toDouble
      val rectCoord =  queryRectangle.split(",")
      val maxX = Math.max(rectCoord(0).toDouble,rectCoord(2).toDouble)
      val minX = Math.min(rectCoord(0).toDouble,rectCoord(2).toDouble)
      val maxY = Math.max(rectCoord(1).toDouble,rectCoord(3).toDouble)
      val minY = Math.min(rectCoord(1).toDouble,rectCoord(3).toDouble)
      return x>=minX && x<=maxX && y<=maxY && y>=minY

    }
    //ST_Contains(queryRectangle,pointString)
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>ST_Contains(queryRectangle,pointString))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()
    //print("points= ", resultDf.count())
    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    def ST_Within(querypoint1: String, querypoint2: String,distance:Double ): Boolean={
      val point1=querypoint1.split(",")
      val x1=point1(0).toDouble
      val y1=point1(1).toDouble
      val point2=querypoint2.split(",")
      val x2=point2(0).toDouble
      val y2=point2(1).toDouble

      return math.sqrt(math.pow(x2-x1,2) + math.pow(y2-y1,2)) <=distance

    }
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2,distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    def ST_Within(querypoint1: String, querypoint2: String,distance:Double ): Boolean={
      val point1=querypoint1.split(",")
      val x1=point1(0).toDouble
      val y1=point1(1).toDouble
      val point2=querypoint2.split(",")
      val x2=point2(0).toDouble
      val y2=point2(1).toDouble

      return math.sqrt(math.pow(x2-x1,2) + math.pow(y2-y1,2)) <=distance

    }
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2,distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
