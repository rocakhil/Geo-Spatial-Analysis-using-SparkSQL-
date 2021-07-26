package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  println("x:"+pointPath)
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  //pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  spark.udf.register("is_neighbour",(x1:Int,y1:Int,z1:Int,x2:Int,y2:Int,z2:Int)=>(HotcellUtils.is_neighbour(x1, y1, z1, x2, y2, z2)))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5) as x ,CalculateY(nyctaxitrips._c5) as y, CalculateZ(nyctaxitrips._c1) as z,COUNT(*) as no_of_points from nyctaxitrips GROUP BY x,y,z")
  pickupInfo.createOrReplaceTempView("pickupInfo")

  // Define the min and max of x, y, z
  val minX = (-74.50/HotcellUtils.coordinateStep).toInt
  val maxX = (-73.70/HotcellUtils.coordinateStep).toInt
  val minY = (40.50/HotcellUtils.coordinateStep).toInt
  val maxY = (40.90/HotcellUtils.coordinateStep).toInt
  val minZ = 1
  val maxZ = 31
  val numCells:Int = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
  //print(numCells)
  // YOU NEED TO CHANGE THIS PART
  val sumOfX=spark.sql("SELECT SUM(no_of_points),SUM(no_of_points*no_of_points) FROM pickupInfo")
  val sigmaX:Long = sumOfX.first().getAs[Long](0);
  val sigmaXSquare:Long = sumOfX.first().getAs[Long](1);
  val xBar =  sigmaX.toDouble/numCells
  val S:Double = Math.pow((sigmaXSquare/numCells).toDouble-(Math.pow(xBar,2)),0.5)
  
  var wijXj_wij = spark.sql("SELECT a.x as x,a.y as y,a.z as z,b.no_of_points as cnt,1 as n from pickupinfo as a join pickupinfo as b on is_neighbour(a.x,a.y,a.z,b.x,b.y,b.z)")
  wijXj_wij.createOrReplaceTempView("wX");

  var sigma_wijXj_wij = spark.sql("SELECT x,y,z,sum(cnt) as sigmaWijXi,sum(n) as sigmaWij from wX group by x,y,z")
  sigma_wijXj_wij.createOrReplaceTempView("SwX_w");
//  sigma_wijXj_wij.show()

  var parts=spark.sql("SELECT x,y,z,sigmaWijXi,cast(%s*sigmaWij as double) as XbarSigmaWij,%d*sigmaWij as N_SigmaWijSqr,(sigmaWij *sigmaWij) as sigmaWijSqr from SwX_w".format(xBar.toString,numCells))
  parts.createOrReplaceTempView("parts");
 // parts.show()

  var parts2=spark.sql("SELECT x,y,z,sigmaWijXi-XbarSigmaWij as Numerator,%s*SQRT((N_SigmaWijSqr-sigmaWijSqr)/(%d-1)) as Denominator FROM parts".format(S.toString,numCells))
  parts2.createOrReplaceTempView("parts2");
 // parts2.show()

  var gscore=spark.sql("SELECT x,y,z FROM parts2 ORDER BY Numerator/Denominator DESC")
  gscore.show()
  return gscore
}
}
