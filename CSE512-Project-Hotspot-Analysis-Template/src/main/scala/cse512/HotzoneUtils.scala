package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
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

  // YOU NEED TO CHANGE THIS PART

}
