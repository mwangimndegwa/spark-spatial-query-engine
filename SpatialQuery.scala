package spatial

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	// ST_Contains: returns true if pointString is inside or on the boundary of queryRectangle
	spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => {
	 try{
	   val rect = queryRectangle.split(",").map(_.trim.toDouble)
           val point = pointString.split(",").map(_.trim.toDouble)
           // rectangle: x1,y1,x2,y2  (order may vary), so compute mins/maxs
           val minX = Math.min(rect(0), rect(2))
           val maxX = Math.max(rect(0), rect(2))
           val minY = Math.min(rect(1), rect(3))
           val maxY = Math.max(rect(1), rect(3))
           val x = point(0)
           val y = point(1)
           (x >= minX) && (x <= maxX) && (y >= minY) && (y <= maxY)
          } catch {
	    case _: Throwable => false // parse error -> false
	  }
	 })
	 val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
	  resultDf.show()

	return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
	rectangleDf.createOrReplaceTempView("rectangle")

	// ST_Contains: returns true if pointString is inside or on the boundary of queryRectangle
	spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => {
         try {
           val rect = queryRectangle.split(",").map(_.trim.toDouble)
           val point = pointString.split(",").map(_.trim.toDouble)
           // rectangle: x1,y1,x2,y2  (order may vary), so compute mins/maxs
           val minX = Math.min(rect(0), rect(2))
           val maxX = Math.max(rect(0), rect(2))
           val minY = Math.min(rect(1), rect(3))
           val maxY = Math.max(rect(1), rect(3))
           val x = point(0)
           val y = point(1)
           (x >= minX) && (x <= maxX) && (y >= minY) && (y <= maxY)
         } catch {
           case _: Throwable => false // parse error -> false
          }
        })

	val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
	resultDf.show()

	return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point")

	// ST_Within: returns true if the Euclidean distance between the two points is <= distance
        spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => {
         try {
           val p1 = pointString1.split(",").map(_.trim.toDouble)
           val p2 = pointString2.split(",").map(_.trim.toDouble)

           val dx = p1(0) - p2(0)
           val dy = p1(1) - p2(1)

           val dist = Math.sqrt(dx * dx + dy * dy)

           dist <= distance
         } catch {
           case _: Throwable => false
         }
       })

	val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
	resultDf.show()

	return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

	val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
	pointDf.createOrReplaceTempView("point1")

	val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
	pointDf2.createOrReplaceTempView("point2")

	// ST_Within: returns true if the Euclidean distance between the two points is <= distance
        spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => {
         try {
           val p1 = pointString1.split(",").map(_.trim.toDouble)
           val p2 = pointString2.split(",").map(_.trim.toDouble)

           val dx = p1(0) - p2(0)
           val dy = p1(1) - p2(1)

           val dist = Math.sqrt(dx * dx + dy * dy)

           dist <= distance
         } catch {
           case _: Throwable => false
         }
       })

	val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
	resultDf.show()

	return resultDf.count()
  }
}
