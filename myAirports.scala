package com.sparkTutorial.myCodes

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}


object myAirports {


  def main(args: Array[String]) {
    val modifyName=(Name:String) =>
    {
      Name.substring(1,Name.length()-1)
    }

    val laxX=33.942
    val laxY= -118.41
    val getDistance=(coorX:Double,coorY:Double) =>
      {
        val lat1=math.Pi/180 * laxX
        val lon1=math.Pi/180 * laxY
        val lat2=math.Pi/180 * coorX
        val lon2=math.Pi/180 * coorY

        val dlon=lon2-lon1
        val dlat= lat2-lat1

        val a= math.pow(math.sin(dlat/2),2)+ math.cos(lat1) * math.cos(lat2) * math.pow(math.sin(dlon/2),2)
        val c = 2 * math.atan2(math.sqrt(a),math.sqrt(1-a))
        val miles= 6367 * c /  1.6
        miles
      }

    val conf = new SparkConf().setAppName("airports").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val airportsRDD = sc.textFile("in/airports.text")

    val airportCoordinatesRDD = airportsRDD.map(line => (line.split(Utils.COMMA_DELIMITER)(3),
      line.split(Utils.COMMA_DELIMITER)(1),line.split(Utils.COMMA_DELIMITER)(6).toDouble,line.split(Utils.COMMA_DELIMITER)(7).toDouble))


    val airportDistancesRDD = airportCoordinatesRDD.map(line => (modifyName(line._1),modifyName( line._2), getDistance(line._3, line._4) ))



    airportDistancesRDD.saveAsTextFile("out/distancesFromLAX.text")
      print("Miles to these airports from LAX: ")
   // for ((country,airport,distance) <- airportDistancesRDD.collect()) println(country+ ", "+ airport + ", " + distance)


  }

}
