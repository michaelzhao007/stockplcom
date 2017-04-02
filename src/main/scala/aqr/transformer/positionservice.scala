package aqr.transformer


import java.io.{File, PrintWriter}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

case class Fill(mType: String, time: BigInt, symName: String, fillPrice: Double, fillSize: Int, sizeIndicator: Int) extends Serializable
case class PriceUpdate(mType: String, time: BigInt, symName: String, curPrice: Double) extends Serializable

/**
  * service to get share's realtime p&l
  * the final result is saved under script/result.txt
  */
object PositionService extends PositionService {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("PositionService").set("spark.ui.showConsoleProgress", "true").set("spark.driver.memory", "5g")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val fillLines   =  sc.textFile("fills")
    val priceLines = sc.textFile("prices")
    val rawfillLines = rawFillLines(fillLines)
    val rawpriceLines = rawPriceLines(priceLines)
    val results = positionService(rawfillLines, rawpriceLines)
    printResults(results)
  }
}


class PositionService extends Serializable {
  /**
    * FillLine string to fill case class
    * @param lines
    * @return
    */
  def rawFillLines(lines: RDD[String]): RDD[Fill] =
    lines.map(line => {
      val arr = line.split(" ")
      Fill(mType = arr(0),
        time = BigInt(arr(1)),
        symName = arr(2),
        fillPrice = arr(3).toDouble,
        fillSize = arr(4).toInt,
        sizeIndicator = if(arr(5).toString=="B") 1 else -1)
    })

  /**
    * Price string to price case class
    * @param lines
    * @return
    */
  def rawPriceLines(lines: RDD[String]): RDD[PriceUpdate] =
    lines.map(line => {
      val arr = line.split(" ")
      PriceUpdate(mType = arr(0),
        time = BigInt(arr(1)),
        symName = arr(2),
        curPrice = arr(3).toDouble)
    })

  /**
    * calculate stock p&l for given price update
    * @param fills
    * @param updatedPrices
    * @return
    */
  def positionService(fills: RDD[Fill], updatedPrices: RDD[PriceUpdate]) = {
    val flist = fills.collect().sortBy(_.time)
    val plist = updatedPrices.collect().sortBy(_.time)
    var res = new ListBuffer[(String, BigInt, String, Int, Double)]()
    var idx = -1
    val inner = new Breaks;
    var idxMap = scala.collection.mutable.Map[String, Int]()
    for(p <- plist) {
      var map = scala.collection.mutable.Map[String, (Int, Double)]()
      if(!idxMap.contains(p.symName)) { idxMap.put(p.symName, 0); idx = idxMap(p.symName); }
      else idx = idxMap(p.symName)
      inner.breakable {
        for (i <- idx until flist.length) {
          val f = flist(i)
          if (f.time > p.time) {
            idxMap.put(p.symName, i)
            inner.break
          }
          else {
            if (p.symName == f.symName) {
              if (!map.contains(p.symName)) {
                map.put(p.symName, (f.sizeIndicator * f.fillSize, f.fillPrice))
              }
              else {
                val fnum = f.sizeIndicator * f.fillSize
                val pnum = map(p.symName)._1
                val total = pnum * map(p.symName)._2 + fnum * f.fillPrice
                if((fnum+pnum)==0) map.put(p.symName, (0, 0))
                else map.put(p.symName, (fnum + pnum, total / (fnum + pnum)))

              }
            }
          }
        }
      }

      if(map.contains(p.symName)) {
        res.append(("PNL", p.time, p.symName, map(p.symName)._1, (p.curPrice-map(p.symName)._2)*map(p.symName)._1))
      }
    }
    res
  }


  def printResults(results: ListBuffer[(String, BigInt, String, Int, Double)]): Unit = {
    val pw = new PrintWriter(new File("result.txt"))
    println("Resulting position:")
    println("  MsgType  Time Symbol signedPosition  P&L")
    pw.write("  MsgType  Time Symbol signedPosition  P&L \n")
    println("================================================")
    for ((msg, time, sym, pos, pl) <- results) {
      println(f"  ${msg}   ${time}  ${sym}   ${pos}      ${pl}")
      pw.write(msg + "    "+ time + "      " + sym+  "    "+ pos + "    "+pl + "\n")
    }
    pw.close

  }
}
