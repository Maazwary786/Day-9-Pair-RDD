import org.apache.spark.sql.SparkSession

object Day09App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day09-PairRDD").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    // Key-value RDD: (accountId, amount)
    val path = "bank_transactions.csv"
    val pw = new java.io.PrintWriter(path)
    pw.write("ACC1,500.0\nACC2,1200.0\nACC1,300.0\nACC3,750.0\nACC2,450.0\nACC1,200.0\n")
    pw.close()

    val pairRDD = sc.textFile(path).map { line =>
      val parts = line.split(",")
      (parts(0), parts(1).toDouble)
    }

    // reduceByKey - combines locally per partition before shuffling (efficient)
    val totalsByAccount = pairRDD.reduceByKey(_ + _)
    println("=== Totals by account (reduceByKey) ===")
    totalsByAccount.collect().foreach(println)

    // groupByKey - shuffles ALL raw values first, groups after (less efficient, more network I/O)
    val groupedByAccount = pairRDD.groupByKey()
    println("\n=== Grouped values by account (groupByKey) ===")
    groupedByAccount.collect().foreach { case (acc, vals) => println(s"$acc -> ${vals.toList}") }

    // mapValues - transform only the value, keeps partitioning (no shuffle)
    val roundedTotals = totalsByAccount.mapValues(v => math.round(v))
    println("\n=== Rounded totals (mapValues, no shuffle) ===")
    roundedTotals.collect().foreach(println)

    // Revenue by product/department style aggregation
    val deptPath = "dept_sales.csv"
    val pwd = new java.io.PrintWriter(deptPath)
    pwd.write("Electronics,1000\nGrocery,500\nElectronics,1500\nGrocery,700\nClothing,300\n")
    pwd.close()

    val deptRevenue = sc.textFile(deptPath)
      .map(l => (l.split(",")(0), l.split(",")(1).toInt))
      .reduceByKey(_ + _)
    println("\n=== Revenue by department ===")
    deptRevenue.collect().foreach(println)

    // Performance note: reduceByKey pre-aggregates on each partition BEFORE
    // shuffling, so far less data crosses the network than groupByKey, which
    // ships every individual value across the shuffle and only combines
    // after all data lands on the destination partition.

    spark.stop()
  }
}
