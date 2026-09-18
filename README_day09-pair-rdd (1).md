# Day 9 — Pair RDD

## Task
Create key-value RDDs, compare `reduceByKey` vs `groupByKey`, use `mapValues`, and aggregate bank transactions and department sales.

## Code — `src/main/scala/Day09App.scala`
```scala
import org.apache.spark.sql.SparkSession

object Day09App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day09-PairRDD").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    val path = "bank_transactions.csv"
    val pw = new java.io.PrintWriter(path)
    pw.write("ACC1,500.0\nACC2,1200.0\nACC1,300.0\nACC3,750.0\nACC2,450.0\nACC1,200.0\n")
    pw.close()

    val pairRDD = sc.textFile(path).map { line =>
      val parts = line.split(",")
      (parts(0), parts(1).toDouble)
    }

    val totalsByAccount = pairRDD.reduceByKey(_ + _)
    println("=== Totals by account (reduceByKey) ===")
    totalsByAccount.collect().foreach(println)

    val groupedByAccount = pairRDD.groupByKey()
    println("\n=== Grouped values by account (groupByKey) ===")
    groupedByAccount.collect().foreach { case (acc, vals) => println(s"$acc -> ${vals.toList}") }

    val roundedTotals = totalsByAccount.mapValues(v => math.round(v))
    println("\n=== Rounded totals (mapValues, no shuffle) ===")
    roundedTotals.collect().foreach(println)

    val deptPath = "dept_sales.csv"
    val pwd = new java.io.PrintWriter(deptPath)
    pwd.write("Electronics,1000\nGrocery,500\nElectronics,1500\nGrocery,700\nClothing,300\n")
    pwd.close()

    val deptRevenue = sc.textFile(deptPath)
      .map(l => (l.split(",")(0), l.split(",")(1).toInt))
      .reduceByKey(_ + _)
    println("\n=== Revenue by department ===")
    deptRevenue.collect().foreach(println)

    spark.stop()
  }
}
```

## Output
> Predicted — confirm by running `sbt run` (element order within `collect()` results may vary).
```
=== Totals by account (reduceByKey) ===
(ACC1,1000.0)
(ACC2,1650.0)
(ACC3,750.0)

=== Grouped values by account (groupByKey) ===
ACC1 -> List(500.0, 300.0, 200.0)
ACC2 -> List(1200.0, 450.0)
ACC3 -> List(750.0)

=== Rounded totals (mapValues, no shuffle) ===
(ACC1,1000)
(ACC2,1650)
(ACC3,750)

=== Revenue by department ===
(Electronics,2500)
(Grocery,1200)
(Clothing,300)
```

## Explanation — what's happening

**1. Building the Pair RDD**
```scala
sc.textFile(path).map { line => val parts = line.split(","); (parts(0), parts(1).toDouble) }
```
Each line `"ACC1,500.0"` becomes a `(String, Double)` tuple — this is what makes it a **Pair RDD**, unlocking key-based operations like `reduceByKey`, `groupByKey`, `mapValues`.

**2. `reduceByKey` — efficient aggregation**
```scala
pairRDD.reduceByKey(_ + _)
```
Sums amounts per account. Internally, Spark first combines values **locally on each partition** (e.g., if two `ACC1` records happen to be on the same partition, they're summed there first), and only then shuffles the partial sums — much less data crosses the network than shipping every raw value.

**3. `groupByKey` — the less efficient alternative**
```scala
pairRDD.groupByKey()
```
Groups all raw values per key **without any local pre-combination** — every individual value is shuffled across the network first, and only combined (here, just materialized into a list) after landing on the destination partition. Same final total is reachable by summing the list, but at higher shuffle cost.

**4. `mapValues` — no shuffle needed**
```scala
totalsByAccount.mapValues(v => math.round(v))
```
Transforms only the value side of each pair, keeping the RDD's existing partitioning intact — since keys don't move, no shuffle is required, unlike `reduceByKey`/`groupByKey`.

**5. Department revenue**
Same `reduceByKey` pattern applied to a differently-shaped CSV — demonstrates the pattern generalizes to any key/value aggregation (accounts, departments, products, etc.).

## Viva Q&A
| Question | Answer |
|---|---|
| Why is `reduceByKey` preferred over `groupByKey` for summing? | `reduceByKey` combines values locally per partition before shuffling (map-side combine), transferring far less data over the network than `groupByKey`, which shuffles every raw value first. |
| Does `mapValues` trigger a shuffle? | No — it only transforms the value of each pair and preserves the RDD's partitioning, so keys never need to move. |
| What would `groupByKey().mapValues(_.sum)` produce compared to `reduceByKey(_+_)`? | The same final totals, but `groupByKey` does it less efficiently since it ships all raw values across the network before summing, whereas `reduceByKey` pre-sums per partition first. |
| What makes an RDD a "Pair RDD"? | Its elements are 2-tuples (`(K, V)`) — this unlocks key-based operations like `reduceByKey`, `groupByKey`, `join`, `mapValues`, etc. |
| In `totalsByAccount.mapValues(v => math.round(v))`, why use `mapValues` instead of plain `map`? | `mapValues` only transforms the value, automatically preserving the existing key-partitioning; a plain `map` with a tuple-returning function would lose Spark's guarantee about partitioning being unchanged. |
