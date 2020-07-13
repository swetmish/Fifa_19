import org.apache.spark.sql.{SparkSession, functions}

object Main_Fifa {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    //start - a. Which club has the most number of left footed midfielders under 30 years of age?
    val df = spark.read.format("csv").option("header", "true").load("C://fifa_data//data.csv")
    df.createOrReplaceTempView("data")
    var data1 = spark.sql("select Club, count(Position) as count_pos from data where Club is not null and Position in ('LM','LW') and Age < 30 group by Club")
    data1.createOrReplaceTempView("data1")
    val data2 = spark.sql("select Club, count_pos, rank() over (order by count_pos desc) as rank from data1")
    data2.filter(data2("rank") === 1).select("Club").show()
    //end - a. Which club has the most number of left footed midfielders under 30 years of age


    //start - Which position pays the highest wage in average?

    /*var data3 = df.withColumn("Wage", functions.regexp_replace(df.col("Wage"), "\\D", ""))
    data3.createOrReplaceTempView("data3")
    var data4 = spark.sql("select Position, avg(Wage) as wg from data3 group by Position")
    var max_avg_wage = data4.groupBy().max("wg").show()
    data4.filter(data4("wg") === max_avg_wage).select("Position").show()*/

    //end - Which position pays the highest wage in average?

    //start - Which team has the most expensive squad value in the world? Does that team also have the largest wage bill ?

    var data_exp = df.withColumn("Value", functions.regexp_replace(df.col("Value"), "\\D", ""))
    import org.apache.spark.sql.types._
    //val max_Value = data_exp.withColumn("Value", data_exp("Value").cast(IntegerType)).groupBy().max("Value").show()
    //println(max_Value("Value"))
    //var max_val = data_exp.groupBy().max("Value").show()
    //data_exp.filter(data_exp("Value") === ).select("Club").show()

    //spark.sql("select Club, Value, rank() over (order by Value desc) as rank  from data_exp where rank == 1").show()






    //end - Which team has the most expensive squad value in the world? Does that team also have the largest wage bill ?
  }
}