package SparkCore
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object insurance {
  
  def main(args: Array[String]): Unit = {
    
    val spark=SparkSession.builder().appName("spark hackathon").master("local[*]")
.config("hive.metastore.uris","thrift://localhost:9083")
.config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
.enableHiveSupport().getOrCreate();
    
spark.sparkContext.setLogLevel("error")

val sc=spark.sparkContext
    
    case class schema(IssuerId:Int,IssuerId2:Int,BusinessDate:String,StateCode:String,SourceName:String,NetworkName:String,
    custnum:Int,MarketCoverage:String,DentalOnlyPlan:String)
//Q1
val insuredata1=sc.textFile("hdfs://localhost:54310/user/hduser/sparkHack2/Hackathon/insuranceinfo1.csv");
//Q2
val header = insuredata1.first
val insuredataupdated = insuredata1.filter(line => line != header)
println("priting after removing header",insuredataupdated.take(3))
//Q3
val findheader=insuredataupdated.filter { x => x==header }

println(findheader +": header found")
//Q4
val filteredinsureRdd = insuredataupdated.filter(row => !row.isEmpty)
//Q5
val splitinsure=filteredinsureRdd.map(x=>x.split(",",-1))
//Q6
val columndata = filteredinsureRdd.filter(row => row.size==10)

println(columndata.take(1))

//Q7
val schemardd = columndata.map(x=>x.split(",")).map(x => schema(x(0).toInt,x(1).toInt,x(2),x(3),x(4),x(5),x(6).toInt,x(7),x(8)))

//Q8
println("no of rows removed:", schemardd.count - columndata.count)
//Q9
val rejectdata1 = filteredinsureRdd.filter(row => row.length<10)

println("no of rejected data",rejectdata1.count)
//Q10
val insuredata2=sc.textFile("hdfs://localhost:54310/user/hduser/sparkHack2/Hackathon/insuranceinfo2.csv");
//Q11
val header2 = insuredata2.first

val insuredataupdated2 = insuredata2.filter(line => line != header2)
val filteredinsureRdd2 = insuredataupdated2.filter(row => !row.isEmpty)

val splitinsure2=filteredinsureRdd2.flatMap(x=>x.split(",",-1))


val columndata2 = splitinsure2.filter(row => row.size<=10)  

println(columndata2.take(1))

println("no of rows removed for 2nd dataset", insuredata2.count - filteredinsureRdd2.count)


val rejectdata2 = filteredinsureRdd2.filter(row => row.length<10)

print("2nd dataset rejected data count",rejectdata2.count)

//Q12
val insuredatamerged=insuredataupdated.union(insuredataupdated2)
println("merged data count",insuredatamerged.count)
//Q13
insuredatamerged.cache;
println("printing merged data after cache:",insuredatamerged.take(5))
//Q14
val mergecount=insuredataupdated.count()+insuredataupdated2.count();

println("is data matching: "+ (mergecount.equals(insuredatamerged)))

//Q15
val actualinsuredata=insuredatamerged.distinct
println("merged data count",actualinsuredata.count)

println("duplicate record count",insuredatamerged.count()-actualinsuredata.count)

//Q16
val insuredatarepart=actualinsuredata.repartition(8)
//Q17
val rdd_20191001=insuredatarepart.map(x=>x.split(",")).filter(x => x(2).equals("2019-10-01"))
val rdd_20191002=insuredatarepart.map(x=>x.split(",")).filter(x => x(2).equals("2019-10-02"))


rdd_20191001.take(1).foreach(println)
rdd_20191002.take(1).foreach(println)

//Q18
rejectdata1.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkHack2/Hackathon/rdddata/rejectdata1")
insuredatamerged.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkHack2/Hackathon/rdddata/insuredatamerged")
rdd_20191001.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkHack2/Hackathon/rdddata/rdd_20191001")
rdd_20191002.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkHack2/Hackathon/rdddata/rdd_20191002")
//Q19 
  import spark.implicits._

val insurdaterepartdf=insuredatarepart.toDF();
println("showing rdd to dataframe",insurdaterepartdf.show())

//Q20
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import spark.implicits._

val simpleSchema = StructType(List(
    StructField("IssuerId",IntegerType,true),
    StructField("IssuerId2",IntegerType,true),
    StructField("BusinessDate",DateType,true),
    StructField("StateCode",StringType,true),
    StructField("SourceName",StringType,true),
    StructField("NetworkName",StringType,true),
    StructField("NetworkURL", StringType,true),
    StructField("custnum", IntegerType, true),
    StructField("MarketCoverage", StringType,true),
    StructField("DentalOnlyPlan", StringType,true)
  ))
  
  //Q21
val insurancedf=spark.read.format("csv").option("inferSchema","false").option("mode", "DROPMALFORMED").option("delimiter",",").schema(simpleSchema).load("hdfs://localhost:54310/user/hduser/sparkHack2/Hackathon/insuranceinfo*.csv")
println(insurancedf.count)
//Q22
//a
val col_rnmDf=insurancedf.withColumnRenamed("StateCode","stcd").withColumnRenamed("SourceName","srcnm")

println(col_rnmDf.show)
import org.apache.spark.sql.functions._

//b
val catDf=col_rnmDf.select(concat($"IssuerId".cast("String"),$"IssuerId2".cast("String")) as "issueridcomposite",$"BusinessDate",$"stcd",$"srcnm",$"NetworkName",$"NetworkURL",$"custnum",$"MarketCoverage",$"DentalOnlyPlan")

println(catDf.show)
//c
import org.apache.spark.sql.functions._
val dropdf=catDf.drop("DentalOnlyPlan")
//d
val coladddf=dropdf.withColumn("sysdt",current_date()).withColumn("systs",unix_timestamp())

println(coladddf.show)
//Q23
val cleandf=coladddf.na.drop()

println(cleandf.show)

//Q24


import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


//
val replace = udf((s: String) =>s.replaceAll("[\\.$|,|;|'[0-9]]", ""))

//spark.udf.register("replace_spl",replace)

print("check udf registerd",spark.catalog.listFunctions.filter('name like "%replace_spl").show(false))

val splremoveddf=cleandf.select(replace(col("NetworkName")))

println(splremoveddf.show)

splremoveddf.write.mode(SaveMode.Overwrite).json("hdfs://localhost:54310/user/hduser/sparkHack2/Hackathon/insurance.json")
splremoveddf.write.mode(SaveMode.Overwrite).option("header","true").option("delimiter","~").csv("hdfs://localhost:54310/user/hduser/sparkHack2/Hackathon/insurancefilter.csv")
 import org.apache.spark.sql
  import spark.implicits._
println("Writing to hive")

splremoveddf.createOrReplaceTempView("insurancedbview")

spark.sql("drop table if exists insurancedbview")
spark.sql("create table if not exists default.insurancedbview(IssuerId Int,IssuerId2 Int,BusinessDate String,StateCode String,SourceName String,NetworkName String,custnum:Int,MarketCoverage:String,DentalOnlyPlan String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
cleandf.write.mode("overwrite").saveAsTable("default.insurancedbview")    


//4 .Tale of handling RDDs, DFs and TempViews  (20% Completion)

//Q30

val cust_states=sc.textFile("hdfs://localhost:54310/user/hduser/sparkHack2/Hackathon/custs_states.csv")

//Q31
val custfilter=cust_states.map(x=>x.split(",")).filter(s => s.length >2)

val statesfilter=cust_states.map(x=>x.split(",")).filter(s => s.length <5)

println(custfilter.take(3))
println(statesfilter.take(3))

//Q32
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
//Q33
val cust_statsDF=spark.read.option("inferSchema","false").csv("hdfs://localhost:54310/user/hduser/sparkHack2/Hackathon/custs_states.csv")

//Q34
val custdf=cust_statsDF.na.drop(how="any").createOrReplaceTempView("custview")
val statesdf=cust_statsDF.filter("_c2 is null").select("_c0","_c1").createOrReplaceTempView("statesview")

//Q35
cleandf.createOrReplaceTempView("insureview")

//Q36
//import org.apache.spark.sql.functions._

//def replace(s:String):String={return s.replaceAll("[\\.$|,|;|'[0-9]]", "")}

//val replaceudf=udf(replace_)
//val replace = udf((s: String) =>s.replaceAll("[\\.$|,|;|'[0-9]]", ""))

//spark.udf.register("remspecialcharudf ", replace _)
//Q37
//spark.sql("select *,remspecialcharudf(NetworkName) as cleannetworkName").show

println(cleandf.withColumn("curdt",current_date).withColumn("curts",current_timestamp).show)
println(cleandf.withColumn("yr",year($"BusinessDate")).withColumn("mth",month($"BusinessDate"))show)
val insurfinaldf=cleandf.withColumn("protocol", when(substring($"NetworkURL",0,5) like "%https%","https")
      .when(substring($"NetworkURL",0,5) like "%http%","http")
      .otherwise("noprotocol"))      
      

insurfinaldf.createOrReplaceTempView("insurancefinalview")

println(spark.sql("select * from custview").show)

println(spark.sql("select * from statesview").show)

println(spark.sql("select i.*,s._c0,c._c3,c._c4 from insurancefinalview i inner join statesview s on i.stcd = s._c0 inner join custview c on i.custnum = c._c0").show)

val finalinsuranceDF=spark.sql("select i.*,s._c0 as state,c._c3 as age,c._c4 as profession from insurancefinalview i inner join statesview s on i.stcd = s._c0 inner join custview c on i.custnum = c._c0")

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

finalinsuranceDF.show

finalinsuranceDF.createOrReplaceTempView("final_tbl")

println(spark.sql("select avg(age),count(state),state,protocol,profession from final_tbl group by state,protocol,profession order by state desc").show(100))


finalinsuranceDF.write.mode(SaveMode.Overwrite).parquet("hdfs://localhost:54310/user/hduser/sparkHack2/Hackathon/insurancefinal.parquet")

    
  }
  
}