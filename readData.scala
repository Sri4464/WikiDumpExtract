import java.io.File
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object readData {

  def main (args:Array[String]): Unit = {


    val warehouseLocation = new File("hive-warehouse").getAbsolutePath
    println(warehouseLocation)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Read the Wiki Data From the file")
      .enableHiveSupport()
      .getOrCreate()

    val customSchema = StructType(Array(
      StructField("language", StringType, true),
      StructField("Page_Name", StringType, true),
      StructField("non_unique_views", StringType, true),
      StructField("bytes_transferred", StringType, true)))

    val readWikiData = spark.read.format("text")
      .option("header", "false")
      .load("/home/cloudera/Desktop/pagecounts-20120101-000000.txt")



    readWikiData.createOrReplaceTempView("STAGE_1_WIKI")
    spark.sql("insert into wiki_ods.wiki_stage " +
      "SELECT CONCTNS.splitted_cnctns[0] AS Language, CONCTNS.splitted_cnctns[1] AS Page_Title, " +
      "CONCTNS.splitted_cnctns[2] AS non_unique_views, CONCTNS.splitted_cnctns[3] AS bytes_transferred FROM " +
      "(SELECT split(value,' ') AS splitted_cnctns FROM STAGE_1_WIKI)CONCTNS")

    spark.sql("insert into table wiki_ods.wiki_ods_1  " +
      "SELECT CONCTNS.splitted_cnctns[0] AS Language , CONCTNS.splitted_cnctns[1] AS category, " +
      "CONCTNS.page_title,CONCTNS.non_unique_views, CONCTNS.bytes_transferred" +
      " FROM" +
      " (SELECT split(Language,'\\\\.') AS splitted_cnctns,page_title,non_unique_views, " +
      "bytes_transferred FROM wiki_ods.wiki_stage)CONCTNS")

    spark.sql("select language,  count(page_title), sum(non_unique_views) as vws from wiki_ods.wiki_ods_1 " +
      "group by language having count(page_title) <= 10 order by vws desc").show()

  }
}
