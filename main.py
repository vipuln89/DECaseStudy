#package import

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import *
import pyspark

#creating a spark session
spark = SparkSession.builder.appName("DE").getOrCreate()
spark

#reading the json file
rawDF = spark.read.json("/Users/vipulnarang/Desktop/Personal/DE Case Study/ol_cdump.json")

#reading a sample to visualize the schema
sample_df = rawDF.where(rawDF.title=='Shaw and death')
sample_df.show()

#schema of the raw data set
rawDF.printSchema()

#Number of rows in the raw dataset
print(rawDF.count())

#Columns in the raw dataset
print(rawDF.columns)

#selecting columns of interest
transform_df = rawDF.select("authors", "genres", "number_of_pages", "publish_date", "title")
transform_df.printSchema()

#flattening the df to by exploding the data for authors. Using explode outer because using just explode drops rows where author is null
transform_df_1 = transform_df.withColumn("author", f.explode_outer("authors.key"))

#flatenning further by exploding_outer genres given they are optional and can be null as well.
transform_df_1 = transform_df_1.withColumn("genre", f.explode_outer("genres"))

#selecting columns of interest
selected_df = transform_df_1.select("title", "author", "genre", "number_of_pages", "publish_date")

#dropping duplicated and creating a df with unique values
sel_unique_df = selected_df.dropDuplicates()


#After profiling the data, it was found that the published date column has numerous patters. Given shortage of time, applying handful clean - up steps to get a clean date column

regex_string_1 = "(^January|February|March|April|May|June|July|August|September|October|November|December|)[ ]((19|20)[0-9][0-9])|\\d+|\\/"
sel_unique_df_clean = sel_unique_df\
                      .withColumn("publish_date_new", regexp_extract(col("publish_date"), regex_string_1,0))\
                      .drop(sel_unique_df.publish_date)

#Dropping na(s)
sel_unique_df_clean2 = sel_unique_df_clean.na.drop(subset=["publish_date_new"])

#A particular type of pattern was not working well with the current version of Spark installed. Therefore, cleaning up the column
regex_string_2 = "^[1-9]$|^1[0-9]$|^2[0-9]$|^3[0-1]$"
sel_unique_df_clean3 = sel_unique_df_clean2\
                       .withColumn("publish_date_cleaned",regexp_replace(col("publish_date_new"),regex_string_2,""))\
                       .drop(sel_unique_df_clean2.publish_date_new)

sel_unique_df_clean4 = sel_unique_df_clean3.na.drop(subset=["publish_date_cleaned"])

sel_unique_df_clean4.count()

#Publish date column, as a result of the above clean up steps, had some whitespaces. So trimming them.
sel_unique_df_clean5 = sel_unique_df_clean4\
                       .withColumn('publish_date', f.trim(sel_unique_df_clean4.publish_date_cleaned))\
                       .drop(sel_unique_df_clean4.publish_date_cleaned)\
                       .filter(length(col("publish_date")) > 3)

print(sel_unique_df_clean5.count())

#Converting the mixed type of formats that are left in the date column, to a common format so that they are converted to a datetype from string
sel_unique_df2 = sel_unique_df_clean5.withColumn("publish_date_new2",
                                          f.when(f.to_date(f.col('publish_date'),"MM/dd/yyyy").isNotNull(),
                                                 f.to_date(f.col('publish_date'),"MM/dd/yyyy"))
                                          .when(f.to_date(f.col('publish_date'),"dd/MM/yyyy").isNotNull(),
                                                 f.to_date(f.col('publish_date'),"dd/MM/yyyy"))
                                          .when(f.to_date(f.col('publish_date'),"dd MMMM yyyy").isNotNull(),
                                                 f.to_date(f.col('publish_date'),"dd MMMM yyyy"))
                                          .when(f.to_date(f.col('publish_date'),"MMMM yyyy").isNotNull(),
                                                 f.to_date(f.col('publish_date'),"MMMM yyyy"))
                                          .when(f.to_date(f.col('publish_date'),"MM/yyyy").isNotNull(),
                                                 f.to_date(f.col('publish_date'),"MM/yyyy"))
                                          .when(f.to_date(f.col('publish_date'),"yyyy").isNotNull(),
                                                 f.to_date(f.col('publish_date'),"yyyy"))
                                          .otherwise("Unknown Format")
                                          )

#converting the published_date column to datetype from string
sel_unique_df3 = sel_unique_df2\
                 .withColumn('date_new',to_date(unix_timestamp(col('publish_date_new2'), 'yyyy-MM-dd')
                 .cast("timestamp"))).drop(sel_unique_df2.publish_date).drop(sel_unique_df2.publish_date_new2)

#extracting year and month into different columns
sel_unique_df4= sel_unique_df3\
                .withColumn('published_year',year(col('date_new')))\
                .withColumn('published_month',month(col('date_new')))

#Applying the cleanup steps to title, author, number of pages and date_new columns
load_df1 = sel_unique_df4.filter("title is not null and author is not null")
load_df1_2 = load_df1.filter("title is not null")
load_df1_3 = load_df1_2.filter("number_of_pages > 20")
load_df2 = load_df1_3.filter("date_new is not null")

#Filtering the dataframes to have data having published year > 1950
final_df = load_df2.filter("published_year > 1950")
final_df = final_df.filter("published_year<2020")

#trimming white space from left and right
cleaning_df = final_df\
              .withColumn('book', f.trim(final_df.title))\
              .drop(final_df.title)

#cleaning the author column
regex_string_3 = '\\/|authors'
cleaning_df2 = cleaning_df\
               .withColumn("authors", f.regexp_replace(f.col("author"), regex_string_3, ""))\
               .drop(cleaning_df.author)

#cleaning white space from author and genres column
cleaning_df3 = cleaning_df2\
               .withColumn('author', f.trim(cleaning_df2.authors))\
               .drop(cleaning_df2.authors)\
               .withColumn('genres', f.trim(cleaning_df2.genre))\
               .drop(cleaning_df2.genre)

print(cleaning_df3.count())

#using final schema to create spark sql table.
temp_table_name = "library"
cleaning_df3.createOrReplaceTempView(temp_table_name)

#Solution - Query 1 - Get the first book / author which was published - and the last one.
spark.sql("(select book, author, date_new from library where date_new = "
                "(select min(date_new) from library) limit 1) "
          "UNION "
          "(select book, author, date_new from library where date_new = "
                "(select max(date_new) from library where author is not null) limit 1)"
          ).show()


#Solution - Query Number 2 - Find the top 5 genres with most published books
spark.sql("select genres, count(distinct book) as num_of_books "
          "from library where genres is not null "
          "group by genres "
          "order by num_of_books DESC "
          "limit 5").show()

#Solution - Query Number 3 - Retrieve the top 5 authors who (co-)authored the most books. - Still Working
spark.sql("select author from library where book IN "
            "(select book from "
                "(select book, count(distinct author) as authorCount from library group by book having authorCount > 1 order by authorCount DESC)) "
          "limit 5").show()

#Solution - Query Number 4 - Per publish year, get the number of authors that published at least one book
spark.sql("select published_year, count(author) as num_of_authors from "
            "(select published_year, author, count(*) as num_of_books "
                "from library group by published_year, author order by published_year ASC) "
          "where num_of_books > 1 "
          "group by published_year "
          "order by published_year ASC").show()

#Solution - Query Number 5 - Find the number of authors and number of books published per month for years between 1950 and 1970!
spark.sql("select published_year, published_month, count(distinct book) as count_of_books, count(distinct author) as count_of_authors "
          "from library "
          "where published_year > 1950 and published_year < 1970 "
          "group by published_year, published_month "
          "Order by published_year, published_month").show()

#spark.stop()


