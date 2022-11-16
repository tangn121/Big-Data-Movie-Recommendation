// open hive tables as spark dataframe (name, crew, rating, title)
val name = spark.table("tangn_name")
val crew = spark.table("tangn_crew")
val rating = spark.table("tangn_rating")
val title = spark.table("tangn_title")

// join rating to the title, keep only titletype='movie'
val movie_rating = spark.sql("""select t.titleid as movie_id, t.primarytitle as primary_title, t.originaltitle as origin_title, t.startyear as year, t.genres as genre, r.averagerating as avg_rating, r.numvotes as num_votes
    from title t
    left join rating r
    on t.titleid = r.titleid
    where t.titletype='movie'
    """)
  movie_rating.createOrReplaceTempView("movie_rating")
  
// select the first director and first writer in the crew
val crew_first = spark.sql("""select titleid as movie_id, split(directors, ',')[0] as director, split(writers, ',')[0] as writer
    from crew
    """)
    crew_first.createOrReplaceTempView("crew_first")
    
// join crew name to crew
val director_name = spark.sql("""select c.movie_id, c.director, ifnull(n.primaryname, 'NA') as director_name
    from crew_first c
    left join name n
    on c.director = n.nameid
    """)
    director_name.createOrReplaceTempView("director_name")

val writer_name = spark.sql("""select c.movie_id, c.writer, ifnull(n.primaryname, 'NA') as writer_name
    from crew_first c
    left join name n
    on c.writer = n.nameid
    """)
    writer_name.createOrReplaceTempView("writer_name")

// join crew to title
val movies = spark.sql("""select m.*, d.director_name, w.writer_name
    from movie_rating m
    left join director_name d
    on m.movie_id = d.movie_id
    left join writer_name w
    on m.movie_id = w.movie_id
    """)
    
// save to Hive
import org.apache.spark.sql.SaveMode
movies.write.mode(SaveMode.Overwrite).saveAsTable("tangn_movies_info")
