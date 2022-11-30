// open hive tables as spark dataframe (name, crew, rating, title)
val name = spark.table("tangn_name")
val crew = spark.table("tangn_crew")
val rating = spark.table("tangn_rating")
val title = spark.table("tangn_title")
val rotten = spark.table("tangn_rotten")

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

movies.createOrReplaceTempView("movies") 

// build on that, assgin the rank to each movie within its genre
val rank_not_null = spark.sql("""select * from movies where avg_rating is not null""")

rank_not_null.createOrReplaceTempView("rank_not_null")

val movies_with_rank = spark.sql("""select *, rank() over (partition by genre order by avg_rating desc) as genre_rank
    from rank_not_null
    """)

movies_with_rank.createOrReplaceTempView("movies_with_rank")

val movies_with_rank_10 = spark.sql("""select * from movies_with_rank where genre_rank<=10""")

movies_with_rank_10.createOrReplaceTempView("movies_with_rank_10")

// join rotten tomato data to the movie table
val movies_with_rotten = spark.sql("""select m.*, r.critic_rating, r.critic_review from movies m right join rotten r on m.primary_title=r.primarytitle and m.year=r.startyear""")

movies_with_rotten.createOrReplaceTempView("movies_with_rotten")

val movies_rotten_rank = spark.sql("""select *, rank() over (partition by genre order by critic_rating desc) as rotten_rank from movies_with_rotten where primary_title is not null""")

movies_rotten_rank.createOrReplaceTempView("movies_rotten_rank")

val movies_rotten_rank_10 = spark.sql("""select * from movies_rotten_rank where rotten_rank<=10""")

movies_rotten_rank_10.createOrReplaceTempView("movies_rotten_rank_10")

// save to Hive
import org.apache.spark.sql.SaveMode

// this table is for only output the info about input movie
movies.write.mode(SaveMode.Overwrite).saveAsTable("tangn_movies_info")

// this table is for recommending based on IMDb ratings
// movies_with_rank.write.mode(SaveMode.Overwrite).saveAsTable("tangn_movies_with_rank_info")

movies_with_rank_10.write.mode(SaveMode.Overwrite).saveAsTable("tangn_movies_with_rank_10")

// this table is for recommending based on rotten tomatoes ratings
movies_rotten_rank_10.write.mode(SaveMode.Overwrite).saveAsTable("tangn_movies_rotten_rank_10")

