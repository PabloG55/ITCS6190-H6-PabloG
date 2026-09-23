# Hands-on L6: Report

**Name:** Pablo Garces
**Student ID:** 801377963
**Email:** <pgarces@charlotte.edu>

---

## Seed and commands

Seed used for `datagen.py`: `801377963`

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

```bash
python3 datagen.py 801377963

docker compose up -d

docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output

# repeated the docker cp + spark-submit above after filling in the schema,
# then again after finishing each of the four tasks

docker compose down
```

---

## Results

For each task, the first ten rows of your output (from the terminal or the CSV file) and one
or two sentences on what they say about your data.

### Task 1: favorite genre per user

```text
+--------+---------+----------+
|user_id |genre    |play_count|
+--------+---------+----------+
|user_1  |Rock     |7         |
|user_10 |Rock     |8         |
|user_100|Hip-Hop  |11        |
|user_11 |Pop      |5         |
|user_12 |Pop      |5         |
|user_13 |Rock     |5         |
|user_14 |Hip-Hop  |3         |
|user_15 |Jazz     |9         |
|user_16 |Pop      |3         |
|user_17 |Jazz     |7         |
+--------+---------+----------+
```

Every one of the 100 users ends up with exactly one favorite genre, which is expected since
`datagen.py` specifically makes each user have a preferred genre. Rock, Hip-Hop, Pop, and Jazz all show up
already in just this first slice of 10 users, so the bias is spread across genres rather than
everyone having the same one.

### Task 2: average listening time per song

```text
+-------+-------------+----------------+----------+
|song_id|title        |avg_duration_sec|play_count|
+-------+-------------+----------------+----------+
|song_1 |Title_song_1 |213.13          |15        |
|song_25|Title_song_25|198.0           |19        |
|song_13|Title_song_13|196.53          |15        |
|song_39|Title_song_39|193.89          |19        |
|song_2 |Title_song_2 |193.52          |27        |
|song_42|Title_song_42|190.58          |12        |
|song_8 |Title_song_8 |187.8           |25        |
|song_36|Title_song_36|187.75          |20        |
|song_46|Title_song_46|185.76          |21        |
|song_11|Title_song_11|185.62          |13        |
+-------+-------------+----------------+----------+
```

With these results, top 10 play counts are around 12 to 30 plays and they don't seem to have any obvious
relationship with how long a song runs on average. Also, the average duration ranges from 213.13s
to 126.5s so it appears to be a spread across the 50 songs.

### Task 3: genre loyalty score, top 10

```text
+-------+---------+----------+-----------+-------------+
|user_id|genre    |play_count|total_plays|loyalty_score|
+-------+---------+----------+-----------+-------------+
|user_3 |Rock     |9         |9          |1.0          |
|user_32|Rock     |8         |8          |1.0          |
|user_17|Jazz     |7         |7          |1.0          |
|user_69|Rock     |4         |4          |1.0          |
|user_56|Rock     |12        |13         |0.923        |
|user_78|Classical|10        |11         |0.909        |
|user_96|Rock     |10        |11         |0.909        |
|user_48|Hip-Hop  |9         |10         |0.9          |
|user_49|Jazz     |9         |10         |0.9          |
|user_6 |Jazz     |8         |9          |0.889        |
+-------+---------+----------+-----------+-------------+
```

Why do users with few plays tend to get a score of 1.0? Would you change the definition of
the score to account for that?

There are exactly four users with a perfect score of 1.0 and they all have a few total plays (between 4 and 9).
This is because of how the query is constructed: it makes it easier for lower play counts to have a perfect
score, because if all those plays happen to be the same genre, the score will be perfect. So as the number of
plays goes up, it is harder for a user to get a perfect score, since they will more likely listen to more
genres. There are a couple of ways to improve this, and the simplest one could be adding a minimum play-count
threshold to actually get more accurate scores.

### Task 4: night owls

```text
+-------+-----------+
|user_id|night_plays|
+-------+-----------+
|user_62|7          |
|user_60|6          |
|user_72|6          |
|user_84|6          |
|user_19|5          |
|user_51|5          |
|user_6 |5          |
|user_76|5          |
|user_94|5          |
|user_18|4          |
+-------+-----------+
```

`user_62` is the biggest night owl with 7 plays between midnight and 5 AM. It also seems that the count
gradually reduces instead of dropping hard. In addition, it also seems that most users have at least one
late night music session.

---

## The plan

Paste the `explain()` output of task 1:

```text
== Physical Plan ==
AdaptiveSparkPlan (43)
+- == Final Plan ==
   ResultQueryStage (25), Statistics(sizeInBytes=8.0 EiB)
   +- TakeOrderedAndProject (24)
      +- * Project (23)
         +- * Filter (22)
            +- Window (21)
               +- WindowGroupLimit (20)
                  +- * Sort (19)
                     +- AQEShuffleRead (18), coalesced
                        +- ShuffleQueryStage (17), Statistics(sizeInBytes=4.8 KiB, rowCount=100)
                           +- Exchange (16)
                              +- WindowGroupLimit (15)
                                 +- * Sort (14)
                                    +- * HashAggregate (13)
                                       +- AQEShuffleRead (12), coalesced
                                          +- ShuffleQueryStage (11), Statistics(sizeInBytes=15.0 KiB, rowCount=311)
                                             +- Exchange (10)
                                                +- * HashAggregate (9)
                                                   +- * Project (8)
                                                      +- * BroadcastHashJoin Inner BuildRight (7)
                                                         :- * Filter (2)
                                                         :  +- Scan csv  (1)
                                                         +- BroadcastQueryStage (6), Statistics(sizeInBytes=4.0 MiB, rowCount=50)
                                                            +- BroadcastExchange (5)
                                                               +- * Filter (4)
                                                                  +- Scan csv  (3)
+- == Initial Plan ==
   TakeOrderedAndProject (42)
   +- Project (41)
      +- Filter (40)
         +- Window (39)
            +- WindowGroupLimit (38)
               +- Sort (37)
                  +- Exchange (36)
                     +- WindowGroupLimit (35)
                        +- Sort (34)
                           +- HashAggregate (33)
                              +- Exchange (32)
                                 +- HashAggregate (31)
                                    +- Project (30)
                                       +- BroadcastHashJoin Inner BuildRight (29)
                                          :- Filter (26)
                                          :  +- Scan csv  (1)
                                          +- BroadcastExchange (28)
                                             +- Filter (27)
                                                +- Scan csv  (3)

(1) Scan csv
Output [2]: [user_id#0, song_id#1]
Location: InMemoryFileIndex [file:/opt/spark/work-dir/shared/input/listening_logs.csv]
PushedFilters: [IsNotNull(song_id)]
ReadSchema: struct<user_id:string,song_id:string>

(3) Scan csv
Output [2]: [song_id#4, genre#7]
Location: InMemoryFileIndex [file:/opt/spark/work-dir/shared/input/songs_metadata.csv]
PushedFilters: [IsNotNull(song_id)]
ReadSchema: struct<song_id:string,genre:string>

(5) BroadcastExchange
Arguments: HashedRelationBroadcastMode(List(input[0, string, false]),false)

(7) BroadcastHashJoin [codegen id : 2]
Left keys [1]: [song_id#1]
Right keys [1]: [song_id#4]
Join type: Inner

(9) HashAggregate
Keys [2]: [user_id#0, genre#7]
Functions [1]: [partial_count(1)]

(10) Exchange
Arguments: hashpartitioning(user_id#0, genre#7, 200), ENSURE_REQUIREMENTS

(13) HashAggregate
Keys [2]: [user_id#0, genre#7]
Functions [1]: [count(1)]
Results [3]: [user_id#0, genre#7, count(1)#37L AS play_count#28L]

(16) Exchange
Arguments: hashpartitioning(user_id#0, 200), ENSURE_REQUIREMENTS

(21) Window
Arguments: [row_number() windowspecdefinition(user_id#0, play_count#28L DESC NULLS LAST,
genre#7 ASC NULLS FIRST, ...) AS rn#38], [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST]

(22) Filter
Condition : (rn#38 = 1)

(24) TakeOrderedAndProject
Arguments: 21, [user_id#0 ASC NULLS FIRST], [user_id#40, genre#41, play_count#42]

(43) AdaptiveSparkPlan
Arguments: isFinalPlan=true
```

Your reading of it: where are the two file scans, which operator is the join and which kind
of join did Spark choose, where are the shuffles (`Exchange`) and why are they needed, and
how does this match the diagram in the SQL / DataFrame tab of the Spark UI?

The two file scans are `(1) Scan csv` on `listening_logs.csv` and `(3) Scan csv` on
`songs_metadata.csv`. The join is `(7) BroadcastHashJoin Inner BuildRight`. Spark
picked a broadcast join. There are two real shuffles (`Exchange`), both needed
because a join doesn't require one here but the two aggregations do:

- `(10) Exchange hashpartitioning(user_id, genre, 200)` is needed so every row belonging to
  the same `(user_id, genre)` pair lands on the same executor and gets summed correctly.

- `(16) Exchange hashpartitioning(user_id, 200)` is needed because `Window.partitionBy("user_id")`
  requires all of one user's rows to be on the same executor before they can be ranked
  against each other.

This matches the SQL / DataFrame tab exactly: the diagram for this query shows the same shape
as boxes and arrows, scan, filter, broadcast join, project, hash aggregate, exchange, hash
aggregate, exchange, sort/window, filter, project, in the same order as the text plan above,
since it was copied directly from the same place too.

---

## Transformations and actions

Which lines of your `main.py` are actions? How many jobs did the program launch according to
the Spark UI, and is that what you expected?

The actions are the two `.count()` calls in `print(f"{logs.count()} log rows, {songs.count()}
songs")`, plus, inside `save()`, `df.show(20, truncate=False)` and
`df.coalesce(1).write...csv(...)` for each task whose result isn't `None`. That's 2 + (2 x 4
tasks) = 10 actions total. Everything else, `groupBy`, `agg`, `join`, `filter`,
`withColumn`, `orderBy`, `select`, `limit`, are transformations and don't run anything
by themselves.

---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

Once again, ports 4040 and 8081 were already taken by other local processes I had running that had
nothing to do with Spark, so `docker compose up -d` failed twice in a row:

```text
Error response from daemon: ports are not available: exposing port TCP 0.0.0.0:4040 -> 127.0.0.1:0: listen tcp 0.0.0.0:4040: bind: address already in use
Error response from daemon: ports are not available: exposing port TCP 0.0.0.0:8081 -> 127.0.0.1:0: listen tcp 0.0.0.0:8081: bind: address already in use
```

I found the owning PIDs with `lsof -nP -iTCP:<port> -sTCP:LISTEN` and killed them, then
`docker compose up -d` succeeded with both workers showing ALIVE.

Other than that, filling in the schema and the four tasks went smoothly, no code errors once
I had the column names and join keys right.
