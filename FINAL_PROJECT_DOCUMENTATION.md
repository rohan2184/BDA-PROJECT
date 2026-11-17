# Customer Segmentation of E-Commerce Big Data using PySpark K-Means (RFM)

## Complete Project Documentation: theory, technology, implementation, results and presentation

| | |
|---|---|
| **Course** | Big Data Analytics, Sem VII CSE/ICT, Adani University |
| **Reference topic** | #34, Customer Segmentation Using Scalable Clustering Techniques |
| **Author** | Rohan Upadhyay (solo project) |
| **Environment** | Google Colab (Apache Spark 4.0.5, 2 cores). Also verified locally on Spark 3.5.3 |
| **Main notebook** | `BDA_Customer_Segmentation.ipynb` |
| **Presentation** | Make-up slot, 16/17 Nov 2026, 10–11 AM. 5 slides in 5 minutes, slide 4 is a live Colab demo |

This file brings together everything in `README.md`, `PHASE_PLAN.md`, `PROJECT_REPORT.md`, `TECHNICAL_GUIDE.md`, `DEMO_GUIDE.md` and `PRESENTATION_SCRIPT.md`. It also adds the theory behind the topic and the technology. You can read it from start to finish as one document. The original files are still there if you need just one part.

---

## Table of contents

**Part A: Overview**
1. Abstract
2. Problem statement and objectives
3. Syllabus and course-outcome mapping

**Part B: Theory**

4. Big Data fundamentals
5. Hadoop, MapReduce and why Spark replaced it for this job
6. Apache Spark in depth
7. Spark SQL and the Catalyst optimiser
8. Spark MLlib
9. Customer segmentation
10. The RFM model
11. Clustering and the K-Means algorithm
12. Feature scaling and transformation
13. Choosing k: the elbow method and the Silhouette score

**Part C: Technology stack**

14. Tools and libraries used, and why

**Part D: The project**

15. Literature review
16. Dataset
17. Methodology (pipeline)
18. Notebook walkthrough with code
19. Results
20. Business actions
21. Discussion
22. Ethics and privacy
23. Limitations and future scope
24. Conclusion

**Part E: Running, maintaining and presenting**

25. Project files and folder map
26. How to run on Colab
27. How to rebuild and change things
28. Known issues and fixes
29. Presentation script (5 slides)
30. Live demo runbook
31. Viva questions and answers
32. Project timeline and status
33. References

---

# PART A: OVERVIEW

## 1. Abstract

An online cosmetics shop logged **20.7 million clickstream events (2.27 GB)** over five months, but it markets to every buyer in the same way. This project uses **Apache Spark** to process the full log:

- It cleans the data and explores it with **Spark SQL**.
- It computes **Recency, Frequency and Monetary (RFM)** values for each of **110,518 buyers**.
- It clusters the buyers with **Spark MLlib K-Means**.

The elbow method and the Silhouette score point to **k = 4** (Silhouette 0.49). The four segments are **Champions, New / Promising, At-Risk and Lost**.

The key finding is that **Champions are under 10% of buyers but earn 34.7% of revenue**. **At-Risk** buyers have not purchased for about 3 months, yet they hold **43.5% of revenue**, so a win-back campaign aimed at them is the highest-value action.

## 2. Problem statement and objectives

### 2.1 Problem
E-commerce sites collect huge event logs: every product view, add-to-cart, removal and purchase. Sending one marketing message to all customers wastes budget. Loyal customers get discounts they don't need, and lapsing customers are ignored. The shop needs to **group customers by behaviour** so that each group gets a suitable action.

### 2.2 Objectives
1. Ingest and clean the full 2.27 GB log with Spark DataFrames.
2. Explore it with Spark SQL: the event funnel, monthly revenue and top brands.
3. Engineer RFM features for each buyer with distributed aggregations.
4. Cluster buyers with MLlib K-Means, choosing k with the elbow and Silhouette methods.
5. Turn the clusters into named segments with marketing actions.

### 2.3 Constraints that shaped the project
- **Data size rule:** the course requires a dataset of 1–4 GB. The first choice, the UCI Online Retail dataset (48 MB), was too small, so on 12 Nov the project switched to the 2.27 GB REES46 cosmetics dataset.
- **Environment:** Google Colab with `pip install pyspark`. No local Hadoop or Windows Spark install.
- **Time budget:** 3 sessions of about 2 hours each, so the scope is deliberately small.

## 3. Syllabus and course-outcome mapping

| Unit | Syllabus topic | Where it appears in the project |
|---|---|---|
| 1: Intro to Big Data | Volume, Velocity, Variety | 2.27 GB / 20.7M-row clickstream log in 5 files (§4, §16) |
| 2: Frameworks | Distributed processing, batch processing, Hadoop/MapReduce | Spark runs the batch job over parallel partitions (§5, §6) |
| 3: NoSQL | Data models beyond relational tables | Discussed as context (§5.4); the data is file-based, so no NoSQL store is needed |
| 4: Apache Spark | SparkSession, DataFrames, transformations, actions, lazy evaluation, caching, Spark SQL | Loading, cleaning, RFM aggregation, SQL queries (§6, §7, §17) |
| 5: Analytics techniques | Preprocessing, scalable ML, clustering, MLlib | VectorAssembler, StandardScaler, KMeans, ClusteringEvaluator (§8, §11–13) |
| 6: Applications | Big Data in e-commerce, privacy and ethics | Business use case and actions, anonymised data (§20, §22) |

Course outcomes covered: **CO1, CO2, CO4, CO5**.

---

# PART B: THEORY

## 4. Big Data fundamentals

### 4.1 What "Big Data" means
Big Data is data whose size, speed or variety is too much for traditional single-machine tools (one spreadsheet, one database server, one Python process) to store and process in reasonable time. The answer is to **distribute** both storage and computation across many machines, or across many cores, and process the pieces in parallel.

### 4.2 The V's of Big Data

| V | Meaning | In this project |
|---|---|---|
| **Volume** | The amount of data | 2.27 GB of CSV, 20,692,840 rows, 5 files. In pandas this would need roughly 10 GB of RAM, which is too much for comfortable single-threaded work. |
| **Velocity** | How fast new data arrives | Clickstream events arrive continuously, about 4 million a month here. |
| **Variety** | Different formats and structures | Semi-structured events: missing `category_code`/`brand`, four event types, timestamps stored as text with a `" UTC"` suffix. |
| **Veracity** | How trustworthy the data is | 5.8% of rows were dropped as nulls, zero prices or exact duplicates. |
| **Value** | The business use you can get from it | Four customer segments, each with a marketing action. |

### 4.3 Types of data
- **Structured:** fixed schema, rows and columns (SQL tables).
- **Semi-structured:** has some structure but is flexible (JSON, logs, CSV with missing fields). This dataset is here.
- **Unstructured:** no schema (images, free text, video).

### 4.4 Batch vs stream processing
- **Batch:** process a large, fixed dataset in one job (this project: 5 months of history).
- **Stream:** process events continuously as they arrive (future scope: Spark Structured Streaming).

### 4.5 Clickstream analytics
A clickstream is the log of every action a user takes on a website. E-commerce clickstreams usually record `view → cart → (remove_from_cart) → purchase`. From this you can build the **conversion funnel** (what share of viewers buy) and customer-level features such as RFM.

## 5. Hadoop, MapReduce and why Spark replaced it for this job

### 5.1 Hadoop ecosystem
- **HDFS (Hadoop Distributed File System):** splits files into large blocks (128 MB by default), stores them across many machines and keeps 3 copies of each block for fault tolerance.
- **YARN:** the cluster resource manager. It decides which job gets which CPU and memory.
- **MapReduce:** the original programming model for processing data stored in HDFS.

### 5.2 MapReduce model
1. **Map:** each mapper reads one input split and emits `(key, value)` pairs, for example `(user_id, price)`.
2. **Shuffle and sort:** the framework groups all values with the same key and sends them to the same reducer.
3. **Reduce:** each reducer aggregates the values per key, for example `sum(price)` per user.

RFM in MapReduce would be: map each purchase to `(user_id, (date, session, price))`, then reduce per user to `(max date, distinct sessions, sum price)`. Spark does this with one `groupBy().agg()`.

### 5.3 Why Spark instead of MapReduce

| | Hadoop MapReduce | Apache Spark |
|---|---|---|
| Intermediate data | Written to disk after every job | Kept in memory where possible |
| Iterative algorithms (like K-Means) | Every iteration is a new job that re-reads from disk, so it is slow | Data is cached once and reused in every iteration |
| API | Low-level Java map/reduce functions | High-level DataFrames, SQL, ML, in Python/Scala/Java/R |
| Execution | Fixed two-stage map → reduce | General DAG of many stages, optimised as a whole |
| Speed | Baseline | Often 10–100× faster for iterative and interactive workloads |

K-Means runs many passes over the same data (every iteration, for every k from 2 to 8, plus the Silhouette passes). That is exactly the kind of iterative workload where Spark's in-memory caching is a large advantage.

### 5.4 Where NoSQL fits (Unit 3 context)
NoSQL databases (key-value like Redis, document like MongoDB, column-family like Cassandra/HBase, graph like Neo4j) are built for fast reads and writes of individual records at scale, with flexible schemas. This project is a **batch analytics job over files**, so a distributed processing engine (Spark) is the right tool and a NoSQL store is not needed. In production, the final `customer_segments` table would often be written to a NoSQL store, such as Cassandra or a key-value cache, so that a website can look up a user's segment in milliseconds.

## 6. Apache Spark in depth

### 6.1 What Spark is
Apache Spark is an open-source, distributed, in-memory data processing engine. It splits data into **partitions** and processes them **in parallel**, on the cores of one machine or across many machines. It provides one engine for SQL, batch processing, streaming, machine learning (MLlib) and graph processing.

### 6.2 Architecture

```
            ┌─────────────────────────────┐
            │      Driver program         │  your notebook: SparkSession,
            │  (DAG scheduler, Catalyst)  │  builds the plan, schedules tasks
            └──────────────┬──────────────┘
                           │
                 ┌─────────▼─────────┐
                 │  Cluster manager  │  local / Standalone / YARN / Kubernetes
                 └─────────┬─────────┘
          ┌────────────────┼────────────────┐
   ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
   │  Executor   │  │  Executor   │  │  Executor   │  run tasks on partitions,
   │ tasks+cache │  │ tasks+cache │  │ tasks+cache │  hold cached data in memory
   └─────────────┘  └─────────────┘  └─────────────┘
```

- **Driver:** runs the main program, creates the `SparkSession`, turns your code into a plan and splits it into **jobs → stages → tasks**.
- **Cluster manager:** gives the application resources. Here it is **local mode**: `master("local[*]")` uses every core of the Colab VM (2 on free Colab) as executor threads in a single JVM. The same code runs unchanged on YARN or Kubernetes.
- **Executors:** worker processes that run tasks (one task per partition) and store cached partitions.
- **Job:** started by each action. **Stage:** a set of tasks that can run without a shuffle. **Task:** one unit of work on one partition.

### 6.3 SparkSession
`SparkSession` is the single entry point to Spark. It is used to read data, run SQL, and create DataFrames. In this project:

```python
spark = (SparkSession.builder
         .appName("BDA-Customer-Segmentation-RFM")
         .master("local[*]")                              # all local cores
         .config("spark.driver.memory", "6g")             # 4g if Colab crashes
         .config("spark.sql.shuffle.partitions", "32")    # default 200 is too many for 2 cores
         .getOrCreate())
```

### 6.4 RDD: Resilient Distributed Dataset
- The low-level data structure underneath every DataFrame.
- **Immutable:** you never change an RDD, you create a new one from it.
- **Partitioned:** split into chunks that are processed in parallel.
- **Lineage (fault tolerance):** each RDD remembers the steps that built it. If an executor dies and a partition is lost, Spark **recomputes** that partition from the lineage instead of keeping replica copies. That is the "resilient" in the name.

### 6.5 DataFrame
A DataFrame is a distributed table with named, typed columns (like a SQL table or a pandas DataFrame, but split across partitions). It is built on RDDs, but because Spark knows the schema it can optimise queries far better than with raw RDDs. The whole project uses the DataFrame API.

### 6.6 Transformations vs actions, and lazy evaluation

| Transformations (lazy, return a new DataFrame) | Actions (run the job, return a result) |
|---|---|
| `filter`, `withColumn`, `select`, `dropDuplicates`, `groupBy().agg()`, `join` | `count()`, `show()`, `first()`, `collect()`, `toPandas()`, `write` |

- Transformations only record a **plan** (a DAG, a directed acyclic graph of operations). Nothing is read or computed yet.
- Work starts only when an **action** is called. Spark then optimises the whole plan at once. For example, it pushes filters down to the file scan and reads only the columns it needs.
- `events.explain()` in the notebook prints the physical plan **before** anything has run. That is the evidence of lazy evaluation.

### 6.7 Narrow vs wide transformations and the shuffle
- **Narrow** (`filter`, `withColumn`, `select`): each output partition depends on one input partition. No data moves between partitions, so they are fast and are combined into one stage.
- **Wide** (`groupBy`, `dropDuplicates`, `join`, `orderBy`, `countDistinct`): rows with the same key must end up in the same partition, so data is **shuffled** across the network or between cores. A shuffle is a stage boundary and the most expensive operation in Spark.
- In `explain()` output a shuffle appears as `Exchange hashpartitioning(...)`.
- `spark.sql.shuffle.partitions` sets how many partitions a shuffle produces. It is 32 here, because the default of 200 creates many tiny tasks on 2 cores.

### 6.8 Partitions and parallelism
The 5 CSVs are split into input partitions: **19 on Colab and 25 locally**. The number depends on the core count and file split size. Each partition is one task; with 2 cores, 2 tasks run at the same time.

### 6.9 Caching and persistence
- `.cache()` keeps a DataFrame in memory (spilling to disk if needed) **after the first action computes it**.
- The cleaned `events` DataFrame (19.5M rows) is reused by every SQL query and by the RFM step. Without the cache, Spark would re-read and re-clean 2.27 GB every time.
- `rfm`, `data` and `segmented` are also cached, because the K-Means loop passes over `data` many times.

### 6.10 Broadcast join
When one side of a join is tiny (here, the 4-row cluster-to-name lookup table), `F.broadcast(small_df)` copies it to every executor. Each partition of the big table joins locally, so no shuffle is needed.

### 6.11 Explicit schema vs `inferSchema`
`inferSchema=True` makes Spark read the whole dataset once just to guess column types: an extra full pass over 2.27 GB. An explicit `StructType` skips that pass and also guarantees the correct types (for example, `user_id` as `long`).

## 7. Spark SQL and the Catalyst optimiser

- `df.createOrReplaceTempView("events")` registers a DataFrame as a temporary SQL table for the session.
- `spark.sql("SELECT ...")` runs standard SQL and returns a DataFrame.
- SQL queries and DataFrame code go through the same **Catalyst optimiser**, so they perform the same:
  1. **Analysis:** resolves column and table names against the schema.
  2. **Logical optimisation:** rule-based rewrites such as predicate pushdown (filter early), column pruning (read only needed columns) and constant folding.
  3. **Physical planning:** chooses join strategies (for example, broadcast vs sort-merge) and aggregation methods.
  4. **Code generation (Tungsten):** compiles the plan into efficient JVM bytecode and uses compact off-heap memory.

## 8. Spark MLlib

MLlib is Spark's scalable machine-learning library. The DataFrame-based API (`pyspark.ml`) is built on a few ideas:

| Concept | Meaning | In this project |
|---|---|---|
| **Transformer** | Has `.transform(df)` and adds columns | `VectorAssembler`, a fitted `StandardScalerModel`, a fitted `KMeansModel` |
| **Estimator** | Has `.fit(df)` and returns a Transformer (a model) | `StandardScaler`, `KMeans` |
| **Evaluator** | Scores a model's output | `ClusteringEvaluator` (Silhouette) |
| **Feature vector** | MLlib models expect all features in one vector column | `VectorAssembler(["R_log","F_log","M_log"]) → features_raw` |

Because the scaler and model are fitted objects, a **new, unseen customer** can be passed through exactly the same steps (log1p → assemble → scale → K-Means) to get a segment. Live demo cell 3 shows this.

MLlib's K-Means is distributed: in each iteration, every partition assigns its points to the nearest centre and computes partial sums. Only those small partial sums are sent to the driver to compute the new centres.

## 9. Customer segmentation

**Customer segmentation** divides a customer base into groups that behave similarly, so each group can be treated differently. Common bases:

| Basis | Examples |
|---|---|
| Demographic | Age, gender, income |
| Geographic | Country, city |
| Psychographic | Lifestyle, values |
| **Behavioural** | Purchase recency, frequency, spend, browsing. **This project.** |

Behavioural segmentation is the most useful for e-commerce, because it uses what customers actually do and needs no personal data. Approaches range from **rule-based** (for example, RFM score bins such as "R=5, F=5, M=5 → Champions") to **machine-learning** (clustering, which finds the groups from the data itself). This project uses the ML approach on RFM features, then gives the clusters business names.

## 10. The RFM model

Proposed in direct marketing and popularised by **Hughes (1994)**, RFM rests on one observation: **customers who bought recently, buy often and spend more are the most valuable and the most likely to respond.**

| Letter | Meaning | Better when | Definition in this project |
|---|---|---|---|
| **R: Recency** | How recently they bought | **Lower** | Days from the last purchase to the reference date 2020-03-01 |
| **F: Frequency** | How often they buy | Higher | Number of distinct purchase sessions (orders) |
| **M: Monetary** | How much they spend | Higher | Sum of `price` over all their purchase rows |

**Reference date:** the day after the last event in the data (2020-02-29 + 1 day = **2020-03-01**). This makes every recency a positive number of days.

**Classic RFM scoring vs clustering:**
- *Classic:* split each feature into quintiles (scores 1–5) and combine them into codes like "555". That is simple, but the cut-offs are arbitrary and the 125 combinations are hard to act on.
- *Clustering (used here):* let K-Means find natural groups in the 3-D RFM space, then name them. This is data-driven and gives a small number of actionable segments.

**Typical RFM segment personas:** Champions (recent, frequent, high spend), Loyal, New / Promising (very recent, few orders), At-Risk (used to spend well, but not recently), Hibernating / Lost (long ago, low value).

## 11. Clustering and the K-Means algorithm

### 11.1 Clustering
Clustering is **unsupervised learning**: there are no labels. The algorithm groups points so that points in the same cluster are similar and points in different clusters are not. Main families:

| Family | Example | Notes |
|---|---|---|
| Partitioning | **K-Means**, K-Medoids | Fast, scalable; needs k; assumes round, similar-size clusters |
| Hierarchical | Agglomerative, **Bisecting K-Means** | Builds a tree; bisecting K-Means is in MLlib |
| Density-based | DBSCAN | Finds arbitrary shapes and noise; hard to scale |
| Model-based | **Gaussian Mixture Model** | Soft (probabilistic) membership; in MLlib |

### 11.2 K-Means objective
Given n points and a chosen k, K-Means finds k centres μ₁…μ_k that minimise the **within-cluster sum of squared errors (WSSSE)**:

```
WSSSE = Σ_{j=1..k}  Σ_{x ∈ C_j}  ‖ x − μ_j ‖²
```

### 11.3 Lloyd's algorithm
1. **Initialise** k centres.
2. **Assign** each point to its nearest centre (Euclidean distance).
3. **Update** each centre to the mean of the points assigned to it.
4. **Repeat** steps 2–3 until the assignments stop changing or `maxIter` (50 here) is reached.

Every step can only lower WSSSE, so the algorithm always converges, but possibly to a **local** minimum that depends on the starting centres.

### 11.4 Initialisation: k-means++ and k-means||
- Random starting centres can give poor results.
- **k-means++** picks each new centre with probability proportional to its squared distance from the nearest centre already chosen, which spreads centres out.
- **k-means||** (Bahmani et al., 2012; Spark's default) is a parallel version that samples many candidates in a few passes, so it scales to distributed data.
- **`seed=42`** fixes the random choices so the result is reproducible.

### 11.5 Strengths and weaknesses
- **Strengths:** simple, fast (roughly linear in n), easy to parallelise, easy-to-explain centres.
- **Weaknesses:** you must choose k; it is sensitive to feature scale (§12) and outliers; it assumes compact, roughly spherical clusters; results can depend on initialisation.

## 12. Feature scaling and transformation

K-Means uses Euclidean distance, so the feature with the largest numeric range dominates. Here Monetary ranges from $0.13 to $3,769, Frequency from 1 to 122 (median 1), and Recency from about 1 to 152 days. Without scaling, Monetary would decide every cluster.

**Step 1: log transform** (`log1p(x) = ln(1 + x)`)
- All three features are strongly **right-skewed** (most buyers order once and spend little; a few spend a lot).
- The log compresses the long tail so outliers don't pull the centres. `log1p` is used instead of `log` so a value of 0 is safe.

**Step 2: standardisation** (`StandardScaler(withMean=True, withStd=True)`)

```
z = (x − mean) / standard deviation
```

Every feature then has mean 0 and standard deviation 1, so all three contribute equally to the distance.

## 13. Choosing k: the elbow method and the Silhouette score

### 13.1 Elbow method (WSSSE)
WSSSE always falls as k grows (with k = n it would be 0), so you can't simply minimise it. Plot WSSSE against k and look for the **elbow**: the point after which adding clusters gives only small improvements. In MLlib this is `model.summary.trainingCost`.

### 13.2 Silhouette score
For each point i:
- **a(i)** = mean distance from i to the other points in its own cluster (cohesion)
- **b(i)** = mean distance from i to the points in the **nearest other** cluster (separation)

```
s(i) = ( b(i) − a(i) ) / max( a(i), b(i) )        range −1 … +1
```

| Value | Meaning |
|---|---|
| close to +1 | well inside its own cluster, far from others |
| about 0 | on the border between two clusters |
| negative | probably in the wrong cluster |

The overall score is the mean of s(i). As a rough guide, above 0.5 is strong structure, 0.25–0.5 is reasonable, and below 0.25 is weak. `ClusteringEvaluator(metricName="silhouette", distanceMeasure="squaredEuclidean")` computes it in Spark.

### 13.3 Using both
The elbow shows where extra clusters stop paying off. Silhouette shows how well separated the clusters are. In practice you also consider **business usefulness** (can each segment get a distinct action?) and **stability** (do you get the same clusters if you re-run?). This project uses all four criteria (§17.6).

---

# PART C: TECHNOLOGY STACK

## 14. Tools and libraries used, and why

| Technology | Version / detail | Role in the project | Why it was chosen |
|---|---|---|---|
| **Google Colab** | Free tier, 2 vCPU, about 12 GB RAM | Runs the notebook | Free, browser-based, no local setup; Python and Java are preinstalled |
| **Python** | 3.x | Language for everything | Standard for data science; PySpark API |
| **Apache Spark / PySpark** | 4.0.5 on Colab, 3.5.3 locally | Distributed processing engine | Scales to data larger than memory; syllabus Unit 4 |
| **Spark SQL** | built in | EDA queries on the `events` and `segments` views | Declarative SQL, same optimiser as DataFrames |
| **Spark MLlib** (`pyspark.ml`) | built in | VectorAssembler, StandardScaler, KMeans, ClusteringEvaluator | Distributed ML; syllabus Unit 5 |
| **Kaggle API / CLI** | `kaggle>=1.8` | Downloads the dataset inside the notebook | Reproducible download; token kept in Colab Secrets (`KAGGLE_API_TOKEN`) |
| **pandas** | — | Small aggregated results only (charts, 4-row naming table, CSV export) | Convenient for plotting; never used on the 20M-row data |
| **matplotlib** | — | All 8 charts in `outputs/` | Standard plotting library |
| **Jupyter notebook** | `.ipynb` | The deliverable | Code, outputs and explanation in one file |
| **nbformat** | — | `tools/build_nb.py` generates the notebook | Notebook kept as reproducible code |
| **python-pptx** | — | `tools/build_deck.py` builds the 5-slide deck from `results.json` | Slide numbers always match the notebook output |
| **Git** | — | Version control | Tracks changes |

**Rule followed throughout:** every processing and modelling step uses Spark (DataFrame API, Spark SQL, MLlib). pandas and matplotlib are used **only** to plot small, already-aggregated results. `toPandas()` is called only on small outputs (such as the funnel, months, brands and the 110k-row RFM table) and never on the 19.5M-row event table.

---

# PART D: THE PROJECT

## 15. Literature review

| Work | Contribution | How this project uses it |
|---|---|---|
| Hughes (1994), *Strategic Database Marketing* | Introduced the **RFM model**: customers who bought recently, buy often and spend more are worth more | The three features |
| MacQueen (1967); Wu et al. (2008), "Top 10 algorithms in data mining" | **K-Means** partitions points into k groups by minimising within-cluster squared distance; one of the top 10 data-mining algorithms | The clustering algorithm |
| Chen, Sain & Guo (2012), *J. Database Marketing & Customer Strategy Mgmt.* | Segmented an online retailer's customers with RFM + K-Means (the original paper on the UCI Online Retail dataset) | The same method, on a dataset about 40 times larger (a clickstream instead of invoices) |
| Zaharia et al. (2016), "Apache Spark: a unified engine for big data processing", *CACM* | In-memory, fault-tolerant distributed processing using RDDs and lineage | The engine |
| Meng et al. (2016), "MLlib: Machine Learning in Apache Spark", *JMLR* | Scalable ML pipelines on Spark, including k-means\|\| initialisation | VectorAssembler, StandardScaler, KMeans, ClusteringEvaluator |
| Rousseeuw (1987), "Silhouettes", *J. Comp. & Applied Math.* | The Silhouette coefficient for checking cluster quality | Choosing k |

## 16. Dataset

**eCommerce Events History in Cosmetics Shop** (REES46 Open CDP), Kaggle `mkechinov/ecommerce-events-history-in-cosmetics-shop`.

| Property | Value |
|---|---|
| Files | `2019-Oct.csv`, `2019-Nov.csv`, `2019-Dec.csv`, `2020-Jan.csv`, `2020-Feb.csv` |
| Size | **2.27 GB** (CSV), about 0.5 GB zipped. Meets the 1–4 GB rule |
| Rows | **20,692,840** events |
| Period | 1 Oct 2019 – 29 Feb 2020 |
| Local path | `data/cosmetics/` (the notebook downloads it automatically on Colab) |

| Column | Type | Meaning |
|---|---|---|
| `event_time` | string → timestamp | When the event happened, e.g. `2019-10-01 00:00:00 UTC` |
| `event_type` | string | `view`, `cart`, `remove_from_cart` or `purchase` |
| `product_id` | long | Product |
| `category_id` | long | Category ID |
| `category_code` | string | Category name (often missing) |
| `brand` | string | Brand (sometimes missing) |
| `price` | double | Product price in $ |
| `user_id` | long | Anonymous numeric user ID |
| `user_session` | string | Session ID. **One purchase session = one order** |

Users are anonymised as numeric IDs, so the data contains no personal information. The earlier UCI Online Retail dataset (48 MB) was dropped because it was too small; its files in `data/` are no longer used.

## 17. Methodology (pipeline)

```
5 CSVs (2.27 GB)
   │  spark.read.csv(files, schema=…)             ← explicit StructType, no inferSchema pass
   ▼
raw DataFrame (20.69M rows, 19 partitions on Colab)
   │  to_timestamp · filter nulls · price > 0 · dropDuplicates · cache
   ▼
events (19.49M rows, cached) ──► Spark SQL view "events": overview, funnel, monthly revenue, top brands
   │  filter event_type = 'purchase'  (1.29M rows)
   │  groupBy(user_id).agg(datediff(max date), countDistinct(session), sum(price))
   ▼
RFM table (110,518 buyers)
   │  log1p → VectorAssembler → StandardScaler(withMean, withStd)
   ▼
feature vectors (cached)
   │  KMeans(k = 2..8) + ClusteringEvaluator (Silhouette)  →  choose k = 4
   ▼
KMeans(k=4, seed=42) → cluster profiles → naming rule → broadcast join → customer_segments.csv
```

### 17.1 Ingestion
- All five files are read into **one DataFrame** with an **explicit schema**, which avoids an extra `inferSchema` pass over 2.27 GB.
- Spark splits the input into partitions (19 on Colab, 25 locally) that are processed in parallel. A full scan took about 14 s on Colab.

### 17.2 Cleaning

| Rule | Reason |
|---|---|
| Parse `event_time` (strip `" UTC"`, `to_timestamp`) | Needed for date arithmetic |
| Drop null `user_id` / `user_session` | Can't attribute the event to a customer or an order |
| Drop `price <= 0` | Free samples or bad data that would distort Monetary |
| `dropDuplicates()` | The raw log contains repeated identical rows. A **wide** transformation (shuffle) |
| Keep only the 7 needed columns | Less memory in the cache |

Result: **19,493,208 rows**; **1,199,632 rows (5.8%)** removed. All of these are **lazy transformations**. `events.explain()` shows `Exchange hashpartitioning` (the shuffle) before anything runs; then the `count()` **action** runs the plan (about 158 s on Colab). The result is **cached** because every later step reuses it.

### 17.3 Exploratory analysis (Spark SQL)
The cleaned DataFrame is registered as the temp view `events`. Queries:
- Overview: distinct users, sessions, products, brands, date range
- Event funnel by `event_type` (events and distinct users)
- Monthly purchase revenue, buyers and orders
- Top 10 brands by purchase revenue

### 17.4 RFM feature engineering
Only `purchase` events are used (1,285,982 rows). One purchase row is one product, and **one order is one `user_session`**.

| Feature | Definition |
|---|---|
| Reference date | last event date + 1 day = **2020-03-01** |
| Recency | `datediff(ref_date, max(purchase date))`, in days (lower is better) |
| Frequency | `countDistinct(user_session)`, the number of orders |
| Monetary | `sum(price)`, in $ |

### 17.5 Scaling
All three features are very right-skewed (most buyers order once; Frequency 1–122; Monetary $0.13–$3,769):
1. `log1p` reduces the skew.
2. `VectorAssembler` combines `[R_log, F_log, M_log]` into one vector.
3. `StandardScaler(withMean=True, withStd=True)` gives each feature mean 0 and standard deviation 1.

### 17.6 Choosing k
K-Means was trained for k = 2..8 (`seed=42`, `maxIter=50`) and scored with WSSSE and Silhouette.

| k | WSSSE (Colab, Spark 4.0) | Silhouette (Colab) | Silhouette (local, Spark 3.5) |
|---|---|---|---|
| 2 | 209,251 | **0.662** | 0.662 |
| 3 | 153,483 | 0.548 | 0.548 |
| **4** | **117,250** | **0.490** | **0.490** |
| 5 | 96,295 | 0.515 | 0.452 |
| 6 | 86,820 | 0.517 | 0.504 |
| 7 | 77,242 | 0.503 | 0.472 |
| 8 | 71,617 | 0.512 | 0.507 |

**Decision: k = 4**, for four reasons:
1. **Business usefulness:** k = 2 has the best Silhouette but only splits buyers into "good" and "bad", which is too coarse to act on.
2. **Elbow:** WSSSE falls by 56k (2→3) and 36k (3→4), then only 21k (4→5) and under 10k per step after that.
3. **Stability:** k = 2, 3 and 4 give identical clusters on Spark 3.5 and 4.0 (same WSSSE and Silhouette). For k ≥ 5 the results change between versions (k = 5: 0.45 locally vs 0.52 on Colab), so those solutions depend on the random starting centres and aren't reliable. The small Silhouette gain at k = 5–8 on Colab (0.50–0.52 vs 0.49) is not trustworthy.
4. **Quality:** Silhouette 0.49 indicates reasonable structure, and the four clusters correspond to four distinct personas.

### 17.7 Naming the clusters
A deterministic, explainable rule on the cluster averages:
1. Highest average Frequency → **Champions**
2. Of the rest, lowest average Recency → **New / Promising**
3. Of the last two, higher average Monetary → **At-Risk**; the other → **Lost**

The names are attached to all 110,518 buyers with a **broadcast join**, and the result is exported to `outputs/customer_segments.csv`.

## 18. Notebook walkthrough with code

The notebook `BDA_Customer_Segmentation.ipynb` is **generated** by `tools/build_nb.py`, so make changes there, not in the notebook.

| § | What the cell does | Key Spark idea | Output to look for |
|---|---|---|---|
| 0 | Detects Colab, installs `pyspark` and `kaggle>=1.8`, creates the SparkSession (6 GB driver memory, 32 shuffle partitions), sets the chart palette | SparkSession, local mode | `Spark version`, `Default parallelism` (2 on Colab) |
| 1a | If `data/cosmetics/*.csv` is missing, downloads it with the Kaggle CLI using the `KAGGLE_API_TOKEN` secret; prints file sizes | — | 5 files, **2.27 GB** |
| 1b | Explicit `StructType` → `spark.read.csv(files, ...)` → `count()` | Explicit schema, action | **20,692,840** rows |
| 2 | Clean → `.cache()` → `explain()` → `count()` | Lazy evaluation, wide transformation, cache | `Exchange hashpartitioning`; **19,493,208** rows |
| 3a | Temp view `events`; overview query | Spark SQL | 1,637,770 users, 4,520,284 sessions |
| 3b | Funnel, monthly revenue and top-brand queries | `GROUP BY`, `COUNT(DISTINCT)` | 110,518 purchasing users |
| 3c | `toPandas()` on the small aggregated results → 3 charts | Only small results go to the driver | `eda_*.png` |
| 4 | Filter purchases → `groupBy(user_id).agg(...)` → RFM, cached | Distributed aggregation | **110,518** buyers; summary statistics |
| 4b | `log1p` columns + histograms | — | `rfm_distributions.png` |
| 5 | `VectorAssembler` → `StandardScaler` → `data` (cached) | MLlib feature pipeline | `features` vectors |
| 6 | Loop over k = 2..8: KMeans + Silhouette | MLlib, `ClusteringEvaluator` | 7 printed lines; slowest step (several minutes on Colab) |
| 6b | Elbow and Silhouette chart, `K_FINAL = 4` | — | `k_selection.png` |
| 7 | Final `KMeans(k=4, seed=42)` → `groupBy(Cluster)` profile | — | Silhouette 0.4895 and the profile table |
| 7b | Naming rule (pandas on 4 rows) | — | Segment table |
| 7c | Broadcast join of names → `customer_segments.csv`, plus charts | `F.broadcast` (no shuffle) | `segment_sizes.png`, scatter plots |
| 8 | Business actions table; saves `outputs/results.json` | — | JSON printout |
| 9 | **Live demo cells:** SQL on segments, look up one buyer, score a new customer | Reusing cached data and a trained model | See §30 |
| — | `# spark.stop()` commented out so the session stays alive for the demo | — | — |

### Key code

**Schema and ingestion**
```python
schema = StructType([
    StructField("event_time", StringType()),   StructField("event_type", StringType()),
    StructField("product_id", LongType()),     StructField("category_id", LongType()),
    StructField("category_code", StringType()), StructField("brand", StringType()),
    StructField("price", DoubleType()),        StructField("user_id", LongType()),
    StructField("user_session", StringType()),
])
raw = spark.read.csv(files, header=True, schema=schema)   # all 5 monthly files
raw_count = raw.count()                                    # action: the 2.3 GB scan happens here
```

**Cleaning (lazy until `count()`)**
```python
events = (raw
          .withColumn("event_time", F.to_timestamp(F.regexp_replace("event_time", " UTC", "")))
          .filter(F.col("user_id").isNotNull() & F.col("user_session").isNotNull())
          .filter(F.col("price") > 0)
          .dropDuplicates()
          .select("event_time", "event_type", "product_id", "brand", "price", "user_id", "user_session")
          .cache())
events.explain()          # plan printed, nothing has run yet
clean_count = events.count()
```

**Spark SQL EDA (funnel example)**
```python
events.createOrReplaceTempView("events")
spark.sql("""
    SELECT event_type, COUNT(*) AS events, COUNT(DISTINCT user_id) AS users
    FROM events GROUP BY event_type ORDER BY events DESC""").show()
```

**RFM**
```python
purchases = events.filter(F.col("event_type") == "purchase")
ref_date = events.agg(F.date_add(F.max(F.to_date("event_time")), 1).alias("ref")).first().ref
rfm = (purchases.groupBy("user_id")
       .agg(F.datediff(F.lit(ref_date), F.max(F.to_date("event_time"))).alias("Recency"),
            F.countDistinct("user_session").alias("Frequency"),
            F.round(F.sum("price"), 2).alias("Monetary"))
       .cache())
```

**Feature pipeline**
```python
rfm_log = (rfm.withColumn("R_log", F.log1p("Recency"))
              .withColumn("F_log", F.log1p("Frequency"))
              .withColumn("M_log", F.log1p("Monetary")))
assembler = VectorAssembler(inputCols=["R_log", "F_log", "M_log"], outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
assembled = assembler.transform(rfm_log)
data = scaler.fit(assembled).transform(assembled).cache()
```

**Choosing k**
```python
evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette",
                                distanceMeasure="squaredEuclidean")
for k in range(2, 9):
    model = KMeans(k=k, seed=42, featuresCol="features", maxIter=50).fit(data)
    sil = evaluator.evaluate(model.transform(data))
    print(k, model.summary.trainingCost, sil)
```

**Final model, naming and broadcast join**
```python
final_model = KMeans(k=4, seed=42, featuresCol="features", maxIter=50).fit(data)
segmented = final_model.transform(data).withColumnRenamed("prediction", "Cluster").cache()
# names = {cluster_id: segment_name} from the naming rule on the 4-row profile
name_df = spark.createDataFrame([(int(c), n) for c, n in names.items()], ["Cluster", "Segment"])
customers = segmented.join(F.broadcast(name_df), "Cluster")
```

### Important variables (if you add cells)

| Name | What it is |
|---|---|
| `raw` | The DataFrame read from the 5 CSVs (not cached) |
| `events` | The cleaned, cached events. Also the SQL view `events` |
| `rfm` | One row per buyer: `user_id, Recency, Frequency, Monetary` (cached) |
| `rfm_log` | `rfm` plus `R_log, F_log, M_log` |
| `assembler`, `scaler` | The MLlib feature stages. `scaler_model` is fitted in live cell 3 |
| `data` | The scaled feature DataFrame used for training (cached) |
| `scores` | pandas table of k, WSSSE and Silhouette |
| `final_model` | The trained `KMeansModel` (k = 4) |
| `segmented` | `data` plus a `Cluster` column (cached) |
| `prof`, `names` | The profile table (pandas) and the cluster-number → segment-name mapping |
| `customers` | Every buyer with their `Segment` (Spark). Also the SQL view `segments` after live cell 1 |
| `RESULTS` | Dict of numbers saved to `outputs/results.json`, which feeds the slide deck |

## 19. Results

All results are from the Colab run (Spark 4.0.5, 13 Nov).

### 19.1 Processing statistics

| Metric | Value |
|---|---|
| Raw events | 20,692,840 (19 partitions, full scan about 14 s) |
| After cleaning | 19,493,208 (1,199,632 removed, 5.8%; clean + cache about 158 s) |
| Users / sessions | 1,637,770 / 4,520,284 |
| Products / brands | 53,903 / 273 |
| Date range | 2019-10-01 → 2020-02-29 |
| Purchase events | 1,285,982 |
| Purchase revenue | $6.35M ($6,348,268) |
| Peak month | Nov 2019 ($1.53M, the month with Black Friday) |
| Buyers with RFM | 110,518 |

### 19.2 Event funnel

| Stage | Events | Distinct users |
|---|---|---|
| view | 9,626,268 | 1,596,359 |
| cart | 5,612,289 | 397,390 |
| remove_from_cart | 2,968,669 | 183,002 |
| purchase | 1,285,982 | **110,518** |

**6.9%** of users who viewed a product made a purchase.

### 19.3 Segments (k = 4, Silhouette 0.4895)

| Segment | Buyers | Share | Avg Recency (days) | Avg Orders | Avg Spend | Revenue | Revenue share |
|---|---|---|---|---|---|---|---|
| Champions | 10,819 | 9.8% | 35.0 | 4.00 | $203.3 | $2.20M | **34.7%** |
| New / Promising | 18,732 | 16.9% | 14.5 | 1.16 | $39.0 | $0.73M | 11.5% |
| At-Risk | 39,800 | 36.0% | 87.5 | 1.22 | $69.4 | $2.76M | **43.5%** |
| Lost | 41,167 | 37.2% | 89.5 | 1.02 | $15.9 | $0.66M | 10.3% |

The local Spark 3.5 run gave the same four segments, with sizes within 50 buyers of these (for example 10,835 Champions) and the same revenue shares to one decimal place. The local outputs are kept in `outputs/local_spark35/`.

### 19.4 Output files (`outputs/`)

| File | Content |
|---|---|
| `eda_event_funnel.png` | View → cart → remove → purchase bar chart |
| `eda_monthly_revenue.png` | Monthly purchase revenue line chart |
| `eda_top_brands.png` | Top 10 brands by revenue |
| `rfm_distributions.png` | Histograms of R, F, M (99th percentile clipped) |
| `k_selection.png` | Elbow (WSSSE) and Silhouette panels, k = 4 marked |
| `segment_sizes.png` | Buyers and revenue per segment |
| `segments_scatter.png`, `segments_scatter_fm.png` | Recency vs Monetary and Frequency vs Monetary, coloured by segment (15k sample) |
| `customer_segments.csv` | All 110,518 buyers with R, F, M, cluster and segment |
| `results.json` | Every number used on the slides |

## 20. Business actions

| Segment | Who they are | Action |
|---|---|---|
| **Champions** | ~10% of buyers, about 4 orders, $203 average, bought about a month ago | VIP or loyalty tier, early access, referral rewards |
| **New / Promising** | Bought in the last 2 weeks, usually one order | Welcome email series and a second-order coupon |
| **At-Risk** | Spent well ($69 average) but gone about 3 months; 43.5% of revenue | **Personalised win-back offers using their favourite brands** (highest-value action) |
| **Lost** | One small ($16) old order | Newsletter only, no paid-ads spend |

## 21. Discussion

- **Pareto effect.** About 10% of buyers (Champions) bring in about 35% of revenue, which is consistent with the RFM literature.
- **At-Risk vs Lost.** Both segments are about 3 months inactive, but At-Risk buyers spent **4 times more** on average. Recency alone can't separate them; clustering on all three features can.
- **Very low repeat rate.** The median Frequency is 1. This is typical of cosmetics e-commerce, and it is why converting New buyers to a second order matters.
- **Why Spark.** A cached 19.5M-row DataFrame and distributed `groupBy` keep every step interactive on just 2 cores. The same code would run unchanged on a multi-node cluster.

## 22. Ethics and privacy

- The data is anonymised: numeric user IDs only, with no names, emails or addresses.
- In real use, behavioural tracking and targeted marketing need user consent (GDPR in Europe, the India DPDP Act 2023).
- The segments should be used to *serve* customers better, never to discriminate (for example, charging higher prices to loyal customers).
- Credentials: the Kaggle API token lives in Colab Secrets or `~/.kaggle/access_token`, never in the notebook, and should be expired after the project.

## 23. Limitations and future scope

**Limitations**
- A 5-month snapshot of a single shop, so seasonality beyond Black Friday isn't captured.
- The choice of k involves judgement, and K-Means assumes compact, round clusters.
- RFM ignores browsing, cart abandonment, product mix and returns.
- It runs on single-node Spark (Colab, 2 cores), not a real cluster.

**Future scope**
- **Spark Structured Streaming** to update segments as new events arrive.
- Behavioural features: views, cart-abandonment rate, brand affinity.
- Compare with **Bisecting K-Means** and **Gaussian Mixture** in MLlib.
- Deploy on a multi-node cluster (Databricks / EMR) with the full multi-category REES46 data (tens of GB).

## 24. Conclusion

Spark processed 2.27 GB of clickstream data end to end: ingestion, cleaning, SQL analytics, feature engineering and MLlib clustering. The result is four actionable customer segments. The single most valuable action is a win-back campaign for the **At-Risk** segment, which holds 43.5% of revenue.

---

# PART E: RUNNING, MAINTAINING AND PRESENTING

## 25. Project files and folder map

```
BDA-PROJECT/
├── BDA_Customer_Segmentation.ipynb              ← main notebook (GENERATED; edit tools/build_nb.py)
├── BDA_Customer_Segmentation_executed.ipynb     ← local Spark 3.5 run with all outputs (demo backup)
├── BDA_Customer_Segmentation_run-by-colab.ipynb ← copy downloaded from Colab
├── BDA_Customer_Segmentation_Presentation.pptx  ← 5-slide deck (GENERATED by tools/build_deck.py)
├── BDA_Customer_Segmentation_Presentation_v2.pptx
├── FINAL_PROJECT_DOCUMENTATION.md ← this file
├── PROJECT_REPORT.md      ← formal write-up
├── TECHNICAL_GUIDE.md     ← handover guide
├── DEMO_GUIDE.md          ← live-demo runbook
├── PRESENTATION_SCRIPT.md ← what to say + viva Q&A
├── PHASE_PLAN.md          ← original plan and status
├── README.md              ← quick start
├── CLAUDE.md              ← instructions for Claude Code
├── outputs/               ← charts, results.json, customer_segments.csv (Colab run)
│   └── local_spark35/     ← the same outputs from the local Spark 3.5 run
├── colab-run-files/       ← raw files downloaded from the Colab run
├── tools/
│   ├── build_nb.py        ← writes the notebook
│   ├── segment_text.py    ← k-choice text, naming rule, actions table (inserted into the notebook)
│   └── build_deck.py      ← writes the .pptx from outputs/results.json + charts
├── data/cosmetics/        ← the 5 CSVs (2.27 GB, not committed)
├── BDASyllabusAdaniUniversity.pdf
└── Big Data Analytics Project and Assignment (1).pdf
```

## 26. How to run on Colab

1. Go to https://colab.research.google.com → **File → Upload notebook** → `BDA_Customer_Segmentation.ipynb`.
2. Click the **🔑 Secrets** icon → **Add new secret**: name `KAGGLE_API_TOKEN`, value your Kaggle API token. Turn on **Notebook access**.
3. **Runtime → Run all.** The first run installs PySpark and downloads about 0.5 GB from Kaggle (unzips to 2.27 GB).
   - Free Colab has 2 cores, so expect about 10–15 minutes in total.
   - If the session crashes from lack of RAM: **Runtime → Restart session**, change `"spark.driver.memory", "6g"` to `"4g"`, and run all again.
4. Check the numbers: **20,692,840** raw → **19,493,208** clean → **110,518** buyers → k = 4, Silhouette ≈ **0.49**. (Verified on 13 Nov.)

## 27. How to rebuild and change things

| Task | Command (from the project folder) | Needs |
|---|---|---|
| Regenerate the notebook after changing `build_nb.py` | `cd tools && python build_nb.py` | `nbformat` |
| Rebuild the deck after new results | `python tools/build_deck.py` | `python-pptx`, `outputs/results.json` and PNGs |
| Rebuild the deck with a video on slide 4 | `python tools/build_deck.py demo.mp4` | as above |
| Re-run everything | Colab: upload notebook → add secret → Run all | Kaggle token |

**Workflow for a change:** edit `tools/build_nb.py` (or `tools/segment_text.py` for text/naming only) → regenerate the notebook and run it → copy the new `outputs/` (above all `results.json`) into the project → rebuild the deck. The slide numbers come from `results.json`, so the deck and notebook always agree.

**Common changes**
- **Different k:** change `K_FINAL = 4` in `build_nb.py`, and update `NAMING_CODE` in `segment_text.py`, because the naming rule assumes 4 clusters.
- **Less memory:** `"spark.driver.memory"` from `"6g"` to `"4g"`.
- **Faster tests on a subset:** in §1 use `files = files[:1]` to read one month.
- **Another dataset:** change the schema, cleaning rules and RFM definition (§1–4). Everything after `data` stays the same.

## 28. Known issues and fixes

| Problem | Cause | Fix |
|---|---|---|
| `SecretNotFoundError: KAGGLE_API_TOKEN` | Colab secret missing or misnamed | Add `KAGGLE_API_TOKEN` exactly, turn on Notebook access |
| Kaggle `404` / `401` | No token or expired token | Create a new token on Kaggle → Settings |
| Session crashes during cleaning | RAM limit (about 12 GB on free Colab) | Use 4g driver memory and restart |
| The k loop seems stuck | 14 passes (7 fits + 7 Silhouette) on 2 cores | Wait; each k prints a line when done |
| `NameError: spark` / `customers` after a disconnect | Colab runtime was recycled | Run all again (the data download is also lost if the VM was reset) |
| Spark on Windows hangs on a path with `*` | No Hadoop `winutils` | The notebook passes an explicit file list; prefer Colab |
| Numbers differ slightly between Colab and local | Spark 4.0 vs 3.5; K-Means initialisation can differ | All counts match exactly; k = 4 segment sizes differ by under 50 buyers |
| Partition count 19 on Colab, 25 locally | Depends on cores and file split size | Expected, not an error |

## 29. Presentation script (5 slides, 5 minutes)

The same text is in each slide's speaker notes in the `.pptx`. Speak it, don't read it.

### Slide 1: Introduction (≈ 50 s)
> Good morning. My project is **customer segmentation of e-commerce big data using PySpark K-Means**, reference topic 34.
> The problem: an online cosmetics shop records millions of clicks, but it markets to every buyer in the same way.
> The dataset is the **REES46 e-commerce events log** from Kaggle: **five monthly CSV files, 2.27 GB, 20.7 million events**, October 2019 to February 2020. Every row is a view, add-to-cart, remove or purchase.
> My objective is to use **Spark** to compute **Recency, Frequency and Monetary** values for every buyer, then use **MLlib K-Means** to group them into segments marketing can act on.

### Slide 2: Literature Review (≈ 50 s)
> - **Hughes, 1994**, introduced the **RFM model**: customers who bought recently, buy often and spend more are the most valuable.
> - **K-Means** (MacQueen, 1967) is one of the top-10 data-mining algorithms (Wu et al., 2008) and the usual choice for clustering RFM values.
> - **Chen, Sain and Guo, 2012**, segmented an online retailer's customers with RFM and K-Means. I apply the same idea to a clickstream log that is about 40 times larger.
> - **Spark and MLlib** (Zaharia et al., 2016; Meng et al., 2016) run this in memory and in parallel, which lets the method scale to gigabytes.

### Slide 3: Methodology (≈ 70 s)
> The whole pipeline runs in Spark.
> 1. **Ingest:** Spark reads all five CSVs into one DataFrame, split into partitions that are processed in parallel (19 on Colab). I give an explicit schema, so Spark doesn't need an extra pass over 2.3 GB to guess the types.
> 2. **Clean:** parse the timestamp and drop missing users, zero prices and duplicate events. That leaves **19.5 million events** (5.8% removed). Spark is **lazy**: these steps only build a plan, which runs at the `count()` action. The cleaned data is **cached** because every later step reuses it.
> 3. **EDA** with **Spark SQL**: the view → cart → purchase funnel, monthly revenue and top brands.
> 4. **RFM:** keep the 1.29 million purchase events; one `groupBy(user_id).agg(...)` gives **110,518 buyers**.
> 5. **Scale:** the values are very skewed, so I apply **log1p**, then **VectorAssembler** and **StandardScaler**.
> 6. **K-Means for k = 2 to 8.** Silhouette is highest at 2, but that is too coarse. The error curve bends at **4** with Silhouette 0.49, so the final model is **KMeans(k = 4, seed = 42)**.

### Slide 4: Working Model, live demo (≈ 90 s)
> *(Alt+Tab to Colab. Follow §30.)*
> "Let me show it running on Colab. Spark read all five files, 2.27 GB and 20.7 million events. Here is the lazy plan, and cleaning kept 19.5 million events. The Spark SQL funnel shows 1.6 million users viewed products but only 110 thousand bought, which is 6.9%. One groupBy builds the RFM table, and the K-Means loop for k from 2 to 8 gives the elbow at 4.
> Now live: *(Shift+Enter on LIVE 1)* a SQL query on the segments; Champions earn about a third of revenue. *(Shift+Enter on LIVE 3)* And the trained model scores a brand-new customer: recent, frequent big spenders are Champions; one small old order is Lost."
> *(Alt+Tab back to the slides.)*

### Slide 5: Conclusion (≈ 50 s)
> Spark K-Means found four segments among 110 thousand buyers:
> - **Champions**: under 10% of buyers, about 4 orders each, but **35% of revenue**. VIP tier and early access.
> - **New / Promising**: 17% who bought in the last 2 weeks. Welcome series and a second-order coupon.
> - **At-Risk**: 36% who spent well but haven't returned for about 3 months. They hold **43.5% of revenue**, so winning them back matters most.
> - **Lost**: 37% with one small $16 order. Newsletter only, no ad spend.
>
> On ethics, the data is anonymised (only numeric user IDs). **Limitations:** a 5-month snapshot and k chosen partly by judgement. **Future scope:** Spark Structured Streaming for live segments, and browsing features. Thank you.

## 30. Live demo runbook (slide 4, about 90 seconds)

**Main idea:** the full pipeline takes 10+ minutes on Colab, but the demo slot is about 90 seconds. So **run the whole notebook before the presentation** and keep the tab open. During the demo, scroll through the existing outputs, then run the **3 live cells** in section 9. Each takes a few seconds because the data is cached.

### 30.1 The day before
1. Upload the latest `BDA_Customer_Segmentation.ipynb` (it has section 9 and `spark.stop()` commented out).
2. Check that the `KAGGLE_API_TOKEN` secret exists with Notebook access on.
3. Run all once and check the numbers (§30.5).
4. Practise the demo twice with a timer.
5. On the presentation laptop: Colab logged in, browser zoom 125–150%, works on the projector resolution.
6. Make sure the backup opens offline: `BDA_Customer_Segmentation_executed.ipynb`, or just the slides.

### 30.2 On the day, 30–40 minutes before the slot
1. Colab → **Runtime → Run all** (re-downloads the data, about 1–2 min, then about 10 min of processing).
2. When done, check that section 9 printed tables with no errors and the RAM/Disk indicator is green.
3. **Keep the tab active.** Free Colab disconnects after about 90 minutes idle. Click a cell every 15–20 minutes, or run `customers.count()`.
4. Browser: Colab in one tab, slides in another window (**Alt+Tab**); collapse the sidebar; **hide the Secrets panel**; zoom to about 125%.
5. Pre-scroll so the file-size output of section 1 is visible first.
6. **Don't clear outputs.**

### 30.3 Click by click

| # | Time | Do this | Point at | Say |
|---|---|---|---|---|
| 1 | 0:00 | Top of section 1 output | `TOTAL 2.27 GB in 5 files`, `Raw events: 20,692,840` | "Spark read all five files, 2.27 GB and 20.7 million events, in one DataFrame." |
| 2 | 0:10 | Section 2 | `Exchange hashpartitioning`, then `Rows after cleaning: 19,493,208` | "This is the lazy plan: nothing ran until count(). The shuffle is the duplicate removal." |
| 3 | 0:25 | Section 3 | Funnel table | "Spark SQL: 1.6 million users viewed, only 110 thousand bought." |
| 4 | 0:35 | Section 4 | RFM table | "One groupBy gives Recency, Frequency and Monetary for 110,518 buyers." |
| 5 | 0:45 | Section 6 | k-loop lines and chart | "K-Means for k from 2 to 8; the elbow is at 4." |
| 6 | 0:55 | Section 9, **LIVE 1**, Shift+Enter | Segment table in 2–5 s | "Now live: a SQL query on the segments. Champions earn a third of revenue." |
| 7 | 1:10 | **LIVE 3**, Shift+Enter | Four predictions | "I can score a brand-new customer with the trained model." |
| 8 | 1:25 | Alt+Tab to slide 5 | — | "So, the conclusion…" |

**LIVE 2** (look up one real buyer) is optional. **Never press Ctrl+F9 (Run all)** during the demo. If a cell is slow, keep talking.

### 30.4 What the live cells print

**LIVE 1: Spark SQL on segments**
```
+---------------+------+----------------+----------+---------+---------+
|        Segment|buyers|avg_recency_days|avg_orders|avg_spend|  revenue|
+---------------+------+----------------+----------+---------+---------+
|        At-Risk| 39800|            88.0|      1.22|     69.4|2761848.0|
|      Champions| 10819|            35.0|       4.0|    203.3|2199805.0|
|New / Promising| 18732|            14.0|      1.16|     39.0| 731462.0|
|           Lost| 41167|            89.0|      1.02|     15.9| 655153.0|
+---------------+------+----------------+----------+---------+---------+
```

**LIVE 2: one real buyer (user 420629649).** 9 orders, last one 7 days ago, $602 spent, so a Champion. Spark found their 1,523 events among 19.5 million.
```
+---------+-------+---------+--------+-------+---------+
|  user_id|Recency|Frequency|Monetary|Cluster|  Segment|
+---------+-------+---------+--------+-------+---------+
|420629649|      7|        9|   601.7|      3|Champions|
+---------+-------+---------+--------+-------+---------+
```

**LIVE 3: score new customers** (log1p → assemble → scale → K-Means)
```
Recency=  5 days  Orders=6  Spend=$ 400  ->  Champions
Recency=  7 days  Orders=1  Spend=$  25  ->  New / Promising
Recency= 90 days  Orders=2  Spend=$ 150  ->  At-Risk
Recency=100 days  Orders=1  Spend=$  10  ->  Lost
```
The cluster *number* (0–3) can differ between runs; the segment *name* is what matters.

### 30.5 Numbers to check after the pre-run

| Output | Expected |
|---|---|
| Total size | 2.27 GB in 5 files |
| Raw events | 20,692,840 |
| Rows after cleaning | 19,493,208 (5.8% removed) |
| Users / sessions | 1,637,770 / 4,520,284 |
| Purchasing users = buyers with RFM | 110,518 |
| Silhouette at k = 4 | ≈ 0.49 |
| Segments | Champions 10,819 · New/Promising 18,732 · At-Risk 39,800 · Lost 41,167 |

Event and buyer counts must match exactly. If segment sizes differ by a few dozen, say "the sizes vary slightly between runs because K-Means starts from random centres".

### 30.6 If something goes wrong

| Problem | What to do |
|---|---|
| Colab disconnected | Don't re-run. Open `BDA_Customer_Segmentation_executed.ipynb` ("here is the same run saved") and continue |
| A live cell gives `NameError` | Runtime restarted; use the fallback above |
| A live cell is slow (> 15 s) | Keep talking; after 30 s stop it (■) and go to slide 5 |
| No internet / projector problem | Stay on slide 4 (its picture shows the segments) and describe the steps |
| Examiner asks for a different query | `spark.sql("SELECT brand, COUNT(*) FROM events WHERE event_type='purchase' GROUP BY brand ORDER BY 2 DESC LIMIT 5").show()` |
| Examiner asks for the Spark plan | `customers.explain()` or `rfm.explain()` |
| Examiner asks how many partitions | `events.rdd.getNumPartitions()` |

### 30.7 Extra live queries (if asked)
```python
# Top 5 brands bought by Champions (join of two views)
spark.sql("""
  SELECT e.brand, ROUND(SUM(e.price),0) AS revenue
  FROM events e JOIN segments s ON e.user_id = s.user_id
  WHERE e.event_type = 'purchase' AND s.Segment = 'Champions' AND e.brand IS NOT NULL
  GROUP BY e.brand ORDER BY revenue DESC LIMIT 5""").show()

# Cart abandonment: users who added to cart but never purchased
spark.sql("""
  SELECT COUNT(DISTINCT user_id) AS cart_users_no_purchase FROM events
  WHERE event_type = 'cart'
    AND user_id NOT IN (SELECT DISTINCT user_id FROM events WHERE event_type = 'purchase')""").show()

# The 4 cluster centres (in scaled log space)
for i, c in enumerate(final_model.clusterCenters()): print(i, names[i], c.round(2))
```
The join and `NOT IN` queries scan 19.5M cached rows, so expect 10–40 s on Colab.

## 31. Viva questions and answers

### Big Data and Spark basics

| Question | Answer |
|---|---|
| How big is the data? | 2.27 GB, 5 CSV files, 20,692,840 rows, 9 columns. After cleaning, 19,493,208 rows. |
| Why is this Big Data? | Volume (2.27 GB / 20.7M rows, ~10 GB in pandas memory), velocity (≈4M events a month arriving continuously), variety (semi-structured log with missing fields and text timestamps). |
| Why Spark and not pandas? | pandas loads everything into one machine's RAM in one thread. Spark splits the data into partitions, processes them in parallel, spills to disk if needed, and the same code runs unchanged on a multi-node cluster. |
| Why Spark and not Hadoop MapReduce? | MapReduce writes to disk between every step. K-Means is iterative, so Spark's in-memory caching is much faster. Spark also has high-level DataFrame, SQL and ML APIs. |
| What is Spark's architecture? | A driver builds the plan and schedules tasks; a cluster manager (local mode here) allocates resources; executors run one task per partition and hold cached data. |
| What is an RDD / how is Spark fault tolerant? | An immutable, partitioned, distributed collection that remembers its lineage. A lost partition is recomputed from the lineage instead of being stored twice. DataFrames run on top of RDDs. |
| What is lazy evaluation? | Transformations only build a DAG/plan. Nothing runs until an action (`count`, `show`, `toPandas`). The notebook shows the plan with `events.explain()` before `count()`. |
| Transformation vs action? | Transformations return a new DataFrame lazily (`filter`, `select`, `groupBy().agg()`). Actions trigger execution and return a result (`count`, `show`, `first`, `toPandas`). |
| Narrow vs wide transformation? | Narrow (`filter`, `withColumn`) works inside each partition. Wide (`groupBy`, `dropDuplicates`) needs a shuffle across partitions, visible as `Exchange hashpartitioning` in `explain()`. |
| Why `cache()`? | The cleaned 19.5M events are reused by every SQL query and the RFM step. Without caching, Spark would re-read and re-clean 2.3 GB each time. |
| Why an explicit schema? | `inferSchema` reads the whole 2.3 GB once just to guess types. `StructType` skips that pass and guarantees correct types. |
| Why `shuffle.partitions = 32`? | The default 200 creates many tiny tasks on a 2-core machine; 32 is enough parallelism with less overhead. |
| What is a broadcast join? | The small table (4 rows) is copied to every executor, so the big table joins locally without a shuffle. |
| Where is Spark SQL used? | The cleaned DataFrame is the temp view `events`; the overview, funnel, monthly revenue and top-brand queries are plain SQL run through the Catalyst optimiser. The segments view powers live cell 1. |
| What is Catalyst? | Spark's query optimiser: it analyses, rewrites (predicate pushdown, column pruning), picks physical strategies and generates code. SQL and DataFrame code get the same optimisation. |

### RFM and K-Means

| Question | Answer |
|---|---|
| What is RFM? | Recency (days since last purchase), Frequency (number of orders), Monetary (total spend). Recent, frequent, high-spending customers are the most valuable (Hughes, 1994). |
| What's an order here? | One `user_session` that contains purchase events. Every purchase row is one product; rows in the same session form one order. |
| Why the reference date 2020-03-01? | It is the day after the last event, so every recency is a positive number of days relative to the end of the data. |
| How does K-Means work? | Choose k centres, assign each point to the nearest centre, move each centre to the mean of its points, repeat until stable. It minimises WSSSE. |
| What is k-means\|\|? | Spark's parallel version of k-means++ initialisation, which spreads the starting centres out for better, faster results. |
| Why log-transform and scale? | Monetary ranges from $0.13 to $3,769 and Frequency from 1 to 122. K-Means uses Euclidean distance, so without log + StandardScaler the largest-range feature would decide every cluster. |
| How did you choose k? | Elbow on WSSSE plus Silhouette. k = 2 has the best Silhouette (0.66) but is too coarse. WSSSE drops 56k and 36k up to k = 4, then only 21k and under 10k per step. k = 4 is also stable across Spark 3.5 and 4.0. Silhouette 0.49 and four actionable personas. |
| Silhouette is higher at k = 5–8 (0.50–0.52). Why not use those? | The gain is small and not reliable: on Spark 3.5, k = 5 scored only 0.45. Those solutions depend on random starting centres. k = 2–4 are identical on both versions, the elbow is at 4, and four segments map to four clear actions. |
| What does Silhouette mean? | It compares each point's mean distance to its own cluster (a) with the nearest other cluster (b): (b − a) / max(a, b). Range −1 to 1; higher is better. 0.49 is reasonable structure. |
| Why seed = 42? | K-Means starts from random centres; a fixed seed makes the result reproducible. |
| How are clusters named? | Highest frequency → Champions; of the rest, most recent → New; of the last two, the higher spender → At-Risk, the other → Lost. |
| At-Risk and Lost have the same recency. Why separate them? | At-Risk buyers spent 4× more ($69 vs $16) and hold 43.5% of revenue. A win-back campaign is worth it for them, not for Lost. |
| How does live cell 3 score a new customer? | It applies the same log1p → VectorAssembler → fitted StandardScaler → fitted KMeans steps and maps the cluster number to its name. |
| Alternatives to K-Means? | Bisecting K-Means and Gaussian Mixture (both in MLlib), DBSCAN, hierarchical clustering. |

### Ethics and limitations

| Question | Answer |
|---|---|
| Ethics / privacy? | The data is anonymised (numeric IDs only). Real use needs consent for tracking and marketing (GDPR, India DPDP Act 2023) and must avoid unfair targeting, such as worse prices for some segments. |
| Limitations? | A 5-month snapshot of one shop, K-Means assumes round clusters, k is partly judgement, RFM ignores browsing behaviour, and it runs on single-node Spark. |
| Future scope? | Structured Streaming for live segments, behavioural features, comparing other MLlib clustering algorithms, a multi-node cluster with the full REES46 data. |

## 32. Project timeline and status

| Date | Milestone |
|---|---|
| 2 Nov 2026 | Project plan written; topic #34 chosen; email requesting the make-up slot (16/17 Nov) drafted |
| 12 Nov 2026 | Dataset switched from UCI Online Retail (48 MB) to REES46 cosmetics (2.27 GB) to meet the 1–4 GB rule; notebook verified end to end on local PySpark 3.5; charts, CSV, deck and script done |
| 13 Nov 2026 | Live-demo cells added; slide 4 changed from video to live demo; report, technical guide and demo guide written; full Colab run (Spark 4.0.5) matched all numbers; deck updated to Colab results |
| 16/17 Nov 2026 | Presentation (make-up slot, 10–11 AM) |

**Session plan used:** Session 1 (setup, ingestion, cleaning, Spark SQL EDA), Session 2 (RFM, scaling, K-Means, naming, charts), Session 3 (slides, demo preparation).

**Remaining:** rehearse once under 5 minutes; after the presentation, expire the Kaggle API token.

## 33. References

1. Hughes, A. M. (1994). *Strategic Database Marketing*. Probus.
2. MacQueen, J. (1967). Some methods for classification and analysis of multivariate observations. *Proc. 5th Berkeley Symposium on Mathematical Statistics and Probability*.
3. Wu, X. et al. (2008). Top 10 algorithms in data mining. *Knowledge and Information Systems*, 14(1).
4. Chen, D., Sain, S. L., & Guo, K. (2012). Data mining for the online retail industry: a case study of RFM model-based customer segmentation using data mining. *J. Database Marketing & Customer Strategy Management*, 19(3).
5. Zaharia, M. et al. (2016). Apache Spark: a unified engine for big data processing. *Communications of the ACM*, 59(11).
6. Meng, X. et al. (2016). MLlib: Machine learning in Apache Spark. *JMLR*, 17(34).
7. Rousseeuw, P. J. (1987). Silhouettes: a graphical aid to the interpretation and validation of cluster analysis. *J. Computational and Applied Mathematics*, 20.
8. Bahmani, B. et al. (2012). Scalable K-Means++. *Proceedings of the VLDB Endowment*, 5(7).
9. Dean, J., & Ghemawat, S. (2008). MapReduce: simplified data processing on large clusters. *Communications of the ACM*, 51(1).
10. Kechinov, M. (2020). eCommerce Events History in Cosmetics Shop. Kaggle / REES46 Open CDP.
