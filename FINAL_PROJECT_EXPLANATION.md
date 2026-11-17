# Customer Segmentation of E-Commerce Big Data using PySpark K-Means (RFM): Full Project Explanation

**Course:** Big Data Analytics, B.Tech Sem VII CSE/ICT, Adani University
**Reference topic:** #34, Customer Segmentation Using Scalable Clustering Techniques
**Author:** Rohan Upadhyay
**Environment:** Google Colab (Spark 4.0.5, 2 cores), Spark SQL, Spark MLlib. Also verified locally on Spark 3.5.3. All results below are from the Colab run.
**Notebook:** `BDA_Customer_Segmentation.ipynb`
**Slides:** `BDA_Customer_Segmentation_Presentation_v2.pptx` (5 slides, speaker notes included)

This single file brings together everything in `PROJECT_REPORT.md`, `TECHNICAL_GUIDE.md`, `PRESENTATION_SCRIPT.md`, `DEMO_GUIDE.md` and the slide deck. It covers the project itself, the Spark concepts behind it, how the notebook works, the slides, the live demo and the viva questions.

---

## Contents

1. [Project at a glance](#1-project-at-a-glance)
2. [Introduction](#2-introduction)
3. [Literature review](#3-literature-review)
4. [Dataset](#4-dataset)
5. [Concepts you need](#5-concepts-you-need)
6. [Methodology](#6-methodology)
7. [Notebook walkthrough](#7-notebook-walkthrough)
8. [Results](#8-results)
9. [Discussion, ethics, limitations and future scope](#9-discussion-ethics-limitations-and-future-scope)
10. [Conclusion and syllabus mapping](#10-conclusion-and-syllabus-mapping)
11. [The presentation, slide by slide](#11-the-presentation-slide-by-slide)
12. [Live demo runbook](#12-live-demo-runbook)
13. [Viva: likely questions and short answers](#13-viva-likely-questions-and-short-answers)
14. [Project files, rebuilding and known issues](#14-project-files-rebuilding-and-known-issues)
15. [References](#15-references)

---

## 1. Project at a glance

| Item | Value |
|---|---|
| Data | REES46 cosmetics-shop clickstream, 5 CSV files, **2.27 GB**, **20,692,840** events, Oct 2019 – Feb 2020 |
| After cleaning | **19,493,208** events (5.8% removed) |
| Users / sessions | 1,637,770 / 4,520,284 |
| Buyers (RFM rows) | **110,518** |
| Model | MLlib K-Means, **k = 4**, `seed=42`, Silhouette **0.49** |
| Segments | Champions 10,819 · New / Promising 18,732 · At-Risk 39,800 · Lost 41,167 |
| Key finding | Champions are **9.8%** of buyers but earn **34.7%** of revenue. At-Risk buyers hold **43.5%** of revenue, so winning them back matters most. |

**Abstract.** An online cosmetics shop logged 20.7 million clickstream events (2.27 GB) over five months, but it markets to every buyer in the same way. This project uses Apache Spark to process the full log. It cleans the data and explores it with SparkSQL, computes Recency, Frequency and Monetary (RFM) values for each of 110,518 buyers, and clusters the buyers with Spark MLlib K-Means. The elbow method and the Silhouette score point to k = 4 (Silhouette 0.49). The four segments are Champions, New / Promising, At-Risk and Lost. A win-back campaign for the At-Risk segment is the highest-value action.

---

## 2. Introduction

### 2.1 Problem statement
E-commerce sites collect huge event logs: every product view, add-to-cart, removal and purchase. A single marketing message for all customers wastes budget. Loyal customers get discounts they don't need, and lapsing customers are ignored. The shop needs to **group customers by behaviour** so that each group gets a suitable action.

### 2.2 Why this is a Big Data problem
| "V" | In this project |
|---|---|
| **Volume** | 2.27 GB of CSV, 20,692,840 rows, 5 files. That's too large for comfortable single-threaded pandas work: it would need roughly 10 GB of RAM. |
| **Velocity** | Clickstream events arrive continuously, about 4 million a month here. |
| **Variety** | Semi-structured events with missing `category_code`/`brand`, four event types, and timestamps stored as text. |

### 2.3 Objectives
1. Ingest and clean the full 2.27 GB log with Spark DataFrames.
2. Explore it with SparkSQL: the event funnel, monthly revenue and top brands.
3. Engineer RFM features for each buyer with distributed aggregations.
4. Cluster buyers with MLlib K-Means, choosing k with the elbow and Silhouette methods.
5. Turn the clusters into named segments with marketing actions.

---

## 3. Literature review

| Work | Contribution | How this project uses it |
|---|---|---|
| Hughes (1994), *Strategic Database Marketing* | Introduced the **RFM model**. Customers who bought recently, buy often and spend more are worth more and are the most likely to respond. | The three features. |
| MacQueen (1967); Wu et al. (2008), "Top 10 algorithms in data mining" | **K-Means** partitions points into k groups by minimising the within-cluster squared distance. It is one of the top 10 data-mining algorithms and the usual choice for RFM segmentation. | The clustering algorithm. |
| Chen, Sain & Guo (2012), *J. Database Marketing & Customer Strategy Mgmt.* | Segmented an online retailer's customers with RFM + K-Means (the UCI Online Retail study). | The same method, on a dataset about 40 times larger (a clickstream instead of invoices). |
| Zaharia et al. (2016), "Apache Spark: a unified engine for big data processing", *CACM* | In-memory, fault-tolerant distributed processing using RDDs and lineage. | The engine. |
| Meng et al. (2016), "MLlib: Machine Learning in Apache Spark", *JMLR* | Scalable ML pipelines on Spark, including distributed K-Means with k-means\|\| initialisation. | VectorAssembler, StandardScaler, KMeans, ClusteringEvaluator. |
| Rousseeuw (1987), "Silhouettes", *J. Comp. & Applied Math.* | The Silhouette coefficient for checking cluster quality. | Choosing k. |

---

## 4. Dataset

**eCommerce Events History in Cosmetics Shop** (REES46 Open CDP), Kaggle `mkechinov/ecommerce-events-history-in-cosmetics-shop`.

| Property | Value |
|---|---|
| Files | `2019-Oct.csv`, `2019-Nov.csv`, `2019-Dec.csv`, `2020-Jan.csv`, `2020-Feb.csv` |
| Size | **2.27 GB** (CSV), about 0.5 GB zipped |
| Rows | **20,692,840** events |
| Period | 1 Oct 2019 – 29 Feb 2020 |
| Columns | `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session` |
| Location | `data/cosmetics/` locally. On Colab the notebook downloads it with the Kaggle API. |

`event_type` is one of `view`, `cart`, `remove_from_cart` or `purchase`. Users are anonymised as numeric IDs, so the data contains no personal information.

---

## 5. Concepts you need

### 5.1 Apache Spark
- Spark is an engine that splits data into **partitions** and processes them **in parallel**, on the cores of one machine or across many machines.
- Here it runs in **local mode** (`master("local[*]")` uses every core on the Colab VM, which is 2 on free Colab). The same code would run on a cluster without changes.
- **SparkSession** is the entry point. **DataFrame** is a distributed table with named columns.
- **RDD** (Resilient Distributed Dataset) is the low-level structure underneath every DataFrame. It is immutable and partitioned, and it remembers its **lineage**, the steps that built it. If a partition is lost, Spark rebuilds it from the lineage, which is how Spark is fault tolerant.

### 5.2 Transformations vs actions, and lazy evaluation
| Transformations (lazy, return a new DataFrame) | Actions (run the job, return a result) |
|---|---|
| `filter`, `withColumn`, `select`, `dropDuplicates`, `groupBy().agg()`, `join` | `count()`, `show()`, `first()`, `collect()`, `toPandas()`, `write` |

- Transformations only build a **plan** (a DAG). Spark's Catalyst optimiser rewrites the plan, for example by pushing filters down to the file scan. Work starts only when an **action** is called.
- `events.explain()` prints that plan before anything has run.

### 5.3 Narrow vs wide transformations
- **Narrow** transformations (`filter`, `withColumn`) work inside each partition. No data moves between partitions.
- **Wide** transformations (`groupBy`, `dropDuplicates`, `join`, `orderBy`) need a **shuffle**, where rows with the same key are sent to the same partition. In `explain()` a shuffle shows up as `Exchange hashpartitioning(...)`.
- `spark.sql.shuffle.partitions = 32` sets how many partitions a shuffle produces. The default of 200 is too many for 2 cores.

### 5.4 Caching
- `.cache()` keeps a DataFrame in memory, spilling to disk if needed, **after the first action computes it**.
- The cleaned `events` DataFrame (19.5M rows) is reused by every SQL query and by the RFM step, so caching it saves re-reading and re-cleaning 2.27 GB each time.

### 5.5 SparkSQL
- `events.createOrReplaceTempView("events")` lets you query the DataFrame with plain SQL.
- `spark.sql("SELECT ...")` returns a DataFrame. It goes through the same optimiser, so SQL and the DataFrame API perform the same.

### 5.6 RFM
| Letter | Meaning | Here |
|---|---|---|
| R: Recency | How recently they bought | Days from the last purchase to 2020-03-01 |
| F: Frequency | How often they buy | Number of distinct purchase sessions (orders) |
| M: Monetary | How much they spend | Sum of `price` over purchase rows |

### 5.7 K-Means, WSSSE and Silhouette
- **K-Means** picks k centres and assigns each point to the nearest one. It then moves each centre to the mean of its points and repeats until nothing changes (or `maxIter` is reached). MLlib uses the **k-means||** method to choose good starting centres in parallel. `seed=42` makes the result reproducible.
- **WSSSE** is the sum of squared distances from each point to its centre. It always falls as k grows, so you look for the **elbow**, where it stops falling quickly.
- **Silhouette**, for a point, is (b − a) / max(a, b). Here a is the mean distance to its own cluster and b is the mean distance to the nearest other cluster. It ranges from −1 to 1, and higher means better-separated clusters. `ClusteringEvaluator` computes the average over all points.

### 5.8 Why log + scaling
K-Means uses distance. Monetary is in dollars (up to 3,769) and Frequency is a count (mostly 1), so without scaling Monetary would decide everything. `log1p` (that is, log(1 + x)) reduces the long tail, and `StandardScaler` gives every feature mean 0 and standard deviation 1.

---

## 6. Methodology

### 6.1 Pipeline overview
```
5 CSVs (2.27 GB)
   │  spark.read.csv(files, schema=…)             ← explicit StructType, no inferSchema pass
   ▼
raw DataFrame (20.69M rows)
   │  to_timestamp · filter nulls · price > 0 · dropDuplicates · cache
   ▼
events (19.49M rows, cached) ──► SparkSQL view "events": funnel, monthly revenue, top brands
   │  filter event_type = 'purchase'  (1.29M rows)
   │  groupBy(user_id).agg(datediff(max date), countDistinct(session), sum(price))
   ▼
RFM table (110,518 buyers)
   │  log1p → VectorAssembler → StandardScaler(withMean, withStd)
   ▼
feature vectors
   │  KMeans(k = 2..8) + ClusteringEvaluator (Silhouette)  →  choose k = 4
   ▼
KMeans(k=4, seed=42) → cluster profiles → naming rule → broadcast join → customer_segments.csv
```

On the slide this is shown as seven steps: **Ingest → Clean → EDA → RFM → Scale → K-Means → Profile**.

### 6.2 Ingestion
- All five files are read into **one DataFrame** with an **explicit schema**. `inferSchema` would need an extra full pass over 2.27 GB just to guess the column types, and the explicit schema also guarantees correct types (for example `user_id` as long).
- Spark splits the input into partitions: **19 on Colab** and 25 locally, depending on core count and file splits. The partitions are processed in parallel.

### 6.3 Cleaning
| Rule | Reason |
|---|---|
| Parse `event_time` (strip `" UTC"`, `to_timestamp`) | Needed for date arithmetic. |
| Drop null `user_id` / `user_session` | Can't attribute the event to a customer or an order. |
| Drop `price <= 0` | These are free samples or bad data and would distort Monetary. |
| `dropDuplicates()` | The raw log contains repeated identical rows. This is a *wide* transformation (a shuffle). |

The result is **19,493,208 rows**, so **1,199,632 rows (5.8%)** were removed.

All of these are **lazy transformations**. `events.explain()` prints the physical plan, including `Exchange hashpartitioning` (the shuffle), before anything runs. The `count()` **action** then runs the plan. The result is **cached** because every later step reuses it.

### 6.4 Exploratory analysis (SparkSQL)
The cleaned DataFrame is registered as the temp view `events`. The queries cover:
- An overview with distinct users, sessions, products and brands.
- The event funnel by `event_type`.
- Monthly purchase revenue.
- The top 10 brands by revenue.

Only the small aggregated results go to the driver (`toPandas()`) for plotting.

### 6.5 RFM feature engineering
Only `purchase` events are used (1,285,982 rows). One purchase row is one product, and **one order is one `user_session`**.

| Feature | Definition |
|---|---|
| Reference date | last event date + 1 day = **2020-03-01** |
| Recency | `datediff(ref_date, max(purchase date))`, in days. Lower is better. |
| Frequency | `countDistinct(user_session)`, the number of orders |
| Monetary | `sum(price)`, in $ |

### 6.6 Scaling
All three features are very right-skewed. Most buyers order once, Frequency ranges from 1 to 122, and Monetary from $0.13 to $3,769. K-Means uses Euclidean distance, so:
1. `log1p` reduces the skew.
2. `VectorAssembler` combines `[R_log, F_log, M_log]` into one vector.
3. `StandardScaler(withMean=True, withStd=True)` gives each feature mean 0 and standard deviation 1, so that no single feature (Monetary in particular) dominates the distance.

### 6.7 Choosing k
K-Means was trained for k = 2..8 (`seed=42`, `maxIter=50`). Each model was scored with:
- **WSSSE**, the within-cluster sum of squared errors (`model.summary.trainingCost`), for the elbow method.
- The **Silhouette** score from `ClusteringEvaluator` (squared Euclidean distance).

| k | WSSSE (Colab, Spark 4.0) | Silhouette (Colab) | Silhouette (local, Spark 3.5) |
|---|---|---|---|
| 2 | 209,251 | **0.662** | 0.662 |
| 3 | 153,483 | 0.548 | 0.548 |
| **4** | **117,250** | **0.490** | **0.490** |
| 5 | 96,295 | 0.515 | 0.452 |
| 6 | 86,820 | 0.517 | 0.504 |
| 7 | 77,242 | 0.503 | 0.472 |
| 8 | 71,617 | 0.512 | 0.507 |

**Decision: k = 4.**
- k = 2 has the best Silhouette but only splits buyers into "good" and "bad", which is too coarse to act on.
- WSSSE falls by 56k from k = 2 to 3 and by 36k from 3 to 4, then by 21k from 4 to 5 and under 10k per step after that. That bend is the elbow.
- **Stability:** k = 2, 3 and 4 produce the same clusters on Spark 3.5 and Spark 4.0 (the same Silhouette and WSSSE). For k ≥ 5 the results change between the two versions; at k = 5 the Silhouette moved from 0.45 to 0.52. Those solutions depend on the random starting centres, so they aren't reliable. On Colab k = 5–8 score slightly higher (0.50–0.52) than k = 4 (0.49), but the gain is small and unstable.
- At k = 4 the Silhouette is 0.49, which indicates reasonable structure, and the clusters correspond to four distinct personas.

### 6.8 Naming the clusters
The naming rule is deterministic and explainable, and it is applied to the cluster averages:
1. Highest average Frequency → **Champions**
2. Of the rest, lowest average Recency → **New / Promising**
3. Of the last two, higher average Monetary → **At-Risk**, and the other → **Lost**

The names are attached to all 110,518 buyers with a **broadcast join**: a small lookup table is copied to every executor, so no shuffle is needed.

---

## 7. Notebook walkthrough

`BDA_Customer_Segmentation.ipynb` is generated by `tools/build_nb.py` (edit the script, not the notebook).

| § | Cell does | Key Spark idea | Output to look for |
|---|---|---|---|
| 0 | Detects Colab, installs `pyspark` and `kaggle>=1.8`, creates the SparkSession (6 GB driver memory, 32 shuffle partitions), sets the chart palette | SparkSession, local mode | `Spark version`, `Default parallelism` (2 on Colab) |
| 1a | If `data/cosmetics/*.csv` is missing, downloads it with the Kaggle CLI using the `KAGGLE_API_TOKEN` secret, then prints the file sizes | — | 5 files, **2.27 GB** |
| 1b | Explicit `StructType` schema → `spark.read.csv(files, ...)` → `count()` | Explicit schema, action | **20,692,840** rows |
| 2 | Clean (timestamp, nulls, price, duplicates) → `.cache()` → `explain()` → `count()` | Lazy evaluation, wide transformation, cache | `Exchange hashpartitioning` in the plan; **19,493,208** rows |
| 3a | Temp view `events`; overview query | SparkSQL | 1,637,770 users, 4,520,284 sessions |
| 3b | Funnel, monthly revenue and top-brand queries | `GROUP BY`, `COUNT(DISTINCT)` | 110,518 purchasing users |
| 3c | `toPandas()` on the **small aggregated** results → 3 charts | Only small results go to the driver | `eda_*.png` |
| 4 | Filter purchases → `groupBy(user_id).agg(...)` → RFM, cached | Distributed aggregation | **110,518** buyers; summary statistics |
| 4b | `log1p` columns + histograms | — | `rfm_distributions.png` |
| 5 | `VectorAssembler` → `StandardScaler` → `data` (cached) | MLlib feature pipeline | `features` vectors |
| 6 | Loop over k = 2..8: KMeans + Silhouette | MLlib, `ClusteringEvaluator` | 7 printed lines; slowest step (several minutes on Colab) |
| 6b | Elbow and Silhouette chart, `K_FINAL = 4` | — | `k_selection.png` |
| 7 | Final `KMeans(k=4, seed=42)` → `groupBy(Cluster)` profile | — | Silhouette 0.4895 and the profile table |
| 7b | Naming rule (pandas on 4 rows) | — | Segment table |
| 7c | Broadcast join of names → `customer_segments.csv`, plus charts | `F.broadcast` (no shuffle) | `segment_sizes.png`, scatter plots |
| 8 | Markdown table of business actions; saves `outputs/results.json` | — | JSON printout |
| 9 | **Live demo cells:** ad-hoc SQL on the segments, look up one buyer, score a new customer | Reusing cached data and a trained model | See section 12 |
| — | `# spark.stop()`, commented out so the session stays alive for the demo | — | — |

### Important variables (if you add cells)
| Name | What it is |
|---|---|
| `raw` | The DataFrame read from the 5 CSVs (not cached) |
| `events` | The cleaned, cached events. Also the SQL view `events`. |
| `rfm` | One row per buyer: `user_id, Recency, Frequency, Monetary` (cached) |
| `rfm_log` | `rfm` plus `R_log, F_log, M_log` |
| `assembler`, `scaler` | The MLlib feature stages. `scaler_model` is fitted in live cell 3. |
| `data` | The scaled feature DataFrame used for training (cached) |
| `scores` | pandas table of k, WSSSE and Silhouette |
| `final_model` | The trained `KMeansModel` (k = 4) |
| `segmented` | `data` plus a `Cluster` column (cached) |
| `prof`, `names` | The profile table (pandas) and the mapping from cluster number to segment name |
| `customers` | Every buyer with their `Segment` (Spark). Also the SQL view `segments` after live cell 1. |
| `RESULTS` | The dict of numbers that is saved to `outputs/results.json`, which feeds the slide deck |

---

## 8. Results

### 8.1 Dataset overview
| Metric | Value |
|---|---|
| Raw → clean events | 20,692,840 → 19,493,208 (5.8% removed) |
| Users / sessions | 1,637,770 / 4,520,284 |
| Products / brands | 53,903 / 273 |
| Purchase revenue | $6.35M |
| Peak month | Nov 2019 ($1.53M, the month that includes Black Friday) |

### 8.2 Event funnel
| Stage | Events | Distinct users |
|---|---|---|
| view | 9,626,268 | 1,596,359 |
| cart | 5,612,289 | 397,390 |
| remove_from_cart | 2,968,669 | 183,002 |
| purchase | 1,285,982 | **110,518** |

**6.9%** of users who viewed a product made a purchase.

### 8.3 Segments (k = 4, Silhouette 0.4895, Colab run)
| Segment | Buyers | Share | Avg Recency (days) | Avg Orders | Avg Spend | Revenue | Revenue share |
|---|---|---|---|---|---|---|---|
| Champions | 10,819 | 9.8% | 35.0 | 4.00 | $203.3 | $2.20M | **34.7%** |
| New / Promising | 18,732 | 16.9% | 14.5 | 1.16 | $39.0 | $0.73M | 11.5% |
| At-Risk | 39,800 | 36.0% | 87.5 | 1.22 | $69.4 | $2.76M | **43.5%** |
| Lost | 41,167 | 37.2% | 89.5 | 1.02 | $15.9 | $0.66M | 10.3% |

The local Spark 3.5 run gave the same four segments, with sizes within 50 buyers of these (for example 10,835 Champions). All revenue shares are the same to one decimal place. The local outputs are kept in `outputs/local_spark35/`.

### 8.4 Business actions
| Segment | Who they are | Action |
|---|---|---|
| Champions | About 4 orders each, recent, high spend | VIP or loyalty tier, early access, referral rewards |
| New / Promising | Bought in the last 2 weeks, usually one order | Welcome email series and a second-order coupon |
| At-Risk | One or two good-sized orders, but no purchase for about 3 months | Personalised win-back offers using their favourite brands |
| Lost | One small (about $16) order, about 3 months ago | Newsletter only, with no paid-ads spend |

### 8.5 Charts (in `outputs/`)
| File | Shows | Used on slide |
|---|---|---|
| `eda_event_funnel.png` | View → cart → remove → purchase funnel | 4 |
| `eda_monthly_revenue.png` | Purchase revenue per month (Nov peak) | — |
| `eda_top_brands.png` | Top 10 brands by revenue | — |
| `rfm_distributions.png` | Skewed R, F, M histograms (raw and log) | — |
| `k_selection.png` | Elbow (WSSSE) and Silhouette for k = 2..8 | 3 |
| `segment_sizes.png` | Buyers and revenue share per segment | 5 |
| `segments_scatter.png`, `segments_scatter_fm.png` | Buyers coloured by segment in RFM space | 4 (`segments_scatter.png`) |

`customer_segments.csv` lists all 110,518 buyers with their segment, and `results.json` holds every number used on the slides.

---

## 9. Discussion, ethics, limitations and future scope

### 9.1 Discussion
- **Pareto effect.** About 10% of buyers (Champions) bring in about 35% of revenue, which is consistent with the RFM literature.
- **At-Risk vs Lost.** Both segments are about 3 months inactive, but At-Risk buyers spent **4 times more** on average ($69 vs $16). Recency alone can't separate them, but the clustering on all three features can.
- **Very low repeat rate.** The median Frequency is 1. This is typical of cosmetics e-commerce, and it is the reason the New → second-order conversion matters.
- **Why Spark.** A single `groupBy` over 1.29M purchase rows and a cached 19.5M-row DataFrame keep every step interactive. The same code would run unchanged on a multi-node cluster.

### 9.2 Ethics and privacy
- The data is anonymised: numeric user IDs only, with no names, emails or addresses.
- In real use, behavioural tracking and targeted marketing need user consent (GDPR / India DPDP Act 2023).
- The segments should be used to *serve* customers better. They must not be used to discriminate, for example by charging higher prices to loyal customers.

### 9.3 Limitations
- It covers a 5-month snapshot of a single shop, so seasonality beyond Black Friday isn't captured.
- The choice of k involves judgement, and K-Means assumes compact, round clusters.
- RFM ignores browsing, cart abandonment, product mix and returns.
- It runs on single-node Spark (Colab, 2 cores) rather than a real cluster.

### 9.4 Future scope
- Use **Spark Structured Streaming** to update the segments as new events arrive.
- Add behavioural features such as views, cart-abandonment rate and brand affinity.
- Compare against **Bisecting K-Means** and **Gaussian Mixture** in MLlib.
- Deploy on a multi-node cluster (Databricks / EMR) with the full multi-category REES46 data (tens of GB).

---

## 10. Conclusion and syllabus mapping

### 10.1 Conclusion
Spark processed 2.27 GB of clickstream data end to end: ingestion, cleaning, SQL analytics, feature engineering and MLlib clustering. The result is four actionable customer segments. The single most valuable action is a win-back campaign for the **At-Risk** segment (43.5% of revenue).

### 10.2 Syllabus mapping
| Unit | Topic | Where |
|---|---|---|
| 1 | Big Data characteristics (3 V's) | §2.2, the 2.27 GB / 20.7M-row dataset |
| 2 | Distributed and batch processing | Spark partitions, parallel batch job |
| 4 | SparkSession, DataFrames, transformations and actions, lazy evaluation, caching, SparkSQL | §6.2–6.5 |
| 5 | Preprocessing, scalable ML, clustering, MLlib | §6.6–6.8 |
| 6 | Big Data in e-commerce, privacy and ethics | §8.4, §9.2 |

Course outcomes: **CO1, CO2, CO4, CO5**.

---

## 11. The presentation, slide by slide

The deck is `BDA_Customer_Segmentation_Presentation_v2.pptx`: **5 slides in 5 minutes**. Every slide carries the footer *"Customer Segmentation of E-Commerce Big Data using PySpark K-Means (RFM) · Big Data Analytics · Adani University"* and a page number. The same speaking text is in each slide's speaker notes. Speak it, don't read it. The timings are targets.

### Slide 1: Introduction (≈ 50 s)
**Title:** *Segmenting 111k shoppers from 21M events with Spark*
**Subtitle:** Rohan Upadhyay · B.Tech Sem VII · Big Data Analytics mini project (Reference topic #34)

**On the slide**
- **Problem:** An online cosmetics shop logs millions of clicks, but markets to every buyer the same way. Its best buyers get no reward, and lapsing buyers get no win-back offer.
- **Dataset:** REES46 eCommerce events (Kaggle): 5 monthly CSVs, 2.27 GB, 20,692,840 events (view, cart, remove, purchase), Oct 2019 – Feb 2020.
- **Objective:** Use Apache Spark to compute Recency, Frequency and Monetary (RFM) value for every buyer, then group buyers with MLlib K-Means into segments that marketing can act on.
- **Stat tiles:** 2.27 GB (5 CSV files) · 20.7M (raw events) · 1.6M (unique users) · 110,518 (buyers) · PySpark (Colab · DataFrames · SparkSQL · MLlib)

**Say**
> Good morning. My project is **customer segmentation of e-commerce big data using PySpark K-Means**, reference topic 34.
>
> The problem: an online cosmetics shop records millions of clicks, but it markets to every buyer in the same way.
>
> The dataset is the **REES46 e-commerce events log** from Kaggle: **five monthly CSV files, 2.27 GB, 20.7 million events**, October 2019 to February 2020. Every row is a view, add-to-cart, remove or purchase.
>
> My objective is to use **Spark** to compute **Recency, Frequency and Monetary** values for every buyer, then use **MLlib K-Means** to group them into segments marketing can act on.

### Slide 2: Literature Review (≈ 50 s)
**Title:** *What prior work tells us*

**On the slide** (four cards)
- **RFM model, Hughes (1994):** Introduced Recency–Frequency–Monetary scoring for database marketing: recent, frequent, high-spend customers are the most likely to respond.
- **K-Means clustering, MacQueen (1967); Wu et al. (2008):** K-Means partitions data into k groups by minimising within-cluster squared distance. It is one of the top-10 data-mining algorithms and is the usual choice for RFM segmentation.
- **RFM + K-Means in e-retail, Chen, Sain & Guo (2012):** Segmented an online retailer's customers with RFM and K-Means (the UCI Online Retail study). We apply the same idea to a 40× larger clickstream log.
- **Apache Spark & MLlib, Zaharia et al. (2016); Meng et al. (2016):** Spark processes data in memory across partitions and machines, and MLlib provides distributed K-Means (k-means|| initialisation), so the method scales to gigabytes of events.

**Say**
> Four pieces of prior work shape this project.
> - **Hughes, 1994**, introduced the **RFM model**: customers who bought recently, buy often and spend more are the most valuable.
> - **K-Means** (MacQueen, 1967) is one of the top-10 data-mining algorithms (Wu et al., 2008) and the usual choice for clustering RFM values.
> - **Chen, Sain and Guo, 2012**, segmented an online retailer's customers with RFM and K-Means. I apply the same idea to a clickstream log that is about 40 times larger.
> - **Spark and MLlib** (Zaharia et al., 2016; Meng et al., 2016) run this in memory and in parallel, which lets the method scale to gigabytes.

### Slide 3: Methodology (≈ 70 s)
**Title:** *Methodology: a Spark pipeline from raw events to segments*

**On the slide**
- **Pipeline boxes:** Ingest (`spark.read.csv`, 5 files · 2.27 GB, 19 partitions) → Clean (parse time, drop nulls, price ≤ 0, duplicates) → EDA (SparkSQL on view 'events') → RFM (purchases → `groupBy(user_id).agg()`) → Scale (log1p → VectorAssembler → StandardScaler) → K-Means (k = 2..8, elbow + Silhouette) → Profile (name segments)
- **Cleaning:** 20,692,840 → 19,493,208 events (5.8% removed: nulls, price ≤ 0, duplicate events). Cleaned data is cached in memory.
- **RFM:** from 1,285,982 purchase events → 110,518 buyers. Recency = days since last order, Frequency = orders (sessions), Monetary = total $.
- **Scaling:** log1p removes heavy skew; StandardScaler stops Monetary dominating the Euclidean distance.
- **Choosing k:** k=2 is too coarse; WSSSE bends at k=4; k≤4 gives identical clusters on Spark 3.5 and 4.0, k≥5 doesn't (unstable) → KMeans(k=4, seed=42), Silhouette 0.49.
- **Chart:** `k_selection.png` (elbow + Silhouette).

**Say**
> The whole pipeline runs in Spark.
> 1. **Ingest:** Spark reads all five CSVs into one DataFrame of 19 partitions, processed in parallel. I give an explicit schema, so Spark doesn't need an extra pass over 2.3 GB to guess the types.
> 2. **Clean:** parse the timestamp and drop missing users, zero prices and duplicate events. That leaves **19.5 million events** (5.8% removed). Spark is **lazy**: these steps only build a plan, which runs at the `count()` action. The cleaned data is **cached**, because every later step reuses it.
> 3. **EDA** with **SparkSQL**: the view → cart → purchase funnel, monthly revenue and top brands.
> 4. **RFM:** keep the 1.29 million purchase events, then one `groupBy(user_id).agg(...)` gives **110,518 buyers**.
> 5. **Scale:** the values are very skewed, so I apply **log1p**, then **VectorAssembler** and **StandardScaler**.
> 6. **K-Means for k = 2 to 8.** Silhouette is highest at 2, but that is too coarse. The error curve bends at **4** with Silhouette 0.49. k = 4 is also stable: up to 4 the clusters are identical on two Spark versions, while 5 and above change between runs. So the final model is **KMeans(k = 4, seed = 42)**.

### Slide 4: Working Model, live demo (≈ 90 s)
**Title:** *Working model: live demo on Google Colab*

**On the slide**
- Banner: **LIVE · switching to the Colab notebook**
- **What I show live:** 1. Spark read 5 files · 2.27 GB · 2. 20.7M → 19.5M events (lazy plan) · 3. SparkSQL funnel + monthly revenue · 4. RFM for 111k buyers via groupBy · 5. Elbow + Silhouette, k = 4 · 6. LIVE: SQL query + score a new customer
- **Pictures:** `segments_scatter.png` (the four final segments) and `eda_event_funnel.png`. These are the fallback if Colab fails.

**Say** *(Alt+Tab to Colab. The notebook was run before the session, so every output is already there. Follow section 12.3.)*
> "Let me show it running on Colab. Spark read all five files, 2.27 GB and 20.7 million events. Here is the lazy plan, and cleaning kept 19.5 million events. The SparkSQL funnel shows that 1.6 million users viewed products but only 110 thousand bought, which is 6.9%. One groupBy builds the RFM table, and the K-Means loop for k from 2 to 8 gives the elbow at 4.
> Now live: *(Shift+Enter on LIVE 1)* a SQL query on the segments; Champions earn about a third of revenue. *(Shift+Enter on LIVE 3)* And the trained model scores a brand-new customer: recent, frequent big spenders are Champions, one small old order is Lost."
>
> *(Alt+Tab back to the slides. If Colab fails, stay on this slide: the chart shows the four segments.)*

### Slide 5: Conclusion (≈ 50 s)
**Title:** *Four segments, four different marketing actions*

**On the slide**
| Segment | Buyers | Avg R / F / M | Revenue share | Action |
|---|---|---|---|---|
| Champions | 10,819 (9.8%) | 35d / 4.0 / $203 | 34.7% | VIP tier, early access, referrals |
| New / Promising | 18,732 (16.9%) | 14d / 1.2 / $39 | 11.5% | Welcome series + 2nd-order coupon |
| At-Risk | 39,800 (36.0%) | 88d / 1.2 / $69 | 43.5% | Personalised win-back offers |
| Lost | 41,167 (37.2%) | 90d / 1.0 / $16 | 10.3% | Newsletter only, no paid-ads spend |

- **Key finding:** Champions are only 9.8% of buyers but bring 34.7% of revenue. At-Risk buyers hold 43.5% of revenue: winning them back matters most.
- **Limitations:** 5-month snapshot of one shop · k chosen partly by judgement · RFM ignores browsing behaviour and product mix · single-node Spark on Colab
- **Future scope:** Spark Structured Streaming for live segments · add view/cart features · Bisecting K-Means / GMM in MLlib · multi-node cluster
- **Chart:** `segment_sizes.png`.

**Say**
> Spark K-Means found four segments among 110,518 buyers:
> - **Champions**: under 10% of buyers, with about 4 orders each, but **35% of revenue**. Give them a VIP tier and early access.
> - **New / Promising**: 17% who bought in the last 2 weeks. Send a welcome series and a second-order coupon.
> - **At-Risk**: 36% who spent well but haven't returned for about 3 months. They hold **43.5% of revenue**, so winning them back matters most.
> - **Lost**: 37% with one small $16 order. Use the newsletter only and don't spend ad money on them.
>
> On ethics, the data is anonymised (only numeric user IDs), so no personal details were used.
> **Limitations:** a 5-month snapshot and k chosen partly by judgement. **Future scope:** Spark Structured Streaming for live segments and adding browsing features. Thank you.

---

## 12. Live demo runbook

**Main idea:** the full pipeline takes **10+ minutes on Colab**, but the demo slot is about 90 seconds. So you **run the whole notebook before the presentation** and keep the tab open. In the demo you:
1. Scroll through the outputs that already exist, which shows the big-data work.
2. Run the **3 live cells** (section 9 of the notebook). Each finishes in a few seconds because the data is cached in Spark memory.

That way you show a real, working Spark session without waiting on a progress bar.

### 12.1 The day before (about 30 min)
1. **Upload the latest notebook** `BDA_Customer_Segmentation.ipynb` to Colab. It contains section **"9. Live demo cells"**, and `spark.stop()` at the end is commented out. An older copy won't have the live cells; use **File → Upload notebook** again.
2. Check that the 🔑 secret `KAGGLE_API_TOKEN` exists with **Notebook access** on.
3. Do a full **Runtime → Run all** once and check the numbers against section 12.5. The run takes about 10–15 minutes.
4. **Practise the demo twice** using section 12.3, with a timer.
5. Test on the presentation laptop: is Colab logged in, is the browser zoom at 125–150% so the back row can read the text, and does it work on the projector resolution?
6. Make sure the **backup** opens offline: `BDA_Customer_Segmentation_executed.ipynb` (VS Code or Jupyter), or just the slides.

### 12.2 On the day, 30–40 minutes before your slot
1. Open Colab → your notebook → **Runtime → Run all**. The runtime starts fresh, so it downloads the dataset again (1–2 minutes) and runs everything (about 10 minutes). Start it **at least 30 minutes early**.
2. When it's done, check that the last cells (section 9) printed tables with no errors, and that the Colab RAM/Disk indicator at the top right is green.
3. **Keep the tab active.** Free Colab disconnects after about 90 minutes idle, and sooner if the tab is closed. Click a cell every 15–20 minutes, or run `customers.count()` in a scratch cell.
4. Prepare the browser:
   - Colab in **one tab**, the slides in another window, so you can **Alt+Tab** between them.
   - Collapse the Colab sidebar (the ☰ icon) and close the file and secret panels.
   - **Hide the 🔑 Secrets panel.** Never show the token on screen.
   - Use **Ctrl + +** to zoom to about 125%.
5. **Pre-scroll** so that the first thing visible is the file-size output in section 1.
6. **Don't clear the outputs** (Edit → Clear all outputs). They are what you show.

### 12.3 The demo, click by click (about 90 s)
On slide 4 say: *"Let me show it running on Colab,"* then press **Alt+Tab**.

| # | Time | Do this | Point at | Say (short) |
|---|---|---|---|---|
| 1 | 0:00 | Top of section 1 output | `TOTAL 2.27 GB in 5 files`, `Raw events: 20,692,840` | "Spark read all five files, 2.27 GB and 20.7 million events, in one DataFrame." |
| 2 | 0:10 | Scroll to section 2 | `Exchange hashpartitioning` in the plan, then `Rows after cleaning: 19,493,208` | "This is the lazy plan: nothing ran until count(). The shuffle is the duplicate removal. Cleaning kept 19.5 million events." |
| 3 | 0:25 | Scroll to section 3 | The funnel table | "SparkSQL: 1.6 million users viewed, only 110 thousand bought." |
| 4 | 0:35 | Scroll to section 4 | The RFM table and summary | "One groupBy gives Recency, Frequency and Monetary for 110,518 buyers." |
| 5 | 0:45 | Scroll to section 6 | The k-loop lines and the elbow/Silhouette chart | "K-Means for k from 2 to 8; the elbow is at 4." |
| 6 | 0:55 | Go to section 9, click **LIVE 1**, press **Shift+Enter** | The segment table appears in about 2–5 s | "Now live: a SQL query on the segments. Champions earn a third of revenue." |
| 7 | 1:10 | Click **LIVE 3**, press **Shift+Enter** | Four predictions | "And I can score a brand-new customer with the trained model. A recent, frequent big spender is a Champion; one small old order is Lost." |
| 8 | 1:25 | **Alt+Tab** back to the slides, go to slide 5 | — | "So, the conclusion…" |

**LIVE 2** (looking up one buyer) is optional. Use it only if you have time or the examiner asks "show me a real customer".

**Tips**
- **Never press Ctrl+F9 (Run all)** during the demo. It would take 10 minutes. Use **Shift+Enter** on single cells only.
- Scroll with the mouse wheel, or jump with the **Table of contents** (the ☰ icon on the left lists the headings).
- If a cell takes a little longer, keep talking. Don't wait silently.

### 12.4 What the live cells print
These values come from the Colab run on 13 Nov; LIVE 3 was checked locally. A Colab run with the same Spark version and seed should reproduce them. Small differences in a rounded column (for example 14.0 vs 15.0) are fine. The cluster *number* (0–3) can differ between runs; the segment *name* is what matters.

**LIVE 1: SparkSQL on segments.** Say: "At-Risk is the biggest revenue pool, $2.76M, from buyers who haven't returned in about 3 months."
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

**LIVE 2: one real buyer (user 420629649).** Say: "9 orders, last one 7 days ago, $602 spent, so a Champion. Spark found their 1,523 events among 19.5 million."
```
+---------+-------+---------+--------+-------+---------+
|  user_id|Recency|Frequency|Monetary|Cluster|  Segment|
+---------+-------+---------+--------+-------+---------+
|420629649|      7|        9|   601.7|      3|Champions|
+---------+-------+---------+--------+-------+---------+

+----------------+------+-------+
|      event_type|events|  value|
+----------------+------+-------+
|            cart|   553|1988.43|
|            view|   422|4599.68|
|remove_from_cart|   417|1693.47|
|        purchase|   131|  601.7|
+----------------+------+-------+
```

**LIVE 3: score new customers.** Say: "The same log → assemble → scale → K-Means pipeline puts an unseen customer into a segment." To change an example, edit the numbers in the `for` line, for example `(3, 10, 900)`.
```
Recency=  5 days  Orders=6  Spend=$ 400  ->  Champions
Recency=  7 days  Orders=1  Spend=$  25  ->  New / Promising
Recency= 90 days  Orders=2  Spend=$ 150  ->  At-Risk
Recency=100 days  Orders=1  Spend=$  10  ->  Lost
```

### 12.5 Numbers to check after the pre-run
| Output | Expected |
|---|---|
| Total size | 2.27 GB in 5 files |
| Raw events | 20,692,840 |
| Rows after cleaning | 19,493,208 (5.8% removed) |
| Users / sessions | 1,637,770 / 4,520,284 |
| Purchasing users | 110,518 |
| Buyers with RFM | 110,518 |
| Silhouette at k = 4 | ≈ 0.49 |
| Segments | Champions 10,819 · New/Promising 18,732 · At-Risk 39,800 · Lost 41,167 |

These are the Colab (Spark 4.0.5) numbers, and the slides use them. The event and buyer counts must match exactly. If the **segment sizes** differ by a few dozen buyers on the day, that's harmless; say "the sizes vary slightly between runs because K-Means starts from random centres". If they differ a lot, download `outputs/results.json` from Colab into `outputs/` and rebuild the deck with `python tools/build_deck.py`.

### 12.6 If something goes wrong
| Problem | What to do on stage |
|---|---|
| Colab disconnected ("Reconnect" button) | **Don't re-run.** Say "the session timed out, here is the same run saved", and open `BDA_Customer_Segmentation_executed.ipynb` or the Colab copy that already shows the outputs. Then continue with the slides. |
| A live cell errors with `NameError` | The runtime was restarted and the variables are gone. Use the fallback above. |
| A live cell is slow (> 15 s) | Keep talking about what it does. If it's still running after 30 s, stop it (■) and move on to slide 5. |
| No internet or the projector won't show the browser | Stay on slide 4. Its picture shows the final segments. Describe the steps from the right-hand list. |
| An examiner asks to run a different query | In a new cell (**+ Code**) type, for example: `spark.sql("SELECT brand, COUNT(*) FROM events WHERE event_type='purchase' GROUP BY brand ORDER BY 2 DESC LIMIT 5").show()`. It takes a few seconds because `events` is cached. |
| An examiner asks to see the Spark plan | `customers.explain()` or `rfm.explain()` |
| An examiner asks how many partitions | `events.rdd.getNumPartitions()` |

### 12.7 Extra live queries (in case you're asked)
```python
# Top 5 brands bought by Champions (a join of the two views)
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
The join and `NOT IN` queries scan the 19.5M cached rows, so expect **10–40 s on Colab**. Run them only if asked.

---

## 13. Viva: likely questions and short answers

| Question | Answer |
|---|---|
| How big is the data? | 2.27 GB, 5 CSV files, 20,692,840 rows, 9 columns. After cleaning, 19,493,208 rows. |
| Why Spark and not pandas? | pandas loads everything into one machine's RAM in one thread. 2.3 GB of CSV becomes ~10+ GB in pandas memory. Spark splits the data into partitions (19 on Colab), processes them in parallel, spills to disk if needed, and the same code runs unchanged on a multi-node cluster. |
| Why give an explicit schema? | `inferSchema` makes Spark read the whole 2.3 GB once just to guess the types. An explicit `StructType` skips that pass and guarantees the correct types (for example, `user_id` as long). |
| What is lazy evaluation? | Transformations (`filter`, `withColumn`, `dropDuplicates`, `groupBy`) only build a DAG/plan. Nothing runs until an action (`count`, `show`, `toPandas`). The notebook shows the plan with `events.explain()` before `count()`. |
| Transformation vs action? | Transformations return a new DataFrame lazily: `filter`, `select`, `groupBy().agg()`. Actions trigger execution and return a result: `count`, `show`, `first`, `toPandas`. |
| Narrow vs wide transformation? | Narrow (`filter`, `withColumn`) works inside each partition. Wide (`groupBy`, `dropDuplicates`) needs a **shuffle** across partitions. You can see `Exchange hashpartitioning` in the `explain()` output. |
| Why `cache()`? | The cleaned 19.5M events are reused by every SparkSQL query and the RFM step. Without caching, Spark would re-read and re-clean the 2.3 GB each time. |
| What's a "Frequency" / an order here? | One `user_session` that contains purchase events. Every purchase row is one product, and rows in the same session form one order. |
| Why log-transform and scale? | Monetary ranges from $0.13 to $3,769 and Frequency from 1 to 122. K-Means uses Euclidean distance, so without log + StandardScaler the largest-range feature would decide every cluster. |
| How did you choose k? | With the elbow on WSSSE plus the Silhouette from `ClusteringEvaluator`. k = 2 has the best Silhouette (0.66), but "good vs bad buyers" is too coarse. WSSSE drops by 56k and then 36k up to k = 4, then only 21k and under 10k per step. k = 4 is also **stable**: up to k = 4 the clusters are identical on Spark 3.5 and 4.0, while k ≥ 5 changes between versions. k = 4 gives Silhouette 0.49 and four actionable personas. |
| The Silhouette is higher at k = 5–8 (0.50–0.52). Why not use those? | The gain over k = 4 (0.49) is small, and it isn't reliable: on local Spark 3.5, k = 5 scored only 0.45. Those solutions depend on the random starting centres. k = 2–4 gave identical results on both Spark versions. The elbow is also at 4, and four segments map to four clear marketing actions. |
| What does Silhouette mean? | For each point it compares the mean distance to its own cluster with the distance to the nearest other cluster. The range is −1 to 1, and higher is better. 0.49 is reasonable structure. |
| Why seed = 42? | K-Means starts from random centres (k-means\|\| in MLlib). A fixed seed makes the result reproducible. |
| How are clusters named? | With an explicit rule on the cluster averages: highest frequency → Champions; of the rest, most recent → New; of the last two, the higher spender → At-Risk, and the other → Lost. |
| At-Risk and Lost have the same recency. Why separate them? | Both are about 3 months inactive, but At-Risk buyers spent 4× more ($69 vs $16) and hold 43.5% of revenue. A win-back campaign is worth it for them and not for Lost. |
| What is an RDD / fault tolerance? | An RDD is an immutable, partitioned, distributed collection. Each one remembers its **lineage** (the steps that built it), so a lost partition is recomputed from the source instead of being stored twice. DataFrames run on top of RDDs. |
| Where is SparkSQL used? | The cleaned DataFrame is registered as the temp view `events`. The overview, funnel, monthly revenue and top-brands queries are plain SQL run by the same engine (Catalyst optimizer). |
| How is the name attached to every buyer? | With a broadcast join: the 4-row name table is copied to every executor, so no shuffle is needed. |
| Ethics / privacy? | The data is anonymised (numeric user IDs only, with no names or emails). In real use you would need consent for behavioural tracking and marketing, and should avoid unfair targeting (for example, worse prices for some segments). |
| Limitations? | A 5-month snapshot of one shop, K-Means assumes round clusters, k is partly judgement, RFM ignores browsing behaviour, and it runs on single-node Spark rather than a real cluster. |

---

## 14. Project files, rebuilding and known issues

### 14.1 Folder map
```
BDA-PROJECT/
├── BDA_Customer_Segmentation.ipynb               ← main notebook (GENERATED; edit tools/build_nb.py, not this)
├── BDA_Customer_Segmentation_executed.ipynb      ← local run with all outputs (backup for the demo)
├── BDA_Customer_Segmentation_run-by-colab.ipynb  ← downloaded from Colab
├── BDA_Customer_Segmentation_Presentation_v2.pptx← final 5-slide deck (GENERATED by tools/build_deck.py)
├── FINAL_PROJECT_EXPLANATION.md                  ← this file
├── PROJECT_REPORT.md · TECHNICAL_GUIDE.md · DEMO_GUIDE.md · PRESENTATION_SCRIPT.md
├── outputs/               ← charts, results.json, customer_segments.csv (local_spark35/ = Spark 3.5 run)
├── tools/
│   ├── build_nb.py        ← writes the notebook
│   ├── segment_text.py    ← k-choice text, naming rule, actions table (inserted into the notebook)
│   └── build_deck.py      ← writes the .pptx from outputs/results.json + charts
└── data/cosmetics/        ← the 5 CSVs (2.27 GB, not committed anywhere)
```

### 14.2 How to rebuild things
| Task | Command (run from the project folder) | Needs |
|---|---|---|
| Regenerate the notebook after changing `build_nb.py` | `cd tools && python build_nb.py` | Python with `nbformat` |
| Rebuild the deck after new results | `python tools/build_deck.py` | `python-pptx`, plus `outputs/results.json` and the PNGs |
| Rebuild the deck with a video on slide 4 | `python tools/build_deck.py demo.mp4` | as above |
| Re-run everything | Colab: upload the notebook → add the `KAGGLE_API_TOKEN` secret → Run all | Kaggle token |

**Workflow for a change:**
1. Edit `tools/build_nb.py`, or `tools/segment_text.py` if you only change text or the naming.
2. Regenerate the notebook and run it.
3. Copy the new `outputs/` (above all `results.json`) into the project.
4. Rebuild the deck.

The slide numbers come from `results.json`, so the deck and the notebook always agree.

**Common changes**
- **Different k:** change `K_FINAL = 4` in `build_nb.py`. The naming rule assumes 4 clusters, so update `NAMING_CODE` in `segment_text.py` too.
- **Less memory (if Colab crashes):** change `"spark.driver.memory", "6g"` to `"4g"`.
- **A subset of the data (faster tests):** in §1, use `files = files[:1]` to read only one month.
- **Another dataset:** change the schema, the cleaning rules and the RFM definition (§1–4). Everything after `data` stays the same.

### 14.3 Known issues and gotchas
| Problem | Cause | Fix |
|---|---|---|
| `SecretNotFoundError: KAGGLE_API_TOKEN` | The Colab secret is missing or misnamed | 🔑 Secrets → add `KAGGLE_API_TOKEN` exactly, and turn on Notebook access |
| Kaggle `404` / `401` | No token, or an expired token | Create a new API token on Kaggle → Settings |
| The Colab session crashes during cleaning | RAM limit (about 12 GB on free Colab) | Use 4g driver memory and restart |
| The k loop seems stuck | It trains 14 times (7 fits plus 7 Silhouette passes) on 2 cores | Wait. It takes several minutes, and each k prints a line when done |
| `NameError: spark` / `customers` after a disconnect | The Colab runtime was recycled | Run all again. The data download is also lost if the VM was reset |
| Running Spark on Windows hangs on a path with `*` | No Hadoop `winutils` | The notebook already passes an explicit file list. Prefer Colab anyway |
| Numbers differ slightly between Colab and local | Spark 4.0 (Colab) vs 3.5 (local); K-Means initialisation can differ | Verified 13 Nov: all counts match exactly, and the k = 4 segment sizes differ by under 50 buyers. k ≥ 5 results differ more. The deck uses the Colab results; the local ones are in `outputs/local_spark35/` |
| Partition count is 19 on Colab and 25 locally | Depends on cores and file split size | Expected. Not an error |

**Security:** the Kaggle token is a secret. Keep it in Colab Secrets or `~/.kaggle/access_token`, never in the notebook. Expire it after the project.

---

## 15. References
1. Hughes, A. M. (1994). *Strategic Database Marketing*. Probus.
2. MacQueen, J. (1967). Some methods for classification and analysis of multivariate observations. *Proc. 5th Berkeley Symp.*
3. Wu, X. et al. (2008). Top 10 algorithms in data mining. *Knowledge and Information Systems*, 14(1).
4. Chen, D., Sain, S. L., & Guo, K. (2012). Data mining for the online retail industry: a case study of RFM model-based customer segmentation. *J. Database Marketing & Customer Strategy Management*, 19(3).
5. Zaharia, M. et al. (2016). Apache Spark: a unified engine for big data processing. *Communications of the ACM*, 59(11).
6. Meng, X. et al. (2016). MLlib: Machine learning in Apache Spark. *JMLR*, 17(34).
7. Rousseeuw, P. J. (1987). Silhouettes: a graphical aid to the interpretation and validation of cluster analysis. *J. Computational and Applied Mathematics*, 20.
8. Kechinov, M. (2020). eCommerce Events History in Cosmetics Shop. Kaggle / REES46 Open CDP.
