# Project Report: Customer Segmentation of E-Commerce Big Data using PySpark K-Means (RFM)

**Course:** Big Data Analytics, Sem VII CSE/ICT, Adani University
**Reference topic:** #34, Customer Segmentation Using Scalable Clustering Techniques
**Author:** Rohan Upadhyay
**Environment:** Google Colab, Apache Spark (PySpark 3.5 locally, 4.0 on Colab), Spark SQL, Spark MLlib
**Notebook:** `BDA_Customer_Segmentation.ipynb`

---

## Abstract

An online cosmetics shop logged **20.7 million clickstream events (2.27 GB)** over five months, but it markets to every buyer in the same way. This project uses **Apache Spark** to process the full log:

- It cleans the data and explores it with **SparkSQL**.
- It computes **Recency, Frequency and Monetary (RFM)** values for each of **110,518 buyers**.
- It clusters the buyers with **Spark MLlib K-Means**.

The elbow method and the Silhouette score point to **k = 4** (Silhouette 0.49). The four segments are Champions, New / Promising, At-Risk and Lost.

The key finding is that **Champions are under 10% of buyers but earn 34.7% of revenue**. **At-Risk** buyers haven't purchased for about 3 months, yet they hold **43.5% of revenue**, so a win-back campaign aimed at them is the highest-value action.

---

## 1. Introduction

### 1.1 Problem statement
E-commerce sites collect huge event logs: every product view, add-to-cart, removal and purchase. A single marketing message for all customers wastes budget. Loyal customers get discounts they don't need, and lapsing customers are ignored. The shop needs to **group customers by behaviour** so that each group gets a suitable action.

### 1.2 Why this is a Big Data problem
| "V" | In this project |
|---|---|
| **Volume** | 2.27 GB of CSV, 20,692,840 rows, 5 files. That's too large for comfortable single-threaded pandas work: it would need roughly 10 GB of RAM. |
| **Velocity** | Clickstream events arrive continuously, about 4 million a month here. |
| **Variety** | Semi-structured events with missing `category_code`/`brand`, four event types, and timestamps stored as text. |

### 1.3 Objectives
1. Ingest and clean the full 2.27 GB log with Spark DataFrames.
2. Explore it with SparkSQL: the event funnel, monthly revenue and top brands.
3. Engineer RFM features for each buyer with distributed aggregations.
4. Cluster buyers with MLlib K-Means, choosing k with the elbow and Silhouette methods.
5. Turn the clusters into named segments with marketing actions.

---

## 2. Literature Review

| Work | Contribution | How this project uses it |
|---|---|---|
| Hughes (1994), *Strategic Database Marketing* | Introduced the **RFM model**. Customers who bought recently, buy often and spend more are worth more. | The three features. |
| MacQueen (1967); Wu et al. (2008), "Top 10 algorithms in data mining" | **K-Means** partitions points into k groups by minimising the within-cluster squared distance. It is one of the top 10 data-mining algorithms. | The clustering algorithm. |
| Chen, Sain & Guo (2012), *J. Database Marketing & Customer Strategy Mgmt.* | Segmented an online retailer's customers with RFM + K-Means. | The same method, on a dataset about 40 times larger (a clickstream instead of invoices). |
| Zaharia et al. (2016), "Apache Spark: a unified engine for big data processing", *CACM* | In-memory, fault-tolerant distributed processing using RDDs and lineage. | The engine. |
| Meng et al. (2016), "MLlib: Machine Learning in Apache Spark", *JMLR* | Scalable ML pipelines on Spark, including the k-means\|\| initialisation. | VectorAssembler, StandardScaler, KMeans, ClusteringEvaluator. |
| Rousseeuw (1987), "Silhouettes", *J. Comp. & Applied Math.* | The Silhouette coefficient for checking cluster quality. | Choosing k. |

---

## 3. Dataset

**eCommerce Events History in Cosmetics Shop** (REES46 Open CDP), Kaggle `mkechinov/ecommerce-events-history-in-cosmetics-shop`.

| Property | Value |
|---|---|
| Files | `2019-Oct.csv`, `2019-Nov.csv`, `2019-Dec.csv`, `2020-Jan.csv`, `2020-Feb.csv` |
| Size | **2.27 GB** (CSV), about 0.5 GB zipped |
| Rows | **20,692,840** events |
| Period | 1 Oct 2019 – 29 Feb 2020 |
| Columns | `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session` |

`event_type` is one of `view`, `cart`, `remove_from_cart` or `purchase`. Users are anonymised as numeric IDs, so the data contains no personal information.

---

## 4. Methodology

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

### 4.1 Ingestion
- All five files are read into **one DataFrame** with an **explicit schema**. `inferSchema` would need an extra full pass over 2.27 GB just to guess the column types.
- Spark splits the input into partitions: 25 locally and 19 on Colab, depending on core count and file splits. The partitions are processed in parallel.

### 4.2 Cleaning
| Rule | Reason |
|---|---|
| Parse `event_time` (strip `" UTC"`, `to_timestamp`) | Needed for date arithmetic. |
| Drop null `user_id` / `user_session` | Can't attribute the event to a customer or an order. |
| Drop `price <= 0` | These are free samples or bad data and would distort Monetary. |
| `dropDuplicates()` | The raw log contains repeated identical rows. This is a *wide* transformation (a shuffle). |

The result is **19,493,208 rows**, so **1,199,632 rows (5.8%)** were removed.

All of these are **lazy transformations**. `events.explain()` prints the physical plan, including `Exchange hashpartitioning` (the shuffle) before anything runs. The `count()` **action** then runs the plan. The result is **cached** because every later step reuses it.

### 4.3 Exploratory analysis (SparkSQL)
The cleaned DataFrame is registered as the temp view `events`. The queries cover:
- An overview with distinct users, sessions, products and brands.
- The event funnel by `event_type`.
- Monthly purchase revenue.
- The top 10 brands by revenue.

### 4.4 RFM feature engineering
Only `purchase` events are used (1,285,982 rows). One purchase row is one product, and **one order is one `user_session`**.

| Feature | Definition |
|---|---|
| Reference date | last event date + 1 day = **2020-03-01** |
| Recency | `datediff(ref_date, max(purchase date))`, in days. Lower is better. |
| Frequency | `countDistinct(user_session)`, the number of orders |
| Monetary | `sum(price)`, in $ |

### 4.5 Scaling
All three features are very right-skewed. Most buyers order once, Frequency ranges from 1 to 122, and Monetary from $0.13 to $3,769. K-Means uses Euclidean distance, so:
1. `log1p` reduces the skew.
2. `VectorAssembler` combines `[R_log, F_log, M_log]` into one vector.
3. `StandardScaler(withMean=True, withStd=True)` gives each feature mean 0 and standard deviation 1, so that no single feature dominates the distance.

### 4.6 Choosing k
K-Means was trained for k = 2..8 (`seed=42`, `maxIter=50`). Each model was scored with:
- **WSSSE**, the within-cluster sum of squared errors (`model.summary.trainingCost`), for the elbow method.
- The **Silhouette** score from `ClusteringEvaluator` (squared Euclidean distance).

| k | WSSSE | Silhouette |
|---|---|---|
| 2 | 209,251 | **0.662** |
| 3 | 153,483 | 0.548 |
| **4** | **117,250** | **0.490** |
| 5 | 104,816 | 0.452 |
| 6 | 84,313 | 0.504 |
| 7 | 76,666 | 0.472 |
| 8 | 71,365 | 0.507 |

**Decision: k = 4.**
- k = 2 has the best Silhouette but only splits buyers into "good" and "bad", which is too coarse to act on.
- WSSSE falls by 56k from k = 2 to 3 and by 36k from 3 to 4, then by only 12k from 4 to 5. That bend is the elbow.
- At k = 4 the Silhouette is 0.49, which indicates reasonable structure, and the clusters correspond to four distinct personas.

### 4.7 Naming the clusters
The naming rule is deterministic and explainable, and it is applied to the cluster averages:
1. Highest average Frequency → **Champions**
2. Of the rest, lowest average Recency → **New / Promising**
3. Of the last two, higher average Monetary → **At-Risk**, and the other → **Lost**

The names are attached to all 110,518 buyers with a **broadcast join**: a small lookup table is copied to every executor, so no shuffle is needed.

---

## 5. Results

### 5.1 Dataset overview
| Metric | Value |
|---|---|
| Users / sessions | 1,637,770 / 4,520,284 |
| Products / brands | 53,903 / 273 |
| Purchase revenue | $6.35M |
| Peak month | Nov 2019 ($1.53M, the month that includes Black Friday) |

### 5.2 Event funnel
| Stage | Events | Distinct users |
|---|---|---|
| view | 9,626,268 | 1,596,359 |
| cart | 5,612,289 | 397,390 |
| remove_from_cart | 2,968,669 | 183,002 |
| purchase | 1,285,982 | **110,518** |

**6.9%** of users who viewed a product made a purchase.

### 5.3 Segments (k = 4, Silhouette 0.4895)
| Segment | Buyers | Share | Avg Recency (days) | Avg Orders | Avg Spend | Revenue | Revenue share |
|---|---|---|---|---|---|---|---|
| Champions | 10,835 | 9.8% | 35.0 | 4.00 | $203.3 | $2.20M | **34.7%** |
| New / Promising | 18,715 | 16.9% | 14.5 | 1.16 | $39.1 | $0.73M | 11.5% |
| At-Risk | 39,845 | 36.1% | 87.6 | 1.22 | $69.3 | $2.76M | **43.5%** |
| Lost | 41,123 | 37.2% | 89.4 | 1.02 | $15.9 | $0.65M | 10.3% |

Charts are in `outputs/`:
- `eda_event_funnel.png`, `eda_monthly_revenue.png`, `eda_top_brands.png`
- `rfm_distributions.png`
- `k_selection.png`
- `segment_sizes.png`, `segments_scatter.png`, `segments_scatter_fm.png`

### 5.4 Business actions
| Segment | Action |
|---|---|
| Champions | VIP or loyalty tier, early access, referral rewards |
| New / Promising | Welcome email series and a second-order coupon |
| At-Risk | Personalised win-back offers using their favourite brands |
| Lost | Newsletter only, with no paid-ads spend |

---

## 6. Discussion

- **Pareto effect.** About 10% of buyers (Champions) bring in about 35% of revenue, which is consistent with the RFM literature.
- **At-Risk vs Lost.** Both segments are about 3 months inactive, but At-Risk buyers spent **4 times more** on average. Recency alone can't separate them, but the clustering on all three features can.
- **Very low repeat rate.** The median Frequency is 1. This is typical of cosmetics e-commerce, and it is the reason the New → second-order conversion matters.
- **Why Spark.** A single `groupBy` over 1.29M purchase rows and a cached 19.5M-row DataFrame keep every step interactive. The same code would run unchanged on a multi-node cluster.

## 7. Ethics and privacy
- The data is anonymised: numeric user IDs only, with no names, emails or addresses.
- In real use, behavioural tracking and targeted marketing need user consent (GDPR / India DPDP Act 2023).
- The segments should be used to *serve* customers better. They must not be used to discriminate, for example by charging higher prices to loyal customers.

## 8. Limitations
- It covers a 5-month snapshot of a single shop, so seasonality beyond Black Friday isn't captured.
- The choice of k involves judgement, and K-Means assumes compact, round clusters.
- RFM ignores browsing, cart abandonment, product mix and returns.
- It runs on single-node Spark (Colab, 2 cores) rather than a real cluster.

## 9. Future scope
- Use **Spark Structured Streaming** to update the segments as new events arrive.
- Add behavioural features such as views, cart-abandonment rate and brand affinity.
- Compare against **Bisecting K-Means** and **Gaussian Mixture** in MLlib.
- Deploy on a multi-node cluster (Databricks / EMR) with the full multi-category REES46 data (tens of GB).

## 10. Conclusion
Spark processed 2.27 GB of clickstream data end to end: ingestion, cleaning, SQL analytics, feature engineering and MLlib clustering. The result is four actionable customer segments. The single most valuable action is a win-back campaign for the **At-Risk** segment (43.5% of revenue).

## 11. Syllabus mapping
| Unit | Topic | Where |
|---|---|---|
| 1 | Big Data characteristics (3 V's) | §1.2, the 2.27 GB / 20.7M-row dataset |
| 2 | Distributed and batch processing | Spark partitions, parallel batch job |
| 4 | SparkSession, DataFrames, transformations and actions, lazy evaluation, caching, SparkSQL | §4.1–4.4 |
| 5 | Preprocessing, scalable ML, clustering, MLlib | §4.5–4.7 |
| 6 | Big Data in e-commerce, privacy and ethics | §5.4, §7 |

Course outcomes: **CO1, CO2, CO4, CO5**.

## References
1. Hughes, A. M. (1994). *Strategic Database Marketing*. Probus.
2. MacQueen, J. (1967). Some methods for classification and analysis of multivariate observations. *Proc. 5th Berkeley Symp.*
3. Wu, X. et al. (2008). Top 10 algorithms in data mining. *Knowledge and Information Systems*, 14(1).
4. Chen, D., Sain, S. L., & Guo, K. (2012). Data mining for the online retail industry: a case study of RFM model-based customer segmentation. *J. Database Marketing & Customer Strategy Management*, 19(3).
5. Zaharia, M. et al. (2016). Apache Spark: a unified engine for big data processing. *Communications of the ACM*, 59(11).
6. Meng, X. et al. (2016). MLlib: Machine learning in Apache Spark. *JMLR*, 17(34).
7. Rousseeuw, P. J. (1987). Silhouettes: a graphical aid to the interpretation and validation of cluster analysis. *J. Computational and Applied Mathematics*, 20.
8. Kechinov, M. (2020). eCommerce Events History in Cosmetics Shop. Kaggle / REES46 Open CDP.
