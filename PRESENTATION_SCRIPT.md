# Presentation Script: 5 slides in 5 minutes

The same text is in each slide's **speaker notes** in the `.pptx`. Speak it, don't read it. The timings are targets.

---

## Slide 1: Introduction (≈ 50 s)

> Good morning. My project is **customer segmentation of e-commerce big data using PySpark K-Means**, reference topic 34.
>
> The problem: an online cosmetics shop records millions of clicks, but it markets to every buyer in the same way.
>
> The dataset is the **REES46 e-commerce events log** from Kaggle: **five monthly CSV files, 2.27 GB, 20.7 million events**, October 2019 to February 2020. Every row is a view, add-to-cart, remove or purchase.
>
> My objective is to use **Spark** to compute **Recency, Frequency and Monetary** values for every buyer, then use **MLlib K-Means** to group them into segments marketing can act on.

## Slide 2: Literature Review (≈ 50 s)

> - **Hughes, 1994**, introduced the **RFM model**: customers who bought recently, buy often and spend more are the most valuable.
> - **K-Means** (MacQueen, 1967) is one of the top-10 data-mining algorithms (Wu et al., 2008) and the usual choice for clustering RFM values.
> - **Chen, Sain and Guo, 2012**, segmented an online retailer's customers with RFM and K-Means. I apply the same idea to a clickstream log that is about 40 times larger.
> - **Spark and MLlib** (Zaharia et al., 2016; Meng et al., 2016) run this in memory and in parallel, which lets the method scale to gigabytes.

## Slide 3: Methodology (≈ 70 s)

> The whole pipeline runs in Spark.
> 1. **Ingest:** Spark reads all five CSVs into one DataFrame of 25 partitions, processed in parallel. I give an explicit schema, so Spark doesn't need an extra pass over 2.3 GB to guess the types.
> 2. **Clean:** parse the timestamp and drop missing users, zero prices and duplicate events. That leaves **19.5 million events** (5.8% removed). Spark is **lazy**: these steps only build a plan, which runs at the `count()` action. The cleaned data is **cached**, because every later step reuses it.
> 3. **EDA** with **SparkSQL**: the view → cart → purchase funnel, monthly revenue and top brands.
> 4. **RFM:** keep the 1.29 million purchase events, then one `groupBy(user_id).agg(...)` gives **110,518 buyers**.
> 5. **Scale:** the values are very skewed, so I apply **log1p**, then **VectorAssembler** and **StandardScaler**.
> 6. **K-Means for k = 2 to 8.** Silhouette is highest at 2, but that is too coarse. The error curve bends at **4** with Silhouette 0.49, so the final model is **KMeans(k = 4, seed = 42)**.

## Slide 4: Working Model, live demo (≈ 90 s)

> *(Alt+Tab to Colab. The notebook was run before the session, so every output is already there. Follow `DEMO_GUIDE.md` section C.)*
> "Let me show it running on Colab. Spark read all five files, 2.27 GB and 20.7 million events. Here is the lazy plan, and cleaning kept 19.5 million events. The SparkSQL funnel shows that 1.6 million users viewed products but only 110 thousand bought, which is 6.9%. One groupBy builds the RFM table, and the K-Means loop for k from 2 to 8 gives the elbow at 4.
> Now live: *(Shift+Enter on LIVE 1)* a SQL query on the segments; Champions earn about a third of revenue. *(Shift+Enter on LIVE 3)* And the trained model scores a brand-new customer: recent, frequent big spenders are Champions, one small old order is Lost."
> *(Alt+Tab back to the slides.)*

## Slide 5: Conclusion (≈ 50 s)

> Spark K-Means found four segments among 110 thousand buyers:
> - **Champions**: under 10% of buyers, with about 4 orders each, but **35% of revenue**. Give them a VIP tier and early access.
> - **New / Promising**: 17% who bought in the last 2 weeks. Send a welcome series and a second-order coupon.
> - **At-Risk**: 36% who spent well but haven't returned for about 3 months. They hold **43.5% of revenue**, so winning them back matters most.
> - **Lost**: 37% with one small $16 order. Use the newsletter only and don't spend ad money on them.
>
> On ethics, the data is anonymised (only numeric user IDs), so no personal details were used.
> **Limitations:** a 5-month snapshot and k chosen partly by judgement. **Future scope:** Spark Structured Streaming for live segments and adding browsing features. Thank you.

---

## Viva: likely questions and short answers

| Question | Answer |
|---|---|
| How big is the data? | 2.27 GB, 5 CSV files, 20,692,840 rows, 9 columns. After cleaning, 19,493,208 rows. |
| Why Spark and not pandas? | pandas loads everything into one machine's RAM in one thread. 2.3 GB of CSV becomes ~10+ GB in pandas memory. Spark splits the data into partitions (25 here), processes them in parallel, spills to disk if needed, and the same code runs unchanged on a multi-node cluster. |
| Why give an explicit schema? | `inferSchema` makes Spark read the whole 2.3 GB once just to guess the types. An explicit `StructType` skips that pass and guarantees the correct types (for example, `user_id` as long). |
| What is lazy evaluation? | Transformations (`filter`, `withColumn`, `dropDuplicates`, `groupBy`) only build a DAG/plan. Nothing runs until an action (`count`, `show`, `toPandas`). The notebook shows the plan with `events.explain()` before `count()`. |
| Transformation vs action? | Transformations return a new DataFrame lazily: `filter`, `select`, `groupBy().agg()`. Actions trigger execution and return a result: `count`, `show`, `first`, `toPandas`. |
| Narrow vs wide transformation? | Narrow (`filter`, `withColumn`) works inside each partition. Wide (`groupBy`, `dropDuplicates`) needs a **shuffle** across partitions. You can see `Exchange hashpartitioning` in the `explain()` output. |
| Why `cache()`? | The cleaned 19.5M events are reused by every SparkSQL query and the RFM step. Without caching, Spark would re-read and re-clean the 2.3 GB each time. |
| What's a "Frequency" / an order here? | One `user_session` that contains purchase events. Every purchase row is one product, and rows in the same session form one order. |
| Why log-transform and scale? | Monetary ranges from $0.13 to $3,769 and Frequency from 1 to 122. K-Means uses Euclidean distance, so without log + StandardScaler the largest-range feature would decide every cluster. |
| How did you choose k? | With the elbow on WSSSE plus the Silhouette from `ClusteringEvaluator`. k = 2 has the best Silhouette (0.66), but "buyers vs non-buyers" is too coarse. WSSSE drops by 56k and then 36k up to k = 4, then only 21k and under 10k per step. k = 4 is also **stable**: up to k = 4 the clusters are identical on Spark 3.5 and 4.0, while k ≥ 5 changes between versions. k = 4 gives Silhouette 0.49 and four actionable personas. |
| The Silhouette is higher at k = 5–8 (0.50–0.52). Why not use those? | The gain over k = 4 (0.49) is small, and it isn't reliable: on local Spark 3.5, k = 5 scored only 0.45. Those solutions depend on the random starting centres. k = 2–4 gave identical results on both Spark versions. The elbow is also at 4, and four segments map to four clear marketing actions. |
| What does Silhouette mean? | For each point it compares the mean distance to its own cluster with the distance to the nearest other cluster. The range is −1 to 1, and higher is better. 0.49 is reasonable structure. |
| Why seed = 42? | K-Means starts from random centres (k-means\|\| in MLlib). A fixed seed makes the result reproducible. |
| How are clusters named? | With an explicit rule on the cluster averages: highest frequency → Champions; of the rest, most recent → New; of the last two, the higher spender → At-Risk, and the other → Lost. |
| At-Risk and Lost have the same recency. Why separate them? | Both are about 3 months inactive, but At-Risk buyers spent 4× more ($69 vs $16) and hold 43.5% of revenue. A win-back campaign is worth it for them and not for Lost. |
| What is an RDD / fault tolerance? | An RDD is an immutable, partitioned, distributed collection. Each one remembers its **lineage** (the steps that built it), so a lost partition is recomputed from the source instead of being stored twice. DataFrames run on top of RDDs. |
| Where is SparkSQL used? | The cleaned DataFrame is registered as the temp view `events`. The overview, funnel, monthly revenue and top-brands queries are plain SQL run by the same engine (Catalyst optimizer). |
| Ethics / privacy? | The data is anonymised (numeric user IDs only, with no names or emails). In real use you would need consent for behavioural tracking and marketing, and should avoid unfair targeting (for example, worse prices for some segments). |
| Limitations? | A 5-month snapshot of one shop, K-Means assumes round clusters, k is partly judgement, RFM ignores browsing behaviour, and it runs on single-node Spark rather than a real cluster. |
