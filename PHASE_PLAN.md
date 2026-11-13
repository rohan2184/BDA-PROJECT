# BDA Mini Project — Phase Plan

**Project:** Customer Segmentation of E-Commerce Data using PySpark K-Means (RFM Analysis)
**Reference topic:** #34 — "Customer Segmentation Using Scalable Clustering Techniques"
**Environment:** Google Colab (PySpark)
**Effort:** 3 sessions × ~2 hours
**Presentation:** make-up slot, 16 or 17 November, 10–11 AM (5 slides, 5 minutes)

---

## 1. Problem Statement

An online cosmetics shop logs over 2 crore (20M) clickstream events but treats every customer the same way. We use Apache Spark to calculate each customer's **Recency, Frequency and Monetary (RFM)** values. Then we cluster customers with **K-Means (Spark MLlib)** into segments such as *Champions*, *Loyal*, *At-Risk* and *Lost*, so marketing can target each group differently.

## 2. Syllabus Mapping

| Unit | Syllabus topic | Where it appears in the project |
|---|---|---|
| 1 – Intro to Big Data | Volume, Velocity, Variety | 2.27 GB / 20.7M-row clickstream log (5 files) |
| 2 – Frameworks | Distributed processing, batch processing | Spark runs the batch job in parallel partitions |
| 4 – Apache Spark | SparkSession, DataFrames, transformations, actions, lazy evaluation, SparkSQL | Loading, cleaning, RFM aggregation, SQL queries |
| 5 – Analytics Techniques | Preprocessing, scalable ML, clustering, MLlib | VectorAssembler, StandardScaler, KMeans, Silhouette score |
| 6 – Applications | Big Data in E-commerce, privacy/ethics | Business use case; data is anonymised (CustomerID only) |

Course outcomes covered: **CO1, CO2, CO4, CO5**.

## 3. Dataset & Tools

- **Dataset (changed 12 Nov 2026 to meet the 1–4 GB rule):** Kaggle *eCommerce Events History in Cosmetics Shop* (REES46), 5 monthly CSVs, Oct 2019 – Feb 2020, **2.27 GB, 20,692,840 events**, 9 columns
  - Download: https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop (the notebook downloads it with the Kaggle API)
  - Columns: `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`
  - RFM is computed from `purchase` events; one order = one `user_session`
  - *(The original UCI Online Retail choice was 48 MB, too small for the rule.)*
- **Tools:** Google Colab, PySpark, pandas, matplotlib/seaborn (charts only)
- **Pipeline:**
  1. Ingest the CSV into a Spark DataFrame
  2. Clean the data (drop nulls, cancellations and negative quantities)
  3. Feature engineering: compute RFM per customer
  4. Scale the features with StandardScaler
  5. Choose k using the elbow method and Silhouette score
  6. Train K-Means
  7. Profile and name the clusters
  8. Visualise the results

---

## 4. Session 1 (~2 hrs): Setup, Ingestion, Cleaning, EDA

**Goal:** a cleaned Spark DataFrame and basic insights.

- [x] Create the Colab notebook `BDA_Customer_Segmentation.ipynb` and share it with the group
- [x] `!pip install pyspark` and create a `SparkSession`
- [x] Download the dataset via the Kaggle API and load all 5 CSVs with `spark.read.csv(files, header=True, schema=...)` (explicit schema, no inferSchema pass over 2.3 GB)
- [x] `printSchema()`, `count()`, `show(5)` (note the row count for the slides)
- [x] Cleaning:
  - drop rows where `CustomerID` is null
  - remove cancelled invoices (`InvoiceNo` starts with `"C"`)
  - keep only `Quantity > 0` and `UnitPrice > 0`
  - add column `TotalPrice = Quantity * UnitPrice`
  - parse `InvoiceDate` to a timestamp
- [x] EDA with **SparkSQL** (`createOrReplaceTempView("events")`; done as event funnel, monthly revenue, top brands):
  - top 10 countries by revenue
  - top 10 products by quantity sold
  - monthly revenue trend
- [x] Save 2–3 EDA charts as PNG for the slides

**Done when:** you have a clean DataFrame with row counts before/after cleaning and 3 SparkSQL query results.

## 5. Session 2 (~2 hrs): RFM + K-Means Clustering

**Goal:** a trained model with named customer segments.

- [x] Reference date = max(`event_time`) + 1 day
- [x] Compute RFM on purchase events using `groupBy("user_id").agg(...)` (Frequency = distinct `user_session`):
  - **Recency** = days since last purchase
  - **Frequency** = count of distinct `InvoiceNo`
  - **Monetary** = sum of `TotalPrice`
- [x] Optional: log-transform Frequency and Monetary to reduce skew
- [x] `VectorAssembler` → `StandardScaler` (MLlib)
- [x] Loop k = 2…8: record the training cost (elbow) and Silhouette score with `ClusteringEvaluator`, then plot both
- [x] Pick k (usually 4) and train the final `KMeans(k=4, seed=42)`
- [x] Profile the clusters: average R, F, M and customer count per cluster
- [x] Name each cluster (e.g., low R + high F + high M = **Champions**; high R + low F = **Lost**)
- [x] Charts: elbow plot, cluster size bar chart, R vs M scatter coloured by cluster
- [x] Optional: export segments to CSV (`customer_segments.csv`)

**Done when:** you have the cluster summary table, cluster names and 3 charts saved.

## 6. Session 3 (~2 hrs): Slides + Working Model Video

**Goal:** a 5-slide deck and a short demo video, ready for the evaluation.

- [x] **Slide 1 – Introduction:** problem, dataset, objective
- [x] **Slide 2 – Literature Review:** 3–4 one-line points (RFM model by Hughes, 1994; K-Means for customer segmentation; Spark MLlib for scalable clustering; Chen et al., 2012, the original paper on the Online Retail dataset)
- [x] **Slide 3 – Methodology:** pipeline diagram (Ingest → Clean → RFM → Scale → K-Means → Profile)
- [x] **Slide 4 – Working Model:** changed (13 Nov) from a recorded video to a **live Colab demo**. The notebook is pre-run, then 3 quick live cells are run on stage (see `DEMO_GUIDE.md`)
- [x] **Slide 5 – Conclusion:** the segments found, business actions per segment, limitations (static data, k chosen manually) and future scope (Spark Streaming, more features)
- [ ] Rehearse once and stay under 5 minutes (about 1 minute per slide)
- [x] Each member owns at least one slide (for viva): solo project, Rohan presents all 5

**Done when:** you have the deck (`.pptx`), the video embedded and one rehearsal finished.

---

## 7. Deliverables Checklist

- [x] `BDA_Customer_Segmentation.ipynb` (runs top to bottom)
- [x] `customer_segments.csv` (optional)
- [x] Charts (EDA + clustering) as PNG
- [x] 5-slide presentation (slide 4 = live demo)

## 8. Likely Viva Questions

1. Why Spark instead of pandas? → It runs distributed, in-memory processing that scales to data that doesn't fit on one machine.
2. What is lazy evaluation? → Transformations build a DAG, and nothing executes until an action (e.g., `count()`, `show()`) is called.
3. Transformation vs action? → `filter`, `groupBy`, `withColumn` vs `count`, `collect`, `show`.
4. Why scale features before K-Means? → K-Means uses Euclidean distance, so without scaling Monetary would dominate.
5. How did you choose k? → Elbow method plus the highest Silhouette score.
6. What is an RDD and how does Spark tolerate faults? → An RDD is an immutable distributed dataset; lost partitions are recomputed from the lineage.
7. Ethical concerns? → Customer privacy (the data is anonymised), consent, and avoiding unfair targeting.

---

## 9. Status (12 Nov 2026)

- Done: notebook (verified end to end on local PySpark 3.5), charts, `customer_segments.csv`, 5-slide deck with speaker notes, `PRESENTATION_SCRIPT.md`, `README.md`.
- 13 Nov: added live-demo cells (notebook section 9), `PROJECT_REPORT.md`, `TECHNICAL_GUIDE.md` and `DEMO_GUIDE.md`.
- Remaining (Rohan): finish one full Colab run and check the numbers, practise the live demo (`DEMO_GUIDE.md`), and rehearse once.
