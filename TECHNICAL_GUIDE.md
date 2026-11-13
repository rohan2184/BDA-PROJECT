# Technical Guide: how this project works and how to take it over

This guide is for anyone who has to run, explain or change the project, including a teammate taking it over. Read the sections in order. Section 1 covers the concepts and section 2 walks through the notebook cell by cell. Sections 3–5 cover the files, how to rebuild things and known problems.

- For results and business findings, see `PROJECT_REPORT.md`.
- For the presentation, see `DEMO_GUIDE.md` and `PRESENTATION_SCRIPT.md`.

---

## 1. Concepts you need (in plain words)

### 1.1 Apache Spark
- Spark is an engine that splits data into **partitions** and processes them **in parallel**, on the cores of one machine or across many machines.
- Here it runs in **local mode** (`master("local[*]")` uses every core on the Colab VM, which is 2 on free Colab). The same code would run on a cluster without changes.
- **SparkSession** is the entry point. **DataFrame** is a distributed table with named columns.
- **RDD** (Resilient Distributed Dataset) is the low-level structure underneath every DataFrame. It is immutable and partitioned, and it remembers its **lineage**, the steps that built it. If a partition is lost, Spark rebuilds it from the lineage, which is how Spark is fault tolerant.

### 1.2 Transformations vs actions, and lazy evaluation
| Transformations (lazy, return a new DataFrame) | Actions (run the job, return a result) |
|---|---|
| `filter`, `withColumn`, `select`, `dropDuplicates`, `groupBy().agg()`, `join` | `count()`, `show()`, `first()`, `collect()`, `toPandas()`, `write` |

- Transformations only build a **plan** (a DAG). Spark's Catalyst optimiser rewrites the plan, for example by pushing filters down to the file scan. Work starts only when an **action** is called.
- `events.explain()` prints that plan before anything has run.

### 1.3 Narrow vs wide transformations
- **Narrow** transformations (`filter`, `withColumn`) work inside each partition. No data moves between partitions.
- **Wide** transformations (`groupBy`, `dropDuplicates`, `join`, `orderBy`) need a **shuffle**, where rows with the same key are sent to the same partition. In `explain()` a shuffle shows up as `Exchange hashpartitioning(...)`.
- `spark.sql.shuffle.partitions = 32` sets how many partitions a shuffle produces. The default of 200 is too many for 2 cores.

### 1.4 Caching
- `.cache()` keeps a DataFrame in memory, spilling to disk if needed, **after the first action computes it**.
- The cleaned `events` DataFrame (19.5M rows) is reused by every SQL query and by the RFM step, so caching it saves re-reading and re-cleaning 2.27 GB each time.

### 1.5 SparkSQL
- `events.createOrReplaceTempView("events")` lets you query the DataFrame with plain SQL.
- `spark.sql("SELECT ...")` returns a DataFrame. It goes through the same optimiser, so SQL and the DataFrame API perform the same.

### 1.6 RFM
| Letter | Meaning | Here |
|---|---|---|
| R: Recency | How recently they bought | Days from the last purchase to 2020-03-01 |
| F: Frequency | How often they buy | Number of distinct purchase sessions (orders) |
| M: Monetary | How much they spend | Sum of `price` over purchase rows |

### 1.7 K-Means, WSSSE and Silhouette
- **K-Means** picks k centres and assigns each point to the nearest one. It then moves each centre to the mean of its points and repeats until nothing changes (or `maxIter` is reached). MLlib uses the **k-means||** method to choose good starting centres in parallel. `seed=42` makes the result reproducible.
- **WSSSE** is the sum of squared distances from each point to its centre. It always falls as k grows, so you look for the **elbow**, where it stops falling quickly.
- **Silhouette**, for a point, is (b − a) / max(a, b). Here a is the mean distance to its own cluster and b is the mean distance to the nearest other cluster. It ranges from −1 to 1, and higher means better-separated clusters. `ClusteringEvaluator` computes the average over all points.

### 1.8 Why log + scaling
K-Means uses distance. Monetary is in dollars (up to 3,769) and Frequency is a count (mostly 1), so without scaling Monetary would decide everything. `log1p` (that is, log(1 + x)) reduces the long tail, and `StandardScaler` gives every feature mean 0 and standard deviation 1.

---

## 2. Notebook walkthrough (`BDA_Customer_Segmentation.ipynb`)

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
| 9 | **Live demo cells:** ad-hoc SQL on the segments, look up one buyer, score a new customer | Reusing cached data and a trained model | See `DEMO_GUIDE.md` |
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

## 3. Folder map

```
BDA-PROJECT/
├── BDA_Customer_Segmentation.ipynb          ← main notebook (GENERATED; edit tools/build_nb.py, not this)
├── BDA_Customer_Segmentation_executed.ipynb ← local run with all outputs (backup for the demo)
├── BDA_Customer_Segmentation_run-by-colab.ipynb ← downloaded from Colab
├── BDA_Customer_Segmentation_Presentation.pptx  ← 5-slide deck (GENERATED by tools/build_deck.py)
├── PROJECT_REPORT.md      ← full write-up
├── TECHNICAL_GUIDE.md     ← this file
├── DEMO_GUIDE.md          ← live-demo runbook
├── PRESENTATION_SCRIPT.md ← what to say + viva Q&A
├── PHASE_PLAN.md          ← original plan and status
├── README.md              ← quick start
├── CLAUDE.md              ← instructions for Claude Code
├── outputs/               ← charts, results.json, customer_segments.csv
├── tools/
│   ├── build_nb.py        ← writes the notebook
│   ├── segment_text.py    ← k-choice text, naming rule, actions table (inserted into the notebook)
│   └── build_deck.py      ← writes the .pptx from outputs/results.json + charts
└── data/cosmetics/        ← the 5 CSVs (2.27 GB, not committed anywhere)
```

---

## 4. How to rebuild things

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

### Common changes
- **Different k:** change `K_FINAL = 4` in `build_nb.py`. The naming rule assumes 4 clusters, so update `NAMING_CODE` in `segment_text.py` too.
- **Less memory (if Colab crashes):** change `"spark.driver.memory", "6g"` to `"4g"`.
- **A subset of the data (faster tests):** in §1, use `files = files[:1]` to read only one month.
- **Another dataset:** change the schema, the cleaning rules and the RFM definition (§1–4). Everything after `data` stays the same.

---

## 5. Known issues and gotchas

| Problem | Cause | Fix |
|---|---|---|
| `SecretNotFoundError: KAGGLE_API_TOKEN` | The Colab secret is missing or misnamed | 🔑 Secrets → add `KAGGLE_API_TOKEN` exactly, and turn on Notebook access |
| Kaggle `404` / `401` | No token, or an expired token | Create a new API token on Kaggle → Settings |
| The Colab session crashes during cleaning | RAM limit (about 12 GB on free Colab) | Use 4g driver memory and restart |
| The k loop seems stuck | It trains 14 times (7 fits plus 7 Silhouette passes) on 2 cores | Wait. It takes several minutes, and each k prints a line when done |
| `NameError: spark` / `customers` after a disconnect | The Colab runtime was recycled | Run all again. The data download is also lost if the VM was reset |
| Running Spark on Windows hangs on a path with `*` | No Hadoop `winutils` | The notebook already passes an explicit file list. Prefer Colab anyway |
| Numbers differ slightly between Colab and local | Spark 4.0 (Colab) vs 3.5 (local); K-Means initialisation can differ | Counts must match exactly. Cluster sizes may shift slightly, so rebuild the deck from the Colab `results.json` |
| Partition count is 19 on Colab and 25 locally | Depends on cores and file split size | Expected. Not an error |

**Security:** the Kaggle token is a secret. Keep it in Colab Secrets or `~/.kaggle/access_token`, never in the notebook. Expire it after the project.
