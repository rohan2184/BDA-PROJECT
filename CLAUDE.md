# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

This is a university mini project for the Big Data Analytics course (Adani University, Sem VII CSE/ICT). It is not a software product yet. It holds the project plan, the Colab notebook, the slide deck and the project documents.

- `FINAL_PROJECT_EXPLANATION.md`: the single, complete explanation of the project (report, technical guide, slides, demo runbook and viva answers in one file).
- `PHASE_PLAN.md`: the source of truth for scope, schedule and deliverables. Read it before doing any work.

## Project decisions (already made with the user)

- **Topic:** #34, Customer Segmentation using PySpark K-Means on RFM (Recency, Frequency, Monetary) features.
- **Dataset (changed 12 Nov 2026):** the project rule requires 1–4 GB of data, so the project uses Kaggle *eCommerce Events History in Cosmetics Shop* (REES46, `mkechinov/ecommerce-events-history-in-cosmetics-shop`): 5 monthly CSVs (Oct 2019–Feb 2020), ~2.3 GB, ~20M events, stored in `data/cosmetics/`. Columns: `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`. RFM is computed from `purchase` events (order = `user_session`). The older UCI Online Retail version (48 MB) was dropped because it is too small. Kaggle access uses an API token (`~/.kaggle/access_token` locally, the Colab secret `KAGGLE_API_TOKEN` on Colab).
- **Environment:** Google Colab with `pip install pyspark`. Do not plan around local Hadoop or a local Spark install on Windows.
- **Budget:** 3 sessions of about 2 hours each. Keep everything small and fast; avoid scope creep.
- **Presentation:** make-up slot on 16 or 17 Nov, 10–11 AM. Format is fixed: 5 slides in 5 minutes (Introduction, Literature Review, Methodology, Working Model, Conclusion). Slide 4 is a **live Colab demo** (decided 13 Nov), not a video. See `DEMO_GUIDE.md`.

## Pipeline (in the notebook `BDA_Customer_Segmentation.ipynb`)

1. Ingest the CSV into a Spark DataFrame.
2. Clean the data: parse `event_time`, drop null `user_id`/`user_session`, drop `price <= 0`, and drop duplicate events.
3. Run SparkSQL EDA on a temp view named `events` (event funnel, monthly revenue, top brands).
4. Compute RFM per buyer from purchase events.
5. `VectorAssembler` → `StandardScaler`.
6. KMeans for k=2..8, comparing the elbow curve and the Silhouette score from `ClusteringEvaluator`.
7. Train the final KMeans (seed=42) and name the clusters.

Use Spark (DataFrame API, SparkSQL, MLlib) for the processing and modelling steps, because the syllabus mapping depends on it. Use pandas or matplotlib only for plotting.

## Working preferences

- Write all documents as local `.md` files in this folder, not as artifacts or Claude Docs.
- Use the AskUserQuestion tool for anything unclear instead of guessing.
