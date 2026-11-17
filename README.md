# BDA Mini Project: Customer Segmentation of E-Commerce Big Data using PySpark K-Means (RFM)

Reference topic #34 · Big Data Analytics · Adani University · Rohan Upadhyay

## Dataset (meets the 1–4 GB requirement)

**eCommerce Events History in Cosmetics Shop** (REES46, Kaggle `mkechinov/ecommerce-events-history-in-cosmetics-shop`)
- 5 monthly CSVs (Oct 2019 → Feb 2020), **2.27 GB** in total, **20,692,840 events**
- Columns: `event_time, event_type (view/cart/remove_from_cart/purchase), product_id, category_id, category_code, brand, price, user_id, user_session`
- Stored locally in `data/cosmetics/`. The notebook downloads it automatically on Colab.

## What's in this folder

| File | What it is |
|---|---|
| `BDA_Customer_Segmentation.ipynb` | The main notebook. It runs top to bottom on Google Colab and downloads the dataset via the Kaggle API. |
| `BDA_Customer_Segmentation_executed.ipynb` | The same notebook with all outputs saved (from a local PySpark 3.5 run). Keep it as a backup to show if Colab is slow during the demo. |
| `BDA_Customer_Segmentation_Presentation.pptx` | The 5-slide deck, with the speaker script in each slide's notes. Slide 4 is the live-demo slide. |
| `BDA_Customer_Segmentation_run-by-colab.ipynb` | A copy downloaded from Colab. |
| `FINAL_PROJECT_EXPLANATION.md` | **Start here.** The whole project in one file: report, technical guide, slide-by-slide content, live-demo runbook and viva answers. |
| `PROJECT_REPORT.md` | Full project report: problem, literature, method, results, discussion, references. |
| `TECHNICAL_GUIDE.md` | Handover guide: Spark concepts, cell-by-cell walkthrough, how to rebuild and change things, known issues. |
| `DEMO_GUIDE.md` | Live-demo runbook: preparation, click-by-click script, expected outputs, fallbacks. |
| `outputs/` | Charts (PNG), `customer_segments.csv` (110,518 buyers with their segment) and `results.json` (every number on the slides). |
| `PRESENTATION_SCRIPT.md` | What to say on each slide, plus viva answers. |
| `tools/` | The scripts that generated the notebook and the deck. `python tools/build_deck.py demo.mp4` rebuilds the deck with the video embedded. |
| `data/` | `cosmetics/` holds the project dataset. `Online Retail.xlsx`, `online_retail.csv` and `online_retail.zip` are the old UCI data, which is no longer used and can be deleted. |

## Your remaining steps (about 45 minutes)

### 1. Run the notebook on Colab (15 min)
1. Go to https://colab.research.google.com → **File → Upload notebook** → `BDA_Customer_Segmentation.ipynb`.
2. Click the **🔑 (Secrets)** icon in the left bar → **Add new secret**. Set the name to `KAGGLE_API_TOKEN` and the value to your Kaggle API token (the `KGAT_…` string). Turn on **Notebook access**.
3. **Runtime → Run all.** The first run installs PySpark and downloads about 0.5 GB from Kaggle (the files unzip to 2.27 GB).
   - Free Colab has only 2 CPU cores, so it is slower than the local check. Expect roughly 5–15 minutes in total. That's fine and shows real Big Data work.
   - If Colab runs out of RAM (the session crashes), go to **Runtime → Restart session**, change `"spark.driver.memory", "6g"` to `"4g"` in the setup cell, and run all again.
4. Check that the numbers match the slides: **20,692,840** raw → **19,493,208** clean events → **110,518** buyers, k = 4 with Silhouette ≈ **0.49**. ✅ Done on 13 Nov: everything matched, and the deck now uses the Colab results.

### 2. Prepare the live demo (see `DEMO_GUIDE.md`)
- The working model is shown **live** instead of as a video.
- Run the whole notebook on Colab 30–40 minutes before your slot and keep the tab open.
- During slide 4, scroll through the outputs and run the three **LIVE** cells in section 9 (each takes a few seconds).
- Backup: `BDA_Customer_Segmentation_executed.ipynb` and the picture on slide 4.
- To use a video after all, record one and run `python tools/build_deck.py demo.mp4`.

### 3. Rehearse once (15 min)
- Read `PRESENTATION_SCRIPT.md` aloud with a timer, and stay **under 5 minutes**.

### 4. After the presentation
- Expire the Kaggle token (Kaggle → Settings → API Tokens → ⋮ → Expire), because it was pasted into a terminal command.

## Key results (Colab run, Spark 4.0.5, 13 Nov)

- 2.27 GB / 20.69M events → 19.49M after cleaning (5.8% removed: missing user/session, price ≤ 0, duplicate events)
- 1.64M users, 4.52M sessions, 53,903 products, 273 brands, $6.35M purchase revenue, peak month Nov 2019
- Funnel: 1.60M users viewed → 397k added to cart → **110,518 bought (6.9%)**
- K-Means k = 4 (seed 42), Silhouette 0.49 (k = 2 scores 0.66 but is too coarse; the WSSSE elbow is at k = 4, and k ≤ 4 is stable across Spark versions)
- All data counts are identical on local Spark 3.5. The K-Means segment sizes differ by under 50 buyers. The local outputs are kept in `outputs/local_spark35/`.

| Segment | Buyers | Avg Recency | Avg Orders | Avg Spend | Revenue share |
|---|---|---|---|---|---|
| Champions | 10,819 (9.8%) | 35 days | 4.0 | $203 | 34.7% |
| New / Promising | 18,732 (16.9%) | 14 days | 1.2 | $39 | 11.5% |
| At-Risk | 39,800 (36.0%) | 88 days | 1.2 | $69 | 43.5% |
| Lost | 41,167 (37.2%) | 89 days | 1.0 | $16 | 10.3% |
