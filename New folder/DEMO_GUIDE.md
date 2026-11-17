# Live Demo Guide (slide 4, about 90 seconds)

This runbook covers what to prepare, what to click and what to say during the live Colab demo, plus what to do if something breaks.

**Main idea:** the full pipeline takes **10+ minutes on Colab**, but the demo slot is about 90 seconds. So you **run the whole notebook before the presentation** and keep the tab open. In the demo you:
1. Scroll through the outputs that already exist, which shows the big-data work.
2. Run the **3 live cells** (section 9). Each finishes in a few seconds because the data is cached in Spark memory.

That way you show a real, working Spark session without waiting on a progress bar.

---

## A. The day before (about 30 min)

1. **Upload the latest notebook** `BDA_Customer_Segmentation.ipynb` from this folder to Colab. It contains the new section **"9. Live demo cells"**, and `spark.stop()` at the end is commented out.
   - If you open an older copy, it won't have the live cells. Use **File → Upload notebook** again.
2. Check that the 🔑 secret `KAGGLE_API_TOKEN` exists with **Notebook access** on.
3. Do a full **Runtime → Run all** once and check the numbers against section E below. The run takes about 10–15 minutes.
4. **Practise the demo twice** using section C, with a timer.
5. Test on the presentation laptop:
   - Is Colab logged in?
   - Is the browser zoom at 125–150% so the back row can read the text?
   - Does it work on the projector resolution?
6. Make sure the **backup** opens offline: `BDA_Customer_Segmentation_executed.ipynb` (VS Code or Jupyter), or just the slides.

## B. On the day, 30–40 minutes before your slot

1. Open Colab → your notebook → **Runtime → Run all**.
   - The runtime starts fresh, so it downloads the dataset again (1–2 minutes) and runs everything (about 10 minutes).
   - Start it **at least 30 minutes early**.
2. When it's done, check:
   - The last cells (section 9) printed tables with no errors.
   - The Colab RAM/Disk indicator at the top right is green.
3. **Keep the tab active.**
   - Free Colab disconnects after about 90 minutes idle, and sooner if the tab is closed.
   - Click a cell every 15–20 minutes, or run `customers.count()` in a scratch cell.
4. Prepare the browser:
   - Colab in **one tab**, the slides in another window, so you can **Alt+Tab** between them.
   - Collapse the Colab sidebar (the ☰ icon) and close the file and secret panels.
   - **Hide the 🔑 Secrets panel.** Never show the token on screen.
   - Use **Ctrl + +** to zoom to about 125%.
5. **Pre-scroll** so that the first thing visible is the file-size output in section 1.
6. **Don't clear the outputs** (Edit → Clear all outputs). They are what you show.

## C. The demo, click by click (about 90 s)

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

## D. What the live cells print (from the local verification run)

The values come from the local run and should match Colab. Small differences in the last decimal are fine.

**LIVE 1: SparkSQL on segments.** Say: "At-Risk is the biggest revenue pool, $2.76M, from buyers who haven't returned in about 3 months."
```
+---------------+------+----------------+----------+---------+---------+
|        Segment|buyers|avg_recency_days|avg_orders|avg_spend|  revenue|
+---------------+------+----------------+----------+---------+---------+
|        At-Risk| 39845|            88.0|      1.22|     69.3|2760310.0|
|      Champions| 10835|            35.0|       4.0|    203.3|2202442.0|
|New / Promising| 18715|            14.0|      1.16|     39.1| 731990.0|
|           Lost| 41123|            89.0|      1.02|     15.9| 653526.0|
+---------------+------+----------------+----------+---------+---------+
```

**LIVE 2: one real buyer (user 420629649).** Say: "9 orders, last one 7 days ago, $602 spent, so a Champion. Spark found their 1,523 events among 19.5 million."
```
+---------+-------+---------+--------+-------+---------+
|  user_id|Recency|Frequency|Monetary|Cluster|  Segment|
+---------+-------+---------+--------+-------+---------+
|420629649|      7|        9|   601.7|      1|Champions|
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

## E. Numbers to check after the pre-run

| Output | Expected |
|---|---|
| Total size | 2.27 GB in 5 files |
| Raw events | 20,692,840 |
| Rows after cleaning | 19,493,208 (5.8% removed) |
| Users / sessions | 1,637,770 / 4,520,284 |
| Purchasing users | 110,518 |
| Buyers with RFM | 110,518 |
| Silhouette at k = 4 | ≈ 0.49 |
| Segments | Champions 10,835 · New/Promising 18,715 · At-Risk 39,845 · Lost 41,123 |

Colab runs Spark 4.0 and the local check ran Spark 3.5. The event and buyer counts must match exactly. If the **cluster sizes** differ, the K-Means random start behaved differently on Spark 4.0. Download `outputs/results.json` from Colab into `outputs/` here and rebuild the deck with `python tools/build_deck.py`, so the slides match what you show.

## F. If something goes wrong

| Problem | What to do on stage |
|---|---|
| Colab disconnected ("Reconnect" button) | **Don't re-run.** Say "the session timed out, here is the same run saved", and open `BDA_Customer_Segmentation_executed.ipynb` or the Colab copy that already shows the outputs. Then continue with the slides. |
| A live cell errors with `NameError` | The runtime was restarted and the variables are gone. Use the fallback above. |
| A live cell is slow (> 15 s) | Keep talking about what it does. If it's still running after 30 s, stop it (■) and move on to slide 5. |
| No internet or the projector won't show the browser | Stay on slide 4. Its picture shows the final segments. Describe the steps from the right-hand list. |
| An examiner asks to run a different query | In a new cell (**+ Code**) type, for example: `spark.sql("SELECT brand, COUNT(*) FROM events WHERE event_type='purchase' GROUP BY brand ORDER BY 2 DESC LIMIT 5").show()`. It takes a few seconds because `events` is cached. |
| An examiner asks to see the Spark plan | `customers.explain()` or `rfm.explain()` |
| An examiner asks how many partitions | `events.rdd.getNumPartitions()` |

## G. Extra live queries (in case you're asked)

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
