"""Builds D:/BDA-PROJECT/BDA_Customer_Segmentation_Presentation.pptx from outputs/results.json + charts.
Usage: python build_deck.py [path/to/demo.mp4]"""
import json, os, sys
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR

ROOT = "D:/BDA-PROJECT"
OUT = f"{ROOT}/outputs"
R = json.load(open(f"{OUT}/results.json"))
VIDEO = sys.argv[1] if len(sys.argv) > 1 else None

INK = RGBColor(0x0B, 0x0B, 0x0B); INK2 = RGBColor(0x52, 0x51, 0x4E)
BLUE = RGBColor(0x2A, 0x78, 0xD6); PAPER = RGBColor(0xFC, 0xFC, 0xFB); LINE = RGBColor(0xE2, 0xE1, 0xDC)
WHITE = RGBColor(255, 255, 255)
SEG_RGB = [RGBColor(0x2A, 0x78, 0xD6), RGBColor(0xEB, 0x68, 0x34), RGBColor(0x1B, 0xAF, 0x7A), RGBColor(0xED, 0xA1, 0x00)]

prs = Presentation()
prs.slide_width, prs.slide_height = Inches(13.333), Inches(7.5)
BLANK = prs.slide_layouts[6]
fmt = lambda n: f"{n:,}"
M = lambda n: f"{n/1e6:.1f}M"


def text(slide, x, y, w, h, runs, size=16, color=INK, bold=False, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP):
    """runs: str or list of paragraphs; a paragraph is str or list of (text, {opts})."""
    tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame; tf.word_wrap = True; tf.vertical_anchor = anchor
    tf.margin_left = tf.margin_right = Inches(0.05)
    for i, p in enumerate(runs if isinstance(runs, list) else [runs]):
        para = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        para.alignment = align; para.space_after = Pt(6)
        for t, o in ([(p, {})] if isinstance(p, str) else p):
            r = para.add_run(); r.text = t
            r.font.size = Pt(o.get("size", size)); r.font.bold = o.get("bold", bold)
            r.font.color.rgb = o.get("color", color); r.font.name = "Segoe UI"
    return tb


def box(slide, x, y, w, h, fill=None, line=None, shape=MSO_SHAPE.RECTANGLE):
    s = slide.shapes.add_shape(shape, Inches(x), Inches(y), Inches(w), Inches(h))
    if fill is None: s.fill.background()
    else: s.fill.solid(); s.fill.fore_color.rgb = fill
    if line is None: s.line.fill.background()
    else: s.line.color.rgb = line; s.line.width = Pt(1)
    s.shadow.inherit = False
    return s


def frame(num, title, kicker):
    s = prs.slides.add_slide(BLANK)
    s.background.fill.solid(); s.background.fill.fore_color.rgb = PAPER
    box(s, 0, 0, 13.333, 0.12, fill=BLUE)
    text(s, 0.6, 0.35, 10, 0.4, kicker.upper(), size=12, color=BLUE, bold=True)
    text(s, 0.6, 0.7, 12.2, 0.8, title, size=30, bold=True)
    text(s, 0.6, 7.0, 10, 0.3, "Customer Segmentation of E-Commerce Big Data using PySpark K-Means (RFM) · Big Data Analytics · Adani University",
         size=10, color=INK2)
    text(s, 12.2, 7.0, 0.6, 0.3, f"{num} / 5", size=10, color=INK2, align=PP_ALIGN.RIGHT)
    return s


def notes(s, t): s.notes_slide.notes_text_frame.text = t


def stat(s, x, y, value, label, w=2.2):
    text(s, x, y, w, 0.6, value, size=26, bold=True, color=BLUE)
    text(s, x, y + 0.6, w, 0.5, label, size=12, color=INK2)


segs = {d["Segment"]: d for d in R["segments"]}
champ, risk, lost, new = segs["Champions"], segs["At-Risk"], segs["Lost"], segs["New / Promising"]
ev, us = R["event_counts"], R["user_counts"]

# ---------------- Slide 1: Introduction ----------------
s = frame(1, f"Segmenting {R['customers']/1e3:.0f}k shoppers from {R['raw_rows']/1e6:.0f}M events with Spark", "Introduction")
text(s, 0.6, 1.5, 7.4, 0.4, "Rohan Upadhyay · B.Tech Sem VII · Big Data Analytics mini project (Reference topic #34)", size=13, color=INK2)
text(s, 0.6, 2.2, 7.1, 4.6, [
    [("Problem  ", {"bold": True, "color": BLUE}),
     ("An online cosmetics shop logs millions of clicks, but markets to every buyer the same way. "
      "Its best buyers get no reward, and lapsing buyers get no win-back offer.", {})],
    [("Dataset  ", {"bold": True, "color": BLUE}),
     (f"REES46 eCommerce events (Kaggle): {R['n_files']} monthly CSVs, {R['data_size_gb']} GB, "
      f"{fmt(R['raw_rows'])} events (view, cart, remove, purchase), Oct 2019 – Feb 2020.", {})],
    [("Objective  ", {"bold": True, "color": BLUE}),
     ("Use Apache Spark to compute Recency, Frequency and Monetary (RFM) value for every buyer, "
      "then group buyers with MLlib K-Means into segments that marketing can act on.", {})],
], size=16)
box(s, 8.1, 2.2, 4.6, 4.4, fill=WHITE, line=LINE)
stat(s, 8.4, 2.4, f"{R['data_size_gb']} GB", f"{R['n_files']} CSV files")
stat(s, 10.6, 2.4, M(R["raw_rows"]), "raw events", w=2.0)
stat(s, 8.4, 3.75, M(R["users"]), "unique users")
stat(s, 10.6, 3.75, fmt(R["customers"]), "buyers", w=2.0)
stat(s, 8.4, 5.1, "PySpark", "Colab · DataFrames · SparkSQL · MLlib", w=4.2)
notes(s, f"""Good morning. My project is customer segmentation of e-commerce big data using PySpark K-Means, reference topic 34.
The problem: an online cosmetics shop records millions of clicks, but it markets to every buyer in the same way.
The dataset is the REES46 e-commerce events log from Kaggle: five monthly CSV files, {R['data_size_gb']} gigabytes, {R['raw_rows']/1e6:.1f} million events, from October 2019 to February 2020. Every row is a view, add-to-cart, remove or purchase.
My objective is to use Spark to compute Recency, Frequency and Monetary values for every buyer, and then use MLlib K-Means to group them into segments marketing can act on.""")

# ---------------- Slide 2: Literature Review ----------------
s = frame(2, "What prior work tells us", "Literature review")
lit = [
    ("Hughes (1994)", "RFM model", "Introduced Recency–Frequency–Monetary scoring for database marketing: recent, frequent, high-spend customers are the most likely to respond."),
    ("MacQueen (1967); Wu et al. (2008)", "K-Means clustering", "K-Means partitions data into k groups by minimising within-cluster squared distance. It is one of the top-10 data-mining algorithms and is the usual choice for RFM segmentation."),
    ("Chen, Sain & Guo (2012)", "RFM + K-Means in e-retail", "Segmented an online retailer's customers with RFM and K-Means (the UCI Online Retail study). We apply the same idea to a 40× larger clickstream log."),
    ("Zaharia et al. (2016); Meng et al. (2016)", "Apache Spark & MLlib", "Spark processes data in memory across partitions and machines, and MLlib provides distributed K-Means (k-means|| initialisation), so the method scales to gigabytes of events."),
]
for i, (who, what, line) in enumerate(lit):
    y = 1.75 + i * 1.28
    box(s, 0.6, y, 0.12, 1.0, fill=BLUE)
    text(s, 0.95, y - 0.02, 3.6, 1.0, [[(what, {"bold": True, "size": 17})], [(who, {"color": INK2, "size": 12})]])
    text(s, 4.7, y, 8.0, 1.1, line, size=15)
notes(s, """Four pieces of prior work shape this project.
First, Hughes in 1994 introduced the RFM model: customers who bought recently, buy often and spend more are the most valuable.
Second, K-Means, from MacQueen 1967, is one of the top ten data-mining algorithms and the usual way to cluster RFM values.
Third, Chen, Sain and Guo in 2012 used RFM with K-Means to segment an online retailer's customers. I apply the same idea to a clickstream log that is about forty times larger.
Fourth, Spark and its MLlib library run the whole pipeline in memory and in parallel, which is what lets this method scale to gigabytes of data.""")

# ---------------- Slide 3: Methodology ----------------
s = frame(3, "Methodology: a Spark pipeline from raw events to segments", "Methodology")
steps = [("Ingest", f"spark.read.csv\n{R['n_files']} files · {R['data_size_gb']} GB\n{R['raw_partitions']} partitions"),
         ("Clean", "parse time, drop\nnulls, price ≤ 0,\nduplicates"),
         ("EDA", "SparkSQL on\nview 'events'"), ("RFM", "purchases →\ngroupBy(user_id)\n.agg()"),
         ("Scale", "log1p →\nVectorAssembler\n→ StandardScaler"), ("K-Means", "k = 2..8\nelbow +\nSilhouette"),
         ("Profile", "name\nsegments")]
w, gap, x0 = 1.62, 0.13, 0.6
for i, (h, sub) in enumerate(steps):
    x = x0 + i * (w + gap)
    shp = box(s, x, 1.7, w, 0.62, fill=BLUE, shape=MSO_SHAPE.CHEVRON if i else MSO_SHAPE.PENTAGON)
    tf = shp.text_frame; tf.text = h
    p = tf.paragraphs[0]; p.alignment = PP_ALIGN.CENTER
    p.runs[0].font.size = Pt(14); p.runs[0].font.bold = True; p.runs[0].font.color.rgb = WHITE
    text(s, x, 2.4, w, 1.1, sub, size=11, color=INK2, align=PP_ALIGN.CENTER)
text(s, 0.6, 3.7, 5.7, 3.3, [
    [("Cleaning: ", {"bold": True}), (f"{fmt(R['raw_rows'])} → {fmt(R['clean_rows'])} events "
      f"({R['removed_pct']}% removed: nulls, price ≤ 0, duplicate events). Cleaned data is cached in memory.", {})],
    [("RFM: ", {"bold": True}), (f"from {fmt(R['purchase_events'])} purchase events → {fmt(R['customers'])} buyers. "
      "Recency = days since last order, Frequency = orders (sessions), Monetary = total $.", {})],
    [("Scaling: ", {"bold": True}), ("log1p removes heavy skew; StandardScaler stops Monetary dominating the Euclidean distance.", {})],
    [("Choosing k: ", {"bold": True}), (f"k=2 is too coarse; WSSSE bends at k=4; k≤4 gives identical clusters on "
      f"Spark 3.5 and 4.0, k≥5 doesn't (unstable) → KMeans(k=4, seed=42), Silhouette {R['final_silhouette']:.2f}.", {})],
], size=13)
s.shapes.add_picture(f"{OUT}/k_selection.png", Inches(6.45), Inches(3.75), width=Inches(6.35))
notes(s, f"""The whole pipeline runs in Spark.
Spark reads all five CSV files into one DataFrame of {R['raw_partitions']} partitions, which are processed in parallel by all available cores.
Cleaning parses the timestamp and drops rows with missing users, zero prices and duplicate events. That leaves {R['clean_rows']/1e6:.1f} million events. Because Spark is lazy, these steps only build a plan, which runs when we call an action like count.
I explore the data with SparkSQL, then keep only purchase events and use groupBy on user ID to compute Recency, Frequency and Monetary for {fmt(R['customers'])} buyers.
The values are very skewed, so I take a log, then use VectorAssembler and StandardScaler.
Finally I train K-Means for k from 2 to 8. The Silhouette score is highest at 2, but that is too coarse. The error curve bends at 4. k = 4 is also stable: up to 4 the clusters are identical on two Spark versions, while 5 and above change between runs. So the final model is k equals 4 with seed 42.""")

# ---------------- Slide 4: Working model ----------------
s = frame(4, "Working model: live demo on Google Colab", "Working model (live demo)")
if VIDEO and os.path.exists(VIDEO):
    s.shapes.add_movie(VIDEO, Inches(0.6), Inches(1.6), Inches(8.4), Inches(5.25),
                       poster_frame_image=f"{OUT}/segments_scatter.png", mime_type="video/mp4")
else:
    box(s, 0.6, 1.6, 8.4, 5.25, fill=RGBColor(0xF0, 0xEF, 0xEC), line=LINE)
    text(s, 0.9, 1.75, 7.8, 0.5, [[("LIVE  ·  switching to the Colab notebook", {"bold": True, "size": 16, "color": INK2})]],
         align=PP_ALIGN.CENTER)
    s.shapes.add_picture(f"{OUT}/segments_scatter.png", Inches(1.35), Inches(2.3), height=Inches(4.4))
text(s, 9.3, 1.6, 3.5, 0.4, "What I show live", size=15, bold=True)
text(s, 9.3, 2.05, 3.5, 2.8, [
    f"1. Spark read 5 files · {R['data_size_gb']} GB",
    f"2. {M(R['raw_rows'])} → {M(R['clean_rows'])} events (lazy plan)",
    "3. SparkSQL funnel + monthly revenue",
    f"4. RFM for {R['customers']/1e3:.0f}k buyers via groupBy",
    "5. Elbow + Silhouette, k = 4",
    "6. LIVE: SQL query + score a new customer",
], size=12, color=INK2)
s.shapes.add_picture(f"{OUT}/eda_event_funnel.png", Inches(9.3), Inches(4.75), width=Inches(3.5))
notes(s, f"""[Switch to the Colab tab, about 90 seconds. The notebook was run before the session, so all outputs are already there.]
This is the notebook on Google Colab. Spark read all five files, {R['data_size_gb']} gigabytes, which is {R['raw_rows']/1e6:.1f} million events.
Here is the lazy plan from explain, and the counts before and after cleaning.
The SparkSQL funnel shows {us['view']/1e6:.1f} million users viewed products but only {fmt(us['purchase'])} bought, which is {R['view_to_purchase_user_pct']} percent.
Here is the RFM table from one groupBy, and the elbow and Silhouette charts for k from 2 to 8.
Now live: I run a SQL query on the segments, and then I put a new customer through the same pipeline and the model tells us their segment.
[If Colab fails, stay on this slide: the chart shows the four segments.]""")

# ---------------- Slide 5: Conclusion ----------------
s = frame(5, "Four segments, four different marketing actions", "Conclusion")
actions = {"Champions": "VIP tier, early access, referrals",
           "New / Promising": "Welcome series + 2nd-order coupon",
           "At-Risk": "Personalised win-back offers",
           "Lost": "Newsletter only, no paid-ads spend"}
hdr = ["Segment", "Buyers", "Avg R / F / M", "Revenue share", "Action"]
widths = [1.75, 1.45, 1.75, 1.05, 2.3]
tbl = s.shapes.add_table(5, 5, Inches(0.6), Inches(1.65), Inches(sum(widths)), Inches(3.3)).table
for j, wd in enumerate(widths):
    tbl.columns[j].width = Inches(wd)
for i in range(5):
    for j in range(5):
        c = tbl.cell(i, j); c.fill.solid()
        c.fill.fore_color.rgb = BLUE if i == 0 else (WHITE if i % 2 else RGBColor(0xF4, 0xF4, 0xF1))
        if i == 0: val = hdr[j]
        else:
            d = R["segments"][i - 1]
            val = [d["Segment"], f"{d['Customers']:,} ({d['CustomerShare%']}%)",
                   f"{d['AvgRecency']:.0f}d / {d['AvgFrequency']:.1f} / ${d['AvgMonetary']:,.0f}",
                   f"{d['RevenueShare%']}%", actions[d["Segment"]]][j]
        c.text = val
        r = c.text_frame.paragraphs[0].runs[0]
        r.font.size = Pt(11); r.font.name = "Segoe UI"; r.font.bold = (i == 0 or j == 0)
        r.font.color.rgb = WHITE if i == 0 else (SEG_RGB[i - 1] if j == 0 else INK)
s.shapes.add_picture(f"{OUT}/segment_sizes.png", Inches(9.1), Inches(1.65), width=Inches(3.9))
text(s, 9.1, 3.2, 3.9, 1.9, [[("Key finding  ", {"bold": True, "color": BLUE}),
     (f"Champions are only {champ['CustomerShare%']}% of buyers but bring {champ['RevenueShare%']}% of revenue. "
      f"At-Risk buyers hold {risk['RevenueShare%']}% of revenue: winning them back matters most.", {})]], size=13)
text(s, 0.6, 5.2, 6.0, 1.7, [[("Limitations", {"bold": True, "size": 15})],
     "5-month snapshot of one shop · k chosen partly by judgement · RFM ignores browsing behaviour and product mix · single-node Spark on Colab"], size=13, color=INK2)
text(s, 6.9, 5.2, 6.0, 1.7, [[("Future scope", {"bold": True, "size": 15})],
     "Spark Structured Streaming for live segments · add view/cart features · Bisecting K-Means / GMM in MLlib · multi-node cluster"], size=13, color=INK2)
notes(s, f"""To conclude: Spark K-Means found four clear segments among {fmt(R['customers'])} buyers.
Champions, {champ['CustomerShare%']} percent of buyers with about 4 orders each, bring {champ['RevenueShare%']} percent of revenue. They should get VIP rewards.
New buyers, {new['CustomerShare%']} percent, bought in the last two weeks. A welcome series and a second-order coupon can build the habit.
At-Risk buyers, {risk['CustomerShare%']} percent, spent well but haven't returned for about three months, and they hold {risk['RevenueShare%']} percent of revenue. They are the most important group to win back.
Lost buyers, {lost['CustomerShare%']} percent, made one small order. They should only get the cheap newsletter.
On ethics, the data is anonymised (only numeric user IDs), so no personal details were used.
The limitations are a five-month snapshot and a k chosen partly by judgement. Next, I would use Spark Structured Streaming to update segments live and add browsing features. Thank you.""")

dst = os.environ.get("DECK_OUT", f"{ROOT}/BDA_Customer_Segmentation_Presentation.pptx")
prs.save(dst)
print("saved", dst)
