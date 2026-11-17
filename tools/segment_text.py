K_TEXT = """The Silhouette score is highest at k = 2 (≈0.66), but two segments ("good" vs "bad" buyers) are too coarse for marketing. The WSSSE curve drops steeply up to **k = 4** (−56k from k=2→3, −36k from k=3→4) and then flattens (−21k from k=4→5, then under −10k per step): that bend is the **elbow**. k = 4 is also **stable**: k = 2, 3 and 4 give the same clusters on Spark 3.5 and Spark 4.0, while the k ≥ 5 solutions change between versions (their Silhouette moves between 0.45 and 0.52), so they depend on the random start. At k = 4 the Silhouette is ≈0.49 (reasonable structure), and the four clusters map to four distinct, actionable RFM personas, so we use **k = 4**."""

NAMING_TEXT = """Clusters are named with a simple, explainable rule on the cluster averages:
1. highest average **Frequency** → **Champions** (repeat buyers who spend the most)
2. of the rest, lowest average **Recency** → **New / Promising** (just bought for the first time)
3. of the last two (both inactive for ~3 months), the higher average **Monetary** → **At-Risk** (spent well but haven't come back), the other → **Lost** (one small order, long ago)"""

NAMING_CODE = '''prof = profile.toPandas()
names, rest = {}, prof.copy()
c = rest.loc[rest.AvgFrequency.idxmax()]; names[c.Cluster] = "Champions";       rest = rest.drop(c.name)
c = rest.loc[rest.AvgRecency.idxmin()];   names[c.Cluster] = "New / Promising"; rest = rest.drop(c.name)
c = rest.loc[rest.AvgMonetary.idxmax()];  names[c.Cluster] = "At-Risk";         rest = rest.drop(c.name)
names[rest.iloc[0].Cluster] = "Lost"
SEG_ORDER = ["Champions", "New / Promising", "At-Risk", "Lost"]'''

ACTIONS_TABLE = """| Segment | Profile | Suggested action |
|---|---|---|
| **Champions** | Repeat buyers (≈4 orders), highest spend, bought within ~1 month | VIP/loyalty tier, early access to new products, referral rewards |
| **New / Promising** | First purchase in the last ~2 weeks, small basket | Welcome series and a second-purchase coupon to build the habit |
| **At-Risk** | One or two good-sized orders, but no purchase for ~3 months | Personalised win-back emails with products from their favourite brands |
| **Lost** | One small order ~3 months ago | Low-cost reactivation (newsletter only); don't spend paid-ads budget here |"""
