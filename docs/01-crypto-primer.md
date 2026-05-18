# 01 — Crypto & Market Terminology Primer

Written assuming zero prior knowledge of crypto or trading. Read this first.

---

## What is a crypto exchange?

A crypto exchange is a marketplace where people buy and sell digital assets like Bitcoin (BTC) or Ethereum (ETH) in exchange for other assets — most commonly **USDT** (Tether, a "stablecoin" pegged 1:1 to the US dollar) or actual fiat money.

**Binance** is the world's largest crypto exchange by trade volume. They expose a free public **WebSocket API** that streams every trade as it happens. We connect to that.

---

## Symbols and trading pairs

A trading pair like **BTCUSDT** means "Bitcoin priced in USDT." If BTCUSDT is `78,000`, it means 1 BTC is worth 78,000 USDT (≈ $78,000 USD).

In our project the symbol is always lowercased in topic/URL paths (`btcusdt@trade`) and uppercased in the database (`BTCUSDT`).

---

## Orders vs trades

These two are often confused — they're related but different.

### Order
A **request** to buy or sell at a specific price. Orders sit in the exchange's **order book** waiting to be matched. There are two main types:

- **Limit order**: "Buy 0.5 BTC, but only if price drops to 77,900 or lower." Sits in the book.
- **Market order**: "Buy 0.5 BTC right now at whatever the best available price is." Doesn't sit — it executes immediately against existing limit orders.

### Trade
The **execution event** when two orders match. A trade always has a buyer and a seller. Each trade has:
- Price
- Quantity
- Timestamp
- An aggressor side (see below)

Our pipeline only ingests **trades** (not orders, not the order book). That's enough to detect anomalies and keeps data volume manageable.

---

## Maker vs taker — the most important concept here

When a trade happens, one side was already in the order book (the **maker**, who provided liquidity) and the other side hit them with a market order (the **taker**, who consumed liquidity).

In Binance's trade message, there's a field `m` (boolean) that means **"was the buyer the maker?"**

| `m` value | Buyer was | Seller was | Aggressor | What happened |
|---|---|---|---|---|
| `false` | Taker | Maker | Buyer | Someone hit a sell order → **market buy** |
| `true`  | Maker | Taker | Seller | Someone hit a buy order → **market sell** |

**Why this matters:** the aggressor is the one who *wanted to trade right now*. If lots of market buys are hitting, that's buying pressure. If lots of market sells are hitting, that's selling pressure. The ratio of aggressor buys vs aggressor sells is a key anomaly signal — it tells us about intent, not just volume.

We split volume into `buy_volume` and `sell_volume` based on `m`:

```python
if m == True:    sell_volume += quantity   # aggressor was seller
else:            buy_volume  += quantity   # aggressor was buyer
```

---

## OHLCV bars

Raw trade-by-trade data is overwhelming (500-1000 trades/sec at peak). To make it analyzable, we **bucket** trades into fixed time windows — in our system, **1-second windows**. Each window becomes a "bar" (or "candle") with these stats:

| Field | Meaning |
|---|---|
| **Open** | Price of the first trade in the window |
| **High** | Highest trade price in the window |
| **Low** | Lowest trade price in the window |
| **Close** | Price of the last trade in the window |
| **Volume** | Total BTC traded in the window |

The "candlestick" charts traders look at are just OHLC bars rendered visually.

We also store a few extras that aren't strictly OHLCV but are crucial for anomaly detection:

| Extra field | Meaning |
|---|---|
| `quote_volume` | Volume in USDT (= sum of price × qty) |
| `buy_volume` | Volume from aggressor buys (see above) |
| `sell_volume` | Volume from aggressor sells |
| `vwap` | Volume-Weighted Average Price (see below) |
| `trade_count` | How many individual trades happened in the bar |

---

## VWAP — Volume-Weighted Average Price

The "fair" average price during a bar, weighted by trade size:

$$
\text{VWAP} = \frac{\sum_i (p_i \cdot q_i)}{\sum_i q_i} = \frac{\text{quote\_volume}}{\text{volume}}
$$

Why use VWAP instead of just the close price?
- One tiny trade at an unusual price can move "close" but not VWAP
- VWAP represents where the *actual money* changed hands
- It's a more robust reference price for detecting outliers

---

## Pumps and dumps

These are the headline anomalies we're looking for.

### Pump
A rapid, often artificial **upward** move in price, usually driven by coordinated buying (or hype, news, whales). Characteristics:
- Sharp positive price move over seconds-to-minutes
- Volume spike well above recent average
- One-sided aggression (lots of market buys, few sells)

### Dump
The opposite — a rapid **downward** move, often:
- The end of a pump (people taking profit)
- Forced liquidations (leveraged positions hitting stop-losses)
- Bad news / panic selling

### "Market manipulation"
Some pumps/dumps are organic (e.g., reaction to a Fed announcement). Others are deliberate:
- A "pump and dump" group artificially pumps a low-liquidity asset, then dumps on retail buyers who FOMO in
- Wash trading: fake volume to lure traders
- Spoofing: placing big orders to scare the market, then cancelling

Our detector doesn't distinguish "manipulation" from "organic large move" — it flags unusual market behavior. Humans label intent.

### How our detector recognizes them

A real pump typically lights up all three rule-based detectors at the same bucket:
1. **price_zscore** fires because the return is many std devs from the rolling mean
2. **volume_spike** fires because the volume is many × the rolling median
3. **aggressor_imbalance** fires because the buy-volume ratio is near 1.0

When all three fire on the same 1-second bar AND the direction is `pump`, that's a high-confidence pump signal. Same logic with sells / dumps.

---

## A note on liquidity

"Liquidity" means how easily you can trade an asset without moving its price. BTCUSDT on Binance has **enormous** liquidity — you can trade millions of dollars with minimal price impact. That's why obvious manipulation is rarer here than on small altcoins.

So most of what our detector flags on BTCUSDT will be:
- Large legitimate trades from institutions
- Reactions to news/macro events
- Brief moments of one-sided pressure during fast moves

That's still useful — it surfaces what's "interesting" in real time. Catching textbook pump-and-dumps would require monitoring low-liquidity altcoins, which is a future extension.

---

## Glossary cheat sheet

| Term | Quick definition |
|---|---|
| BTC / ETH | Bitcoin / Ethereum (cryptocurrencies) |
| USDT | Tether — stablecoin pegged to US dollar |
| BTCUSDT | "BTC priced in USDT" — a trading pair |
| Order book | Live list of unfilled buy/sell limit orders |
| Limit order | Order at a specific price, waits in the book |
| Market order | Order at the best available price, fills immediately |
| Trade | Execution event matching a buyer and seller |
| Maker | The side that was already in the book |
| Taker / aggressor | The side that hit the book with a market order |
| Spread | Distance between best bid and best ask |
| OHLCV | Open, High, Low, Close, Volume — standard bar fields |
| Candle / candlestick | Visual rendering of an OHLC bar |
| VWAP | Volume-Weighted Average Price within a window |
| Pump | Rapid upward price move (often with buy aggression) |
| Dump | Rapid downward price move (often with sell aggression) |
| Liquidity | How much you can trade without moving the price |
| Whale | A market participant with very large capital |
| Z-score | How many std devs a value is from the mean |
