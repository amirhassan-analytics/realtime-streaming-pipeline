# Power BI dashboard — mart_minute_bars

Connect Power BI Desktop to the PostgreSQL database
(`localhost:5432`, database `stream`, user `stream`) and build:

1. **Price by symbol (line chart)**
   - Axis: `window_start`
   - Values: `close_price`
   - Legend: `symbol`
2. **Volume by symbol (bar chart)**
   - Axis: `symbol`
   - Values: `total_volume` (sum)
3. **Latest prices (card / table)**
   - Filter `window_start` to the last 60 minutes
   - Columns: `symbol`, `close_price`, `high_price`, `low_price`

Refresh: set the dataset to refresh every 1 minute while the pipeline
is running, or use DirectQuery against `mart_minute_bars`.

Suggested DAX measure for the price change in the window:
```
PriceChangePct = DIVIDE(MAX(mart_minute_bars[close_price]) - MIN(mart_minute_bars[open_price]), MIN(mart_minute_bars[open_price]))
```
