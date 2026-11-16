# Crypto Arbitrage Scanner

A multi-exchange cryptocurrency arbitrage scanner designed to collect, process, and analyze real-time market data across various exchanges for arbitrage opportunities.

## 📌 Overview

This system connects to multiple centralized cryptocurrency exchanges, retrieves key trading data (such as order books, tickers, and market metadata), and standardizes it for further analysis. Its modular design allows for the addition of new exchanges and data types.

### Supported Exchanges

Currently supports data fetching from the following exchanges:

- Kraken
- OKX
- Gate.io
- Bybit
- Bitmex
- Bitget
- XT
- MEXC
- Bitfinex
- Bitmart
- Coinbase
- Coinex
- Hashkey
- Hyperliquid
- HTX
- KuCoin
- Whitebit
- Exmo
- Poloniex
- Phemex
- Hollaex
- Bitopro
- Bitrue
- Bequant
- BigONE
- LBank
- Bitbns

## ⚙️ Features

- **Multi-Exchange Support**: Easily extendable architecture to add more exchanges.
- **Data Types**:
  - Market metadata (symbols, quote/base pairs)
  - Real-time tickers (bid/ask prices, volume)
  - Order books (depth up to top levels)
  - Currency and network data (fees, activity status, contract addresses)
- **Token Matching by Contract Address**: The same ticker on two exchanges is treated as one asset only when the contract addresses match, so different tokens sharing a symbol do not produce false opportunities. Network names are normalized through an alias table.
- **Order Book Verification**: Opportunities are confirmed against live order book depth, not just top-of-book prices.
- **Spot Market Links**: Every confirmed deal is stored with links to the spot market pages on both exchanges.
- **Blacklists**: Exchanges and currencies can be excluded from buying or selling via configuration.
- **Exchange Time Sync**: Local clock offset is synchronized with exchanges that support it.
- **Asynchronous Processing**: Each exchange runs in a separate task with its own symbol queue for high throughput.
- **Event-Driven**: Uses delegate events for new order book data to allow reactive integration with analysis or alert systems.
- **Error Logging**: Integrated error handling and logging via NLog.
- **Regression Tests**: Core queue and SQL logic is covered by a test project.

## 🧩 Architecture
![image](https://github.com/user-attachments/assets/d2083358-68cf-4f7f-ae3c-1664e2688e4e)

1. **Exchange Service**  
   A unified interface for retrieving market data (ask, bid, fees, supported networks).
2. **Collector Service**  
   Collects raw data from exchanges and performs initial filtering of arbitrage opportunities. 
3. **DB Service**  
   Stores and provides fast SQL access to historical and real-time data.  
4. **Verifier Service**  
   Confirms and monitors identified arbitrage opportunities, filters out price spikes and anomalies.

## 🔁 Scanner Workflow
1. Retrieve real-time data using [CCXT](https://github.com/ccxt/ccxt) and store it in ClickHouse, including:
   - **ask/bid** prices and 24h volume,
   - spot/futures/swap market metadata,
   - supported transfer networks with withdrawal fees and contract addresses.
2. For each currency pair, the system performs pairwise comparison across exchanges:
   - compares `ask` price on one exchange with `bid` on another;
   - only uses spot markets with sufficient volume and fresh quotes;
   - matches base and quote assets by contract address on a shared network;
   - skips blacklisted exchanges and currencies.
3. If `ask_A < bid_B` and the spread `(bid_B – ask_A) / ask_A × 100` is within the configured threshold range, the situation is considered a potential arbitrage.
4. A potential deal event is generated and passed to the **Verifier Service**.
5. **Verifier Service** monitors the opportunity:
   - walks the order book depth level by level (asks on the buy exchange vs. bids on the sell exchange) to calculate executable volume, average prices and real profit;
   - subtracts the withdrawal fee of the cheapest active network;
   - if the opportunity worsens, it is cancelled;
   - if confirmed, the deal is saved to ClickHouse with links to both spot markets and logged.

## ⚡ Quick Start

### 1. Installing ClickHouse

#### Option A – Install locally (Debian/Ubuntu):

```bash
sudo apt install -y clickhouse-server clickhouse-client
sudo service clickhouse-server start
```
#### Option B – Docker:

```bash
docker run -d \
  --name clickhouse-server \
  -p 8123:8123 \
  -p 9000:9000 \
  -e CLICKHOUSE_PASSWORD=xxx \
  clickhouse/clickhouse-server:latest
```

### 2. Configuration

Specify the connection parameters and optional blacklists in `appsettings.json`:

```json
{
  "ConnectionStrings": {
    "Clickhouse": "Host=0.0.0.0;Protocol=http;Port=8123;Username=default;Password=xxx"
  },
  "BlackLists": {
    "BuyExchanges": [ "binance" ],
    "SellExchanges": [ "binance" ],
    "BaseCurrency": [ "BROCCOLI" ],
    "QuotedCurrency": [ ]
  }
}
```

Some exchanges (OKX, Bybit, KuCoin) return currency and network data only with an API key. Read-only keys are enough. Pass them through environment variables so they never end up in the repository:

```bash
export ExchangeCredentials__okx__ApiKey=...
export ExchangeCredentials__okx__Secret=...
export ExchangeCredentials__okx__Password=...   # API passphrase (OKX, KuCoin)
```

### 3. Running the Project

#### Option A – Locally without Docker:

```bash
git clone https://github.com/Alex-ok2005/crypto-arbitrage-scanner.git
cd crypto-arbitrage-scanner

dotnet build crypto-arbitrage-scanner.sln --configuration Release

dotnet run --project ArbitrageScanner.csproj --configuration Release
```

#### Option B – Using Docker Compose:

Create a `docker-compose.yml` file:

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      - CLICKHOUSE_PASSWORD=xxx
    volumes:
      - clickhouse-data:/var/lib/clickhouse

  scanner:
    build: .
    depends_on:
      - clickhouse
    environment:
      - ConnectionStrings__Clickhouse=Host=clickhouse;Protocol=http;Port=8123;Username=default;Password=xxx

volumes:
  clickhouse-data:
```

Start the project:

```bash
docker compose up --build
```

---

## ⚙️ Installation

1. Install [.NET SDK 9.0+](https://dotnet.microsoft.com/download).
2. Clone the repository:
   
```bash
git clone https://github.com/Alex-ok2005/crypto-arbitrage-scanner.git
cd crypto-arbitrage-scanner
```

3. Build the project:
   
```bash
dotnet build crypto-arbitrage-scanner.sln
```

## 🧪 Tests

Regression tests live in `tests/ArbitrageScanner.Tests` and run as a console app:

```bash
dotnet run --project tests/ArbitrageScanner.Tests
```

## 🤝 Contributing

Want to contribute? You're welcome!

1. Fork the repository.
2. Create a new branch: `feature/your-feature`.
3. Make changes and add tests.
4. Open a Pull Request.
