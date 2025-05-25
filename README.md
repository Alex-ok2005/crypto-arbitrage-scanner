# Crypto Arbitrage Scanner

A multi-exchange cryptocurrency arbitrage scanner designed to collect, process, and analyze real-time market data across various exchanges for arbitrage opportunities.

## 📌 Overview

This system connects to multiple centralized cryptocurrency exchanges, retrieves key trading data (such as order books, tickers, and market metadata), and standardizes it for further analysis. Its modular design allows for the addition of new exchanges and data types.

### Supported Exchanges

Currently supports data fetching from the following exchanges:

- Kraken
- Gate.io
- Bitmex
- Bitget
- XT
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
- Timex
- Bitopro
- Bitrue

## ⚙️ Features

- **Multi-Exchange Support**: Easily extendable architecture to add more exchanges.
- **Data Types**:
  - Market metadata (symbols, quote/base pairs)
  - Real-time tickers (bid/ask prices, volume)
  - Order books (depth up to top levels)
  - Currency and network data (fees, activity status)
- **Asynchronous Processing**: Each exchange runs in a separate task with its own symbol queue for high throughput.
- **Event-Driven**: Uses delegate events for new order book data to allow reactive integration with analysis or alert systems.
- **Error Logging**: Integrated error handling and logging via `Logger`.

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
1. Retrieve real-time data using [CCXT](https://github.com/ccxt/ccxt), including:
   - **ask/bid** prices,
   - trading fees,
   - supported transfer networks.
2. For each currency pair, the system performs pairwise comparison:
   - compares `ask` price on one exchange with `bid` on another;
   - checks for a shared network;
   - considers both sending and receiving fees.
3. If the following condition is met: `(bid_B × (1 – fee_B)) – (ask_A × (1 + fee_A)) > порог` the situation is considered arbitrage.
4. A potential deal event is generated and passed to the **Verifier Service**.
5. **Verifier Service** monitors the opportunity:
   - if the opportunity worsens, it is cancelled;
   - if confirmed, the user is notified.

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
  clickhouse/clickhouse-server:latest
```

### 2. Connecting to the Database

Specify the connection parameters in `appsettings.json`:

```json
{
  "ConnectionStrings": {
    "Clickhouse": "Host=0.0.0.0;Protocol=http;Port=8123;Username=default;Password=xxx"
  }
}
```

### 3. Running the Project

#### Option A – Locally without Docker:

```bash
git clone https://github.com/yourusername/crypto-arbitrage-scanner.git
cd crypto-arbitrage-scanner

dotnet restore
dotnet build --configuration Release

dotnet run --project src/CollectorService
```

#### Option B – Using Docker Compose:

Create a `docker-compose.yml` file:

```yaml
version: "3.8"
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse-data:/var/lib/clickhouse

  collector:
    build: ./src/CollectorService
    depends_on:
      - clickhouse
    environment:
      - CLICKHOUSE__Host=clickhouse
      - CLICKHOUSE__HttpPort=8123
      - CLICKHOUSE__TcpPort=9000

volumes:
  clickhouse-data:
```

Start the project:

```bash
docker-compose up --build
```

---

## ⚙️ Installation

1. Install [.NET SDK 8.0+](https://dotnet.microsoft.com/download).
2. Clone the repository:
   
```bash    
git clone https://github.com/yourusername/crypto-arbitrage-scanner.git cd crypto-arbitrage-scanner
```

3. Build the project:
   
```bash
dotnet restore dotnet build
```

## 🤝 Contributing

Want to contribute? You're welcome!

1. Fork the repository.
2. Create a new branch: `feature/your-feature`.
3. Make changes and add tests.
4. Open a Pull Request.

Please make sure to follow the [code of conduct](CODE_OF_CONDUCT.md).
