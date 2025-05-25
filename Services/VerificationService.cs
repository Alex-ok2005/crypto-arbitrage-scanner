using ArbitrageScanner.Interfaces;
using ArbitrageScanner.Models;
using Microsoft.Extensions.Hosting;
using NLog;
using System.Data;

namespace ArbitrageScanner.Services
{
    public class VerificationService : BackgroundService
    {
        private readonly Logger _logger;
        private readonly IDBService _dBService;
        private readonly IExchangeService _exService;
        private readonly CollectorService _collectorService;

        private readonly double deposit = 100;      // Test deposit amount used to calculate profit, in quote currency
        private int _processedDealsCount = 1;       // Counter for verified deals
        private readonly object _locker = new();    // Synchronization object

        private static readonly Dictionary<PairStruct, ProfitTrade> ProfitTrades = [];

        public VerificationService(
            Logger logger,
            IDBService dBService,
            IExchangeService exService,
            CollectorService collectorService)
        {
            _logger = logger;
            _dBService = dBService;
            _exService = exService;
            _collectorService = collectorService;

            _exService.EventOrderBook += OrderBookHandler;
            _collectorService.EventProfitTrade += ProfitTradeHandler;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await ProcessNextTradeAsync();
                await Task.Delay(10, stoppingToken); // Add a small delay to reduce CPU usage
            }
        }
        private async Task ProcessNextTradeAsync()
        {
            ProfitTrade? tradeToProcess = null;

            // Select a trade for checking/rechecking
            lock (_locker)
            {
                tradeToProcess = ProfitTrades.Values
                    .Where(p => p.State == ProfitTradesState.New || p.State == ProfitTradesState.Checked)
                    .OrderBy(x => x.State)
                    .ThenBy(x => x.EventTime)
                    .ThenByDescending(x => x.PrcProfit)
                    .FirstOrDefault();
            }

            if (tradeToProcess != null)
            {
                await ProcessTrade(tradeToProcess);
            }
        }
        private async Task ProcessTrade(ProfitTrade trade)
        {
            var pairKey = new PairStruct(trade.Pair.Symbol, trade.Pair.BuyExchange, trade.Pair.SellExchange);
            var newState = trade.State == ProfitTradesState.New
                ? ProfitTradesState.Checking
                : ProfitTradesState.ReChecking;

            var updatedTrade = new ProfitTrade
            {
                Pair = trade.Pair,
                BuyPrice = trade.BuyPrice,
                SellPrice = trade.SellPrice,
                EventTime = trade.EventTime,
                PrcProfit = trade.PrcProfit,
                Networks = trade.Networks.ToArray(),
                HasFuture = trade.HasFuture,
                HasSwap = trade.HasSwap,
                PrcRealProfit = trade.PrcRealProfit,
                State = newState,
                BuyOrderBook = null,
                SellOrderBook = null
            };

            lock (_locker)
            {
                ProfitTrades[pairKey] = updatedTrade;
            }

            await EnqueueSymbolsForProcessing(trade);
        }
        private async Task EnqueueSymbolsForProcessing(ProfitTrade trade)
        {
            try
            {
                var buyExchange = _exService.Exchanges[trade.Pair.BuyExchange];
                var sellExchange = _exService.Exchanges[trade.Pair.SellExchange];

                if (!buyExchange.symbolQueue.Contains(trade.Pair.Symbol))
                    await Task.Run(() => buyExchange.symbolQueue.Enqueue(trade.Pair.Symbol));

                if (!sellExchange.symbolQueue.Contains(trade.Pair.Symbol))
                    await Task.Run(() => sellExchange.symbolQueue.Enqueue(trade.Pair.Symbol));
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Error enqueueing symbols for processing");
            }
        }
        private void ProfitTradeHandler(ProfitTrade[] trades, CancellationToken token)
        {
            // Remove expired trades
            lock (_locker)
            {
                var keysToRemove = ProfitTrades
                    .Where(kvp => kvp.Value.State == ProfitTradesState.Expired || kvp.Value.State == ProfitTradesState.Unconfirmed)
                    .Select(kvp => kvp.Key);

                _logger.Info($"Removing {keysToRemove.Count()} expired or unconfirmed trades from the queue.");

                foreach (var key in keysToRemove)
                {
                    ProfitTrades.Remove(key);
                }
            }

            foreach (var profitTrade in trades)
            {
                lock (_locker)
                {
                    var pairKey = new PairStruct(profitTrade.Pair.Symbol, profitTrade.Pair.BuyExchange, profitTrade.Pair.SellExchange);

                    if (ProfitTrades.TryGetValue(pairKey, out var existingTrade))
                    {
                        if ((existingTrade.State == ProfitTradesState.New && existingTrade.PrcProfit < profitTrade.PrcProfit)
                            || existingTrade.State == ProfitTradesState.Expired
                            || existingTrade.State == ProfitTradesState.Unconfirmed)
                        {
                            ProfitTrades[pairKey] = profitTrade;
                        }
                    }
                    else
                    {
                        ProfitTrades.Add(pairKey, profitTrade);
                    }
                }
            }
        }
        private void OrderBookHandler(string exchangeId, VerifyOrderBook orderBook, CancellationToken token)
        {
            try
            {
                IOrderedEnumerable<ProfitTrade> tradesToProcess;

                lock (_locker)
                {
                    tradesToProcess = ProfitTrades.Values
                        .Where(p => p.Pair.Symbol == orderBook.Symbol &&
                            (p.State is ProfitTradesState.Checking or ProfitTradesState.ReChecking) &&
                            (p.Pair.BuyExchange == exchangeId || p.Pair.SellExchange == exchangeId))
                        .OrderBy(p => p.EventTime);
                }

                foreach (var trade in tradesToProcess)
                {
                    ProcessOrderBookForTrade(trade, exchangeId, orderBook, token);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"Error in 'OrderBookHandler' method for exchange {exchangeId}");
            }
        }
        private void ProcessOrderBookForTrade(ProfitTrade trade, string exchangeId, VerifyOrderBook orderBook, CancellationToken token)
        {
            lock (_locker)
            {
                var updatedTrade = trade.Clone();

                if (trade.Pair.BuyExchange == exchangeId)
                    updatedTrade.BuyOrderBook = orderBook;
                else
                    updatedTrade.SellOrderBook = orderBook;

                if (updatedTrade.BuyOrderBook != null &&
                    updatedTrade.SellOrderBook != null &&
                    updatedTrade.BuyOrderBook.UpdatedTime >= trade.EventTime &&
                    updatedTrade.SellOrderBook.UpdatedTime >= trade.EventTime)
                {
                    UpdateTradeWithOrderBooks(updatedTrade, token);
                }

                var pairKey = new PairStruct(trade.Pair.Symbol, trade.Pair.BuyExchange, trade.Pair.SellExchange);
                ProfitTrades[pairKey] = updatedTrade;
            }
        }
        private void UpdateTradeWithOrderBooks(ProfitTrade trade, CancellationToken token)
        {
            trade.EventTime = trade.BuyOrderBook.UpdatedTime < trade.SellOrderBook.UpdatedTime
                ? trade.BuyOrderBook.UpdatedTime
                : trade.SellOrderBook.UpdatedTime;

            var isVerified = VerificateTrade(trade, deposit, out var deal);

            var oldState = trade.State;
            trade.State = isVerified
                ? ProfitTradesState.Checked
                : (trade.State == ProfitTradesState.ReChecking ? ProfitTradesState.Expired : ProfitTradesState.Unconfirmed);

            if (trade.State is ProfitTradesState.Checking or ProfitTradesState.ReChecking)
            {
                trade.VerifyTime = trade.EventTime;
            }

            if (isVerified)
            {
                SaveVerifiedDeal(deal, trade, oldState, token);
            }
            else if (trade.State == ProfitTradesState.Expired)
            {
                RemoveDeal(trade, token);
            }
        }
        private void SaveVerifiedDeal(SpotSpotDeal deal, ProfitTrade trade, ProfitTradesState oldState, CancellationToken token)
        {
            try
            {
                var data = CreateDealDataTable(deal);

                if (_dBService.RemoveItem(trade, "arbitrage.profit_trades").Result &&
                    _dBService.Export(data, "arbitrage.profit_trades", token).Result)
                {
                    _logger.Info($"{(oldState == ProfitTradesState.Checking
                    ? $"New deal N{_processedDealsCount} found" 
                        : "Deal re-verified")}: {deal.Symbol} | {deal.BuyExchange} => {deal.SellExchange} | network: {deal.NetworkName} | profit: {string.Format("{0:f2}", deal.ProfitPercentage)}%");
                    if (oldState == ProfitTradesState.Checking) _processedDealsCount++;
                }
                else
                {
                    _logger.Warn("Verified deal was not saved to the database.");
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Error while saving deal to the database");
            }
        }
        private void RemoveDeal(ProfitTrade trade, CancellationToken token)
        {
            try
            {
                if (_dBService.RemoveItem(trade, "arbitrage.profit_trades").Result)
                {
                    _logger.Info($"Deal expired. {trade.Pair.GetName()}");
                }
                else
                {
                    _logger.Warn("Failed to remove deal from the database.");
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Error while removing deal from the database");
            }
        }
        private static DataTable CreateDealDataTable(SpotSpotDeal deal)
        {
            var data = new DataTable();
            data.Columns.AddRange(
            [
                new("event_time", typeof(DateTime)),
                new("symbol", typeof(string)),
                new("network_name", typeof(string)),
                new("buy_exchange", typeof(string)),
                new("sell_exchange", typeof(string)),
                new("selected_buy_fee", typeof(double)),
                new("buy_order_count", typeof(uint)),
                new("sell_order_count", typeof(uint)),
                new("total_buy_cost", typeof(double)),
                new("total_sell_revenue", typeof(double)),
                new("avg_buy_price", typeof(double)),
                new("avg_sell_price", typeof(double)),
                new("min_buy_price", typeof(double)),
                new("max_sell_price", typeof(double)),
                new("min_buy_volume", typeof(double)),
                new("max_sell_volume", typeof(double)),
                new("max_profit_usdt", typeof(double)),
                new("profit_percentage", typeof(double)),
                new("min_profit_usdt", typeof(double)),
                new("min_profit_percentage", typeof(double)),
                new("deposit", typeof(double)),
                new("custom_total_buy_cost", typeof(double)),
                new("custom_profit_usdt", typeof(double)),
                new("custom_profit_percentage", typeof(double)),
                new("Is_deleted", typeof(byte))
            ]);

            data.Rows.Add(
                deal.EventTime,
                deal.Symbol,
                deal.NetworkName,
                deal.BuyExchange,
                deal.SellExchange,
                deal.SelectedBuyFee,
                deal.BuyOrderCount,
                deal.SellOrderCount,
                deal.TotalBuyCost,
                deal.TotalSellRevenue,
                deal.AvgBuyPrice,
                deal.AvgSellPrice,
                deal.MinBuyPrice,
                deal.MaxSellPrice,
                deal.MinBuyVolume,
                deal.MaxSellVolume,
                deal.MaxProfitUSDT,
                deal.ProfitPercentage,
                deal.MinProfitUSDT,
                deal.MinProfitPercentage,
                deal.Deposit,
                deal.CustomTotalBuyCost,
                deal.CustomProfitUSDT,
                deal.CustomProfitPercentage,
                0);

            return data;
        }
        private bool VerificateTrade(ProfitTrade trade, double deposit, out SpotSpotDeal? deal)
        {
            deal = null;

            try
            {
                if (!trade.BuyOrderBook.Asks.Any() || !trade.SellOrderBook.Bids.Any())
                    return false;

                var buyOrders = trade.BuyOrderBook.Asks
                    .GroupBy(o => o.price)
                    .ToDictionary(g => g.Key, g => g.Sum(v => v.volume));

                var sellOrders = trade.SellOrderBook.Bids
                    .GroupBy(o => o.price)
                    .ToDictionary(g => g.Key, g => g.Sum(v => v.volume));

                var selectedNetwork = trade.Networks
                    .Where(n => n.BuyWithdraw == true && n.BuyActive == true && n.SellActive == true)
                    .OrderBy(n => n.BuyFee)
                    .FirstOrDefault();

                var (selectedNetworkName, selectedBuyFeeBase) = selectedNetwork == null
                    ? ("<no data>", 0)
                    : (selectedNetwork.Name, selectedNetwork.BuyFee ?? 0);

                var calculationResult = CalculateTradeProfit(
                    buyOrders,
                    sellOrders,
                    selectedBuyFeeBase,
                    deposit);

                if (calculationResult.MaxProfitUSDT <= 0)
                    return false;

                deal = CreateSpotSpotDeal(trade, selectedNetworkName, calculationResult);
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"Error in 'VerificateTrade' method for pair {trade.Pair.GetName()}");
                return false;
            }
        }

        private static (double MaxProfitUSDT, double ProfitPercentage, double SelectedBuyFee,
                 int BuyOrderCount, int SellOrderCount, double TotalBuyCost, double TotalSellRevenue,
                 double AvgBuyPrice, double AvgSellPrice, double MinBuyPrice, double MaxSellPrice,
                 double MinBuyVolume, double MaxSellVolume, double MinProfitUSDT, double MinProfitPercentage,
                 double CustomTotalBuyCost, double CustomProfitUSDT, double CustomProfitPercentage)
            CalculateTradeProfit(
                Dictionary<double, double> buyOrders,
                Dictionary<double, double> sellOrders,
                double selectedBuyFeeBase,
                double deposit)
        {
            double totalVolume = 0;
            double totalBuyCost = 0;
            double totalSellRevenue = 0;
            double customTotalBuyCost = 0;
            double customTotalSellRevenue = 0;
            bool customFull = false;
            int buyOrderCount = 0;
            int sellOrderCount = 0;
            double minBuyPrice = double.MaxValue;
            double maxSellPrice = 0;
            double minBuyVolume = 0;
            double maxSellVolume = 0;

            var buyEnumerator = buyOrders.OrderBy(o => o.Key).GetEnumerator();
            var sellEnumerator = sellOrders.OrderByDescending(o => o.Key).GetEnumerator();

            bool hasBuy = buyEnumerator.MoveNext();
            bool hasSell = sellEnumerator.MoveNext();

            while (hasBuy && hasSell)
            {
                var buyPrice = buyEnumerator.Current.Key;
                var sellPrice = sellEnumerator.Current.Key;

                if (minBuyPrice > buyPrice)
                {
                    minBuyPrice = buyPrice;
                    minBuyVolume = buyEnumerator.Current.Value;
                }

                if (maxSellPrice < sellPrice)
                {
                    maxSellPrice = sellPrice;
                    maxSellVolume = sellEnumerator.Current.Value;
                }

                if (sellPrice <= buyPrice)
                    break;

                var tradeVolume = Math.Min(buyEnumerator.Current.Value, sellEnumerator.Current.Value);

                totalVolume += tradeVolume;
                totalBuyCost += tradeVolume * buyPrice;
                totalSellRevenue += tradeVolume * sellPrice;

                if (!customFull)
                {
                    if (totalBuyCost <= deposit)
                    {
                        customTotalBuyCost = totalBuyCost;
                        customTotalSellRevenue = totalSellRevenue;
                        customFull = totalBuyCost == deposit;
                    }
                    else
                    {
                        var customVolume = (deposit - customTotalBuyCost) / buyPrice;
                        customTotalSellRevenue += customVolume * sellPrice;
                        customTotalBuyCost = deposit;
                        customFull = true;
                    }
                }

                if (buyEnumerator.Current.Value >= sellEnumerator.Current.Value)
                {
                    sellOrderCount++;
                    hasSell = sellEnumerator.MoveNext();
                }

                if (buyEnumerator.Current.Value <= sellEnumerator.Current.Value)
                {
                    buyOrderCount++;
                    hasBuy = buyEnumerator.MoveNext();
                }
            }

            if (totalVolume == 0)
                return default;

            var avgBuyPrice = totalBuyCost / totalVolume;
            var avgSellPrice = totalSellRevenue / totalVolume;
            var selectedBuyFee = selectedBuyFeeBase * avgBuyPrice;

            var maxProfitUSDT = totalSellRevenue - totalBuyCost - selectedBuyFee;
            var profitPercentage = totalBuyCost != 0 ? (maxProfitUSDT / totalBuyCost) * 100 : 0;

            var minTradeVolume = Math.Min(minBuyVolume, maxSellVolume);
            var minTotalBuyCost = minTradeVolume * minBuyPrice;
            var minTotalSellRevenue = minTradeVolume * maxSellPrice;

            var minProfitUSDT = minTotalSellRevenue - minTotalBuyCost - selectedBuyFee;
            var minProfitPercentage = minTotalBuyCost != 0 ? (minProfitUSDT / minTotalBuyCost) * 100 : 0;

            var customProfitUSDT = customTotalSellRevenue - customTotalBuyCost - selectedBuyFee;
            var customProfitPercentage = customTotalBuyCost != 0 ? (customProfitUSDT / customTotalBuyCost) * 100 : 0;

            return (
                maxProfitUSDT, profitPercentage, selectedBuyFee,
                buyOrderCount > 0 ? buyOrderCount : 1,
                sellOrderCount > 0 ? sellOrderCount : 1,
                totalBuyCost, totalSellRevenue,
                avgBuyPrice, avgSellPrice,
                minBuyPrice, maxSellPrice,
                minBuyVolume, maxSellVolume,
                minProfitUSDT, minProfitPercentage,
                customTotalBuyCost, customProfitUSDT, customProfitPercentage);
        }

        private SpotSpotDeal CreateSpotSpotDeal(
            ProfitTrade trade,
            string networkName,
            (double MaxProfitUSDT, double ProfitPercentage, double SelectedBuyFee,
             int BuyOrderCount, int SellOrderCount, double TotalBuyCost, double TotalSellRevenue,
             double AvgBuyPrice, double AvgSellPrice, double MinBuyPrice, double MaxSellPrice,
             double MinBuyVolume, double MaxSellVolume, double MinProfitUSDT, double MinProfitPercentage,
             double CustomTotalBuyCost, double CustomProfitUSDT, double CustomProfitPercentage) calc)
        {
            return new SpotSpotDeal
            {
                EventTime = new DateTime(Math.Max(trade.BuyOrderBook.UpdatedTime.Ticks, trade.SellOrderBook.UpdatedTime.Ticks)),
                Symbol = trade.Pair.Symbol,
                NetworkName = networkName,
                BuyExchange = trade.Pair.BuyExchange,
                SellExchange = trade.Pair.SellExchange,
                SelectedBuyFee = calc.SelectedBuyFee,
                BuyOrderCount = calc.BuyOrderCount,
                SellOrderCount = calc.SellOrderCount,
                TotalBuyCost = calc.TotalBuyCost,
                TotalSellRevenue = calc.TotalSellRevenue,
                AvgBuyPrice = calc.AvgBuyPrice,
                AvgSellPrice = calc.AvgSellPrice,
                MinBuyPrice = calc.MinBuyPrice,
                MaxSellPrice = calc.MaxSellPrice,
                MinBuyVolume = calc.MinBuyVolume,
                MaxSellVolume = calc.MaxSellVolume,
                MaxProfitUSDT = calc.MaxProfitUSDT,
                ProfitPercentage = calc.ProfitPercentage,
                MinProfitUSDT = calc.MinProfitUSDT,
                MinProfitPercentage = calc.MinProfitPercentage,
                Deposit = deposit,
                CustomTotalBuyCost = calc.CustomTotalBuyCost,
                CustomProfitUSDT = calc.CustomProfitUSDT,
                CustomProfitPercentage = calc.CustomProfitPercentage
            };
        }
    }
}
