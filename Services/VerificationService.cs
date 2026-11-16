using ArbitrageScanner.Interfaces;
using ArbitrageScanner.Models;
using Microsoft.Extensions.Hosting;
using NLog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ArbitrageScanner.Services
{
    /// <summary>
    /// Проверяет найденные арбитражные сделки по актуальным стаканам и сохраняет подтверждённые результаты.
    /// </summary>
    public class VerificationService : BackgroundService
    {
        private readonly Logger _logger;
        private readonly IDBService _dBService;
        private readonly IExchangeService _exService;
        private readonly CollectorService _collectorService;

        private readonly double deposit = 100;      // Тестовый размер депозита для расчёта прибыли в валюте котировки
        private int _processedDealsCount = 1;       // Счётчик подтверждённых сделок
        private readonly object _locker = new();    // Объект синхронизации

        private static readonly Dictionary<PairStruct, ProfitTrade> ProfitTrades = [];

        /// <summary>
        /// Инициализирует сервис верификации и подписывается на события бирж и коллектора.
        /// </summary>
        /// <param name="logger">Логгер для диагностического вывода.</param>
        /// <param name="dBService">Сервис базы данных для сохранения результатов.</param>
        /// <param name="exService">Сервис бирж, используемый для постановки в очередь запросов стаканов.</param>
        /// <param name="collectorService">Сервис-коллектор, формирующий кандидаты прибыльных трейдов.</param>
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
        /// <summary>
        /// Непрерывно обрабатывает сделки, пока не поступит запрос на отмену.
        /// </summary>
        /// <param name="stoppingToken">Токен отмены, полученный от хоста.</param>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await ProcessNextTradeAsync();
                await Task.Delay(10, stoppingToken); // Небольшая задержка, чтобы снизить нагрузку на CPU
            }
        }
        /// <summary>
        /// Выбирает следующую сделку из очереди по правилам приоритета и планирует её обработку.
        /// </summary>
        private async Task ProcessNextTradeAsync()
        {
            ProfitTrade? tradeToProcess = null;

            // Выбор сделки для проверки/перепроверки
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
        /// <summary>
        /// Обновляет состояние сделки и ставит нужные символы в очередь для получения стаканов.
        /// </summary>
        /// <param name="trade">Сделка для обработки.</param>
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
        /// <summary>
        /// Добавляет символы сделки в очереди бирж, чтобы инициировать загрузку стаканов.
        /// </summary>
        /// <param name="trade">Сделка, определяющая какие символы ставить в очередь.</param>
        private Task EnqueueSymbolsForProcessing(ProfitTrade trade)
        {
            try
            {
                var buyExchange = _exService.Exchanges[trade.Pair.BuyExchange];
                var sellExchange = _exService.Exchanges[trade.Pair.SellExchange];

                var buySymbol = string.IsNullOrWhiteSpace(trade.Pair.BuySymbol)
                    ? trade.Pair.Symbol
                    : trade.Pair.BuySymbol;
                var sellSymbol = string.IsNullOrWhiteSpace(trade.Pair.SellSymbol)
                    ? trade.Pair.Symbol
                    : trade.Pair.SellSymbol;

                buyExchange.symbolQueue.EnqueueIfMissing(buySymbol);
                sellExchange.symbolQueue.EnqueueIfMissing(sellSymbol);
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Ошибка при постановке символов в очередь на обработку");
            }

            return Task.CompletedTask;
        }
        /// <summary>
        /// Обрабатывает пачки прибыльных сделок, поступающие от сервиса-коллектора.
        /// </summary>
        /// <param name="trades">Сделки для добавления или обновления.</param>
        /// <param name="token">Токен отмены, переданный вызывающим кодом.</param>
        private void ProfitTradeHandler(ProfitTrade[] trades, CancellationToken token)
        {
            // Удаляем протухшие сделки
            lock (_locker)
            {
                var keysToRemove = ProfitTrades
                    .Where(kvp => kvp.Value.State == ProfitTradesState.Expired || kvp.Value.State == ProfitTradesState.Unconfirmed)
                    .Select(kvp => kvp.Key)
                    .ToList();

                _logger.Info($"Удаляем {keysToRemove.Count()} истёкших или неподтверждённых сделок из очереди.");

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
        /// <summary>
        /// Обрабатывает входящие обновления стаканов и запускает верификацию, когда доступны обе стороны.
        /// </summary>
        /// <param name="exchangeId">Биржа, предоставившая стакан.</param>
        /// <param name="orderBook">Снимок стакана.</param>
        /// <param name="token">Токен отмены от вызывающего кода.</param>
        private void OrderBookHandler(string exchangeId, VerifyOrderBook orderBook, CancellationToken token)
        {
            try
            {
                List<ProfitTrade> tradesToProcess;

                lock (_locker)
                {
                    tradesToProcess = ProfitTrades.Values
                        .Where(p => IsOrderBookMatch(exchangeId, orderBook.Symbol, p) &&
                            (p.State is ProfitTradesState.Checking or ProfitTradesState.ReChecking) &&
                            (p.Pair.BuyExchange == exchangeId || p.Pair.SellExchange == exchangeId))
                        .OrderBy(p => p.EventTime)
                        .ToList();
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
        /// <summary>
        /// Проверяет, соответствует ли символ стакана символу сделки для конкретной биржи.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи, от которой пришёл стакан.</param>
        /// <param name="symbol">Символ рынка, полученный в стакане.</param>
        /// <param name="trade">Сделка, по которой выполняется сопоставление.</param>
        /// <returns><c>true</c>, если символ стакана подходит для покупки или продажи на бирже.</returns>
        private static bool IsOrderBookMatch(string exchangeId, string symbol, ProfitTrade trade)
        {
            if (trade.Pair.BuyExchange == exchangeId)
            {
                var buySymbol = string.IsNullOrWhiteSpace(trade.Pair.BuySymbol)
                    ? trade.Pair.Symbol
                    : trade.Pair.BuySymbol;
                return buySymbol == symbol;
            }

            if (trade.Pair.SellExchange == exchangeId)
            {
                var sellSymbol = string.IsNullOrWhiteSpace(trade.Pair.SellSymbol)
                    ? trade.Pair.Symbol
                    : trade.Pair.SellSymbol;
                return sellSymbol == symbol;
            }

            return false;
        }
        /// <summary>
        /// Обновляет запись сделки в памяти последним стаканом и запускает расчёт прибыли, когда данные готовы.
        /// </summary>
        /// <param name="trade">Сделка для обновления.</param>
        /// <param name="exchangeId">Биржа, которой принадлежит полученный стакан.</param>
        /// <param name="orderBook">Снимок стакана.</param>
        /// <param name="token">Токен отмены.</param>
        private void ProcessOrderBookForTrade(ProfitTrade trade, string exchangeId, VerifyOrderBook orderBook, CancellationToken token)
        {
            Func<Task>? databaseAction = null;

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
                    databaseAction = UpdateTradeWithOrderBooks(updatedTrade, token);
                }

                var pairKey = new PairStruct(trade.Pair.Symbol, trade.Pair.BuyExchange, trade.Pair.SellExchange);
                ProfitTrades[pairKey] = updatedTrade;
            }

            if (databaseAction != null)
            {
                _ = RunDatabaseActionAsync(databaseAction);
            }
        }
        /// <summary>
        /// Применяет данные стаканов к сделке, проверяет прибыльность и сохраняет подтверждённые сделки.
        /// </summary>
        /// <param name="trade">Обновляемая сделка.</param>
        /// <param name="token">Токен отмены для операций с БД.</param>
        /// <returns>Отложенное действие для записи результата в БД или <c>null</c>, если запись не нужна.</returns>
        private Func<Task>? UpdateTradeWithOrderBooks(ProfitTrade trade, CancellationToken token)
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
                if (deal is null)
                {
                    return null;
                }

                return () => SaveVerifiedDealAsync(deal, trade, oldState, token);
            }

            if (trade.State == ProfitTradesState.Expired)
            {
                return () => RemoveDealAsync(trade, token);
            }

            return null;
        }
        /// <summary>
        /// Запускает отложенное действие с БД и логирует ошибку, если действие завершилось исключением.
        /// </summary>
        /// <param name="databaseAction">Асинхронное действие с базой данных.</param>
        private async Task RunDatabaseActionAsync(Func<Task> databaseAction)
        {
            try
            {
                await databaseAction();
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Ошибка при выполнении отложенного действия с базой данных");
            }
        }
        /// <summary>
        /// Сохраняет подтверждённую сделку и удаляет устаревшие записи в зависимости от предыдущего состояния.
        /// </summary>
        /// <param name="deal">Рассчитанная сделка spot-to-spot.</param>
        /// <param name="trade">Метаданные исходной сделки.</param>
        /// <param name="oldState">Предыдущее состояние сделки перед верификацией.</param>
        /// <param name="token">Токен отмены для операций с базой данных.</param>
        private async Task SaveVerifiedDealAsync(SpotSpotDeal deal, ProfitTrade trade, ProfitTradesState oldState, CancellationToken token)
        {
            try
            {
                token.ThrowIfCancellationRequested();
                var data = CreateDealDataTable(deal);

                if (await _dBService.RemoveItem(trade, "arbitrage.profit_trades") &&
                    await _dBService.Export(data, "arbitrage.profit_trades", token))
                {
                    var messagePrefix = oldState == ProfitTradesState.Checking
                        ? $"Найдена новая сделка N{Interlocked.Increment(ref _processedDealsCount) - 1}"
                        : "Сделка перепроверена";

                    _logger.Info($"{messagePrefix}: {deal.Symbol} | {deal.BuyExchange} => {deal.SellExchange} | сеть: {deal.NetworkName} | прибыль: {string.Format("{0:f2}", deal.ProfitPercentage)}%");
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
        /// <summary>
        /// Удаляет сделку из базы данных, когда она истекла или стала недействительной.
        /// </summary>
        /// <param name="trade">Сделка для удаления.</param>
        /// <param name="token">Токен отмены.</param>
        private async Task RemoveDealAsync(ProfitTrade trade, CancellationToken token)
        {
            try
            {
                token.ThrowIfCancellationRequested();
                if (await _dBService.RemoveItem(trade, "arbitrage.profit_trades"))
                {
                    _logger.Info($"Сделка истекла. {trade.Pair.GetName()}");
                }
                else
                {
                    _logger.Warn("Не удалось удалить сделку из базы данных.");
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Ошибка при удалении сделки из базы данных");
            }
        }
        /// <summary>
        /// Формирует <see cref="DataTable"/> представление проверенной сделки для экспорта.
        /// </summary>
        /// <param name="deal">Подтверждённая сделка для преобразования.</param>
        /// <returns>Таблица данных с одной заполненной строкой.</returns>
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
                new("buy_exchange_url", typeof(string)),
                new("sell_exchange_url", typeof(string)),
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
                deal.BuyExchangeUrl ?? (object)DBNull.Value,
                deal.SellExchangeUrl ?? (object)DBNull.Value,
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
        /// <summary>
        /// Проверяет прибыльность, используя агрегированные стаканы, и формирует сделку при успешном результате.
        /// </summary>
        /// <param name="trade">Сделка для проверки.</param>
        /// <param name="deposit">Размер депозита для моделирования сделки.</param>
        /// <param name="deal">Полученная сделка при успешной проверке.</param>
        /// <returns><c>true</c>, если сделка приносит прибыль.</returns>
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
                _logger.Error(ex, $"Ошибка в методе 'VerificateTrade' для пары {trade.Pair.GetName()}");
                return false;
            }
        }
        /// <summary>
        /// Агрегирует стаканы покупки и продажи, чтобы вычислить метрики прибыльности для указанного депозита.
        /// </summary>
        /// <param name="buyOrders">Агрегированные заявки на покупку, сгруппированные по цене.</param>
        /// <param name="sellOrders">Агрегированные заявки на продажу, сгруппированные по цене.</param>
        /// <param name="selectedBuyFeeBase">Сетевая комиссия за единицу для стороны покупки.</param>
        /// <param name="deposit">Сумма депозита в валюте котировки.</param>
        /// <returns>Кортеж рассчитанных показателей прибыльности.</returns>
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

        /// <summary>
        /// Создаёт DTO сделки spot-to-spot из результатов расчётов и метаданных сделки.
        /// </summary>
        /// <param name="trade">Исходная сделка.</param>
        /// <param name="networkName">Название выбранной сети перевода.</param>
        /// <param name="calc">Предварительно рассчитанные метрики прибыльности.</param>
        /// <returns>Сделка spot-to-spot, готовая к экспорту.</returns>
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
                Symbol = trade.Pair.GetProfitTradesSymbol(),
                Base = trade.Pair.Base,
                Quote = trade.Pair.Quote,
                NetworkName = networkName,
                BuyExchange = trade.Pair.BuyExchange,
                SellExchange = trade.Pair.SellExchange,
                BuyExchangeUrl = GetExchangeWebsiteUrl(trade.Pair.BuyExchange, trade.Pair),
                SellExchangeUrl = GetExchangeWebsiteUrl(trade.Pair.SellExchange, trade.Pair),
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

        /// <summary>
        /// Определяет предпочтительный URL сайта для отображения страницы рынка биржи.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи.</param>
        /// <param name="pair">Торговая пара, для которой требуется сформировать URL.</param>
        /// <returns>Определённый URL или <c>null</c>, если он недоступен.</returns>
        private string? GetExchangeWebsiteUrl(string exchangeId, Pair pair)
        {
            try
            {
                if (!_exService.Exchanges.TryGetValue(exchangeId, out var exchangeData))
                {
                    return null;
                }

                var spotUrlSettings = exchangeData.spotUrlSettings;
                var (baseSymbol, quoteSymbol) = pair.GetSymbolsForExchange(exchangeId);

                if (!string.IsNullOrWhiteSpace(spotUrlSettings?.Template)
                    && !string.IsNullOrWhiteSpace(baseSymbol)
                    && !string.IsNullOrWhiteSpace(quoteSymbol))
                {
                    return BuildSpotMarketUrl(spotUrlSettings.Template, baseSymbol, quoteSymbol);
                }

                if (!string.IsNullOrWhiteSpace(spotUrlSettings?.FallbackUrl))
                {
                    return spotUrlSettings.FallbackUrl;
                }

                var exchange = exchangeData.exchange;
                var urlsSource = exchange.urls;

                var preferredUrl = ExtractUrl(urlsSource, "markets", "trade");
                if (!string.IsNullOrWhiteSpace(preferredUrl))
                {
                    return preferredUrl;
                }

                var baseUrl = ExtractUrl(urlsSource, "www")
                    ?? ExtractUrl(urlsSource, "logo")
                    ?? ExtractUrl(urlsSource, "referral");

                if (string.IsNullOrWhiteSpace(baseUrl))
                {
                    return null;
                }

                return AppendRelativePath(baseUrl, "markets");
            }
            catch (Exception ex)
            {
                _logger.Warn(ex, $"Failed to resolve website URL for exchange '{exchangeId}'.");
                return null;
            }
        }

        /// <summary>
        /// Собирает URL страницы спотового рынка на основе шаблона и обозначений базовой/котируемой валюты.
        /// </summary>
        /// <param name="template">Шаблон URL с плейсхолдерами валют.</param>
        /// <param name="baseSymbol">Символ базовой валюты.</param>
        /// <param name="quoteSymbol">Символ котируемой валюты.</param>
        /// <returns>Сформированный URL для страницы спота.</returns>
        private static string BuildSpotMarketUrl(string template, string baseSymbol, string quoteSymbol)
        {
            var upperBase = baseSymbol.ToUpperInvariant();
            var upperQuote = quoteSymbol.ToUpperInvariant();
            var lowerBase = baseSymbol.ToLowerInvariant();
            var lowerQuote = quoteSymbol.ToLowerInvariant();

            return template
                .Replace("{BASE}", upperBase)
                .Replace("{QUOTE}", upperQuote)
                .Replace("{BASE_UPPER}", upperBase)
                .Replace("{QUOTE_UPPER}", upperQuote)
                .Replace("{BASE_LOWER}", lowerBase)
                .Replace("{QUOTE_LOWER}", lowerQuote);
        }

        /// <summary>
        /// Пытается извлечь URL из источника, используя приоритетный список ключей.
        /// </summary>
        /// <param name="urlsSource">Источник, который может содержать сопоставления URL.</param>
        /// <param name="keys">Упорядоченный список ключей для поиска.</param>
        /// <returns>Найденный URL или <c>null</c>.</returns>
        private static string? ExtractUrl(object? urlsSource, params string[] keys)
        {
            foreach (var key in keys)
            {
                var value = ExtractUrlForKey(urlsSource, key);
                if (!string.IsNullOrWhiteSpace(value))
                {
                    return value;
                }
            }

            return null;
        }

        /// <summary>
        /// Извлекает URL по одному ключу из словарей с различными типами значений.
        /// </summary>
        /// <param name="urlsSource">Источник наподобие словаря, содержащий значения URL.</param>
        /// <param name="key">Ключ для поиска.</param>
        /// <returns>Строка URL при успехе, иначе <c>null</c>.</returns>
        private static string? ExtractUrlForKey(object? urlsSource, string key)
        {
            switch (urlsSource)
            {
                case IDictionary<string, object> objectDict when TryGetValueIgnoreCase(objectDict, key, out var valueObj):
                    return valueObj?.ToString();
                case IDictionary<string, string> stringDict when TryGetValueIgnoreCase(stringDict, key, out var valueStr):
                    return valueStr;
                case IDictionary dictionary:
                    foreach (DictionaryEntry entry in dictionary)
                    {
                        if (entry.Key is string entryKey && string.Equals(entryKey, key, StringComparison.OrdinalIgnoreCase))
                        {
                            return entry.Value?.ToString();
                        }
                    }
                    break;
            }

            return null;
        }

        /// <summary>
        /// Извлекает значение из словаря, игнорируя регистр ключа.
        /// </summary>
        /// <typeparam name="T">Тип значения.</typeparam>
        /// <param name="dictionary">Словарь для проверки.</param>
        /// <param name="key">Ключ для поиска.</param>
        /// <param name="value">Выходное значение при успешном поиске.</param>
        /// <returns><c>true</c> при совпадении ключа.</returns>
        private static bool TryGetValueIgnoreCase<T>(IDictionary<string, T> dictionary, string key, out T value)
        {
            if (dictionary.TryGetValue(key, out value))
            {
                return true;
            }

            foreach (var kvp in dictionary)
            {
                if (string.Equals(kvp.Key, key, StringComparison.OrdinalIgnoreCase))
                {
                    value = kvp.Value;
                    return true;
                }
            }

            value = default!;
            return false;
        }

        /// <summary>
        /// Добавляет относительный путь к базовому URL биржи, нормализуя слэши.
        /// </summary>
        /// <param name="baseUrl">Базовый URL биржи.</param>
        /// <param name="relativePath">Относительный путь для добавления.</param>
        /// <returns>Скомбинированный URL.</returns>
        private static string AppendRelativePath(string baseUrl, string relativePath)
        {
            if (string.IsNullOrWhiteSpace(baseUrl))
            {
                return relativePath;
            }

            var trimmedBase = baseUrl.Trim();
            var trimmedTarget = relativePath.Trim('/');

            if (trimmedBase.TrimEnd('/').EndsWith(trimmedTarget, StringComparison.OrdinalIgnoreCase))
            {
                return trimmedBase;
            }

            if (Uri.TryCreate(trimmedBase, UriKind.Absolute, out var baseUri))
            {
                return new Uri(baseUri, trimmedTarget).ToString();
            }

            if (!trimmedBase.EndsWith('/'))
            {
                trimmedBase += "/";
            }

            return trimmedBase + trimmedTarget;
        }
    }
}
