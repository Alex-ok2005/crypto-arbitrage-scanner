using ArbitrageScanner.Interfaces;
using ArbitrageScanner.Models;
using Microsoft.Extensions.Hosting;
using NLog;
using System.Data;

namespace ArbitrageScanner.Services
{
    public class CollectorService : BackgroundService
    {
        private readonly Logger _logger;
        private readonly IDBService _dbService;
        private readonly IExchangeService _exchangeService;

        public delegate void ProfitTradeHandler(ProfitTrade[] trades, CancellationToken token);
        public event ProfitTradeHandler EventProfitTrade;

        // Configuration parameters
        private readonly CollectorConfig _config = new()
        {
            WhiteListExchange = [],
            WhiteListSymbols = [],
            BlackListExchange = [],
            BlackListSymbols = [],
            MinVolume = 90000,
            MinProfitPercentage = 0,
            MaxProfitPercentage = 100,
            UpdateLagSeconds = 300,
            CollectionIntervalMs = 120000,
            DelayBetweenRequestsMs = 1000
        };

        public CollectorService(Logger logger, IDBService dbService, IExchangeService exchangeService)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _dbService = dbService ?? throw new ArgumentNullException(nameof(dbService));
            _exchangeService = exchangeService ?? throw new ArgumentNullException(nameof(exchangeService));

            _dbService.TruncateTables();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.Info("CollectorService started");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await CollectDataCycleAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    _logger.Info("CollectorService is stopping");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "Error in main data collection loop");
                    await Task.Delay(5000, stoppingToken);
                }
            }
        }
        private async Task CollectDataCycleAsync(CancellationToken stoppingToken)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // Collect data from exchanges
            await GetExchangesDataAsync(stoppingToken);

            // Retrieve and publish profitable trades
            await ProcessProfitTradesAsync(stoppingToken);

            stopwatch.Stop();
            _logger.Info($"Exchange data loading completed in {stopwatch.Elapsed.TotalSeconds:0.00} seconds.");

            // Wait for the next data collection cycle
            if (!stoppingToken.IsCancellationRequested)
            {
                var delay = Math.Max(0, _config.CollectionIntervalMs - stopwatch.ElapsedMilliseconds);
                if (delay > 0)
                {
                    _logger.Info($"Waiting {_config.CollectionIntervalMs / 1000} seconds until the next loading...");
                    await Task.Delay((int)delay, stoppingToken);
                }
            }
        }
        private async Task ProcessProfitTradesAsync(CancellationToken token)
        {
            try
            {
                var profitTrades = await _dbService.GetProfitTrades(
                    _config.MinVolume,
                    (_config.MinProfitPercentage, _config.MaxProfitPercentage),
                    _config.WhiteListExchange,
                    _config.UpdateLagSeconds);

                if (profitTrades?.Length > 0)
                {
                    EventProfitTrade?.Invoke(profitTrades, token);
                    _logger.Info($"{profitTrades.Length} profitable trades published");
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Error while processing profitable trades");
            }
        }
        private async Task GetExchangesDataAsync(CancellationToken stoppingToken)
        {
            var exchanges = GetFilteredExchanges();
            _logger.Info($"Starting data collection for {exchanges.Count} exchanges");

            var tasks = exchanges.Select(exchangeId =>
                ProcessExchangeDataAsync(exchangeId, stoppingToken)).ToList();

            await Task.WhenAll(tasks);
            _logger.Info($"Data collection completed for {exchanges.Count} exchanges");
        }
        private List<string> GetFilteredExchanges()
        {
            var exchanges = _exchangeService.Exchanges.Keys.ToList();

            if (_config.WhiteListExchange.Length > 0)
            {
                exchanges = exchanges.Intersect(_config.WhiteListExchange).ToList();
            }

            if (_config.BlackListExchange.Length > 0)
            {
                exchanges = exchanges.Except(_config.BlackListExchange).ToList();
            }

            return exchanges;
        }
        private async Task ProcessExchangeDataAsync(string exchangeId, CancellationToken stoppingToken)
        {
            try
            {
                var (fee, exchange, symbolQueue) = _exchangeService.Exchanges[exchangeId];
                if (exchange == null) return;

                var exchangeName = _exchangeService.Exchanges[exchangeId].exchange.name;

                // Retrieve market data
                await ProcessExchangeDataSectionAsync(
                    () => _exchangeService.GetMarketData(exchangeId),
                    $"market data for {exchangeName}",
                    "arbitrage.market_data",
                    stoppingToken);

                await Task.Delay(_config.DelayBetweenRequestsMs, stoppingToken);

                // Retrieve currency data
                await ProcessExchangeDataSectionAsync(
                    () => _exchangeService.GetCurrencies(exchangeId),
                    $"currency data for {exchangeName}",
                    "arbitrage.currencies",
                    stoppingToken);

                await Task.Delay(_config.DelayBetweenRequestsMs, stoppingToken);

                // Retrieve ticker data
                await ProcessExchangeDataSectionAsync(
                    () => _exchangeService.GetTickers(exchangeId),
                    $"ticker data for {exchangeName}",
                    "arbitrage.trading_data",
                    stoppingToken);

                _logger.Info($"Data collection completed for {exchangeName}");
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"Error while processing exchange {exchangeId}");
            }
        }
        private async Task ProcessExchangeDataSectionAsync(
            Func<Task<DataTable>> dataGetter,
            string dataDescription,
            string tableName,
            CancellationToken stoppingToken)
        {
            try
            {
                var data = await dataGetter();
                if (data == null || data.Rows.Count == 0)
                {
                    _logger.Warn($"{dataDescription} not received");
                    return;
                }

                var success = await _dbService.Export(data, tableName, stoppingToken);
                if (success)
                {
                    _logger.Info($"{dataDescription} successfully exported to {tableName}");
                }
                else
                {
                    _logger.Warn($"Failed to export {dataDescription} to {tableName}");
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"Error while processing {dataDescription}");
            }
        }
        private class CollectorConfig
        {
            public string[] WhiteListExchange { get; init; }
            public string[] WhiteListSymbols { get; init; }
            public string[] BlackListExchange { get; init; }
            public string[] BlackListSymbols { get; init; }
            public long MinVolume { get; init; }
            public int MinProfitPercentage { get; init; }
            public int MaxProfitPercentage { get; init; }
            public int UpdateLagSeconds { get; init; }
            public int CollectionIntervalMs { get; init; }
            public int DelayBetweenRequestsMs { get; init; }
        }
    }
}
