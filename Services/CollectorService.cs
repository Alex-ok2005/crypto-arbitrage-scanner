using ArbitrageScanner.Interfaces;
using ArbitrageScanner.Models;
using Microsoft.Extensions.Hosting;
using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ArbitrageScanner.Services
{
    public class CollectorService : BackgroundService
    {
        private readonly Logger _logger;
        private readonly IDBService _dbService;
        private readonly IExchangeService _exchangeService;

        public delegate void ProfitTradeHandler(ProfitTrade[] trades, CancellationToken token);
        public event ProfitTradeHandler EventProfitTrade;

        // Конфигурационные параметры
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
            CollectionIntervalMs = 150000,
            DelayBetweenRequestsMs = 1000
        };

        /// <summary>
        /// Инициализирует коллектор зависимостями и подготавливает службы для сбора данных.
        /// </summary>
        /// <param name="logger">Логгер для диагностики.</param>
        /// <param name="dbService">Сервис базы данных для работы с ClickHouse.</param>
        /// <param name="exchangeService">Сервис бирж, предоставляющий данные рынков и тикеров.</param>
        public CollectorService(Logger logger, IDBService dbService, IExchangeService exchangeService)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _dbService = dbService ?? throw new ArgumentNullException(nameof(dbService));
            _exchangeService = exchangeService ?? throw new ArgumentNullException(nameof(exchangeService));
        }

        /// <summary>
        /// Основной фоновый цикл до отмены: управляет сбором данных и обработкой сделок.
        /// </summary>
        /// <param name="stoppingToken">Токен отмены, сигнализирующий остановку сервиса.</param>
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
                    _logger.Error(ex, "Ошибка в основном цикле сбора данных");
                    await Task.Delay(5000, stoppingToken);
                }
            }
        }
        /// <summary>
        /// Выполняет один цикл сбора данных: запросы к биржам и получение прибыльных сделок.
        /// </summary>
        /// <param name="stoppingToken">Токен отмены для досрочного завершения цикла.</param>
        private async Task CollectDataCycleAsync(CancellationToken stoppingToken)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // Получаем данные с бирж
            await GetExchangesDataAsync(stoppingToken);

            // Получаем и публикуем прибыльные сделки
            await ProcessProfitTradesAsync(stoppingToken);

            stopwatch.Stop();
            _logger.Info($"Exchange data loading completed in {stopwatch.Elapsed.TotalSeconds:0.00} seconds.");

            // Ожидаем следующий цикл сбора данных
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
        /// <summary>
        /// Получает прибыльные сделки из базы и публикует их на верификацию.
        /// </summary>
        /// <param name="token">Токен отмены для операций публикации.</param>
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
                _logger.Error(ex, "Ошибка при обработке прибыльных сделок");
            }
        }
        /// <summary>
        /// Запрашивает данные рынков, валют и тикеров по настроенным биржам.
        /// </summary>
        /// <param name="stoppingToken">Токен отмены для прерывания длительных операций.</param>
        private async Task GetExchangesDataAsync(CancellationToken stoppingToken)
        {
            var exchanges = GetFilteredExchanges();
            _logger.Info($"Starting data collection for {exchanges.Count} exchanges");

            var tasks = exchanges.Select(exchangeId =>
                ProcessExchangeDataAsync(exchangeId, stoppingToken)).ToArray();

            await Task.WhenAll(tasks);
            _logger.Info($"Data collection completed for {exchanges.Count} exchanges");
        }
        /// <summary>
        /// Применяет фильтр белого/чёрного списка к списку бирж.
        /// </summary>
        /// <returns>Отфильтрованный набор идентификаторов бирж.</returns>
        private List<string> GetFilteredExchanges()
        {
            var exchanges = _exchangeService.Exchanges.Keys.ToList();

            if (_config.WhiteListExchange.Length > 0)
            {
                var whiteList = new HashSet<string>(_config.WhiteListExchange);
                exchanges.RemoveAll(exchangeId => !whiteList.Contains(exchangeId));
            }

            if (_config.BlackListExchange.Length > 0)
            {
                var blackList = new HashSet<string>(_config.BlackListExchange);
                exchanges.RemoveAll(exchangeId => blackList.Contains(exchangeId));
            }

            return exchanges;
        }
        /// <summary>
        /// Последовательно обрабатывает данные рынков, валют и тикеров для указанной биржи.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи, с которой собираются данные.</param>
        /// <param name="stoppingToken">Токен отмены для операции.</param>
        private async Task ProcessExchangeDataAsync(string exchangeId, CancellationToken stoppingToken)
        {
            try
            {
                if (!_exchangeService.Exchanges.TryGetValue(exchangeId, out var exchangeData))
                {
                    _logger.Warn($"Биржа {exchangeId} не найдена в конфигурации");
                    return;
                }

                var (_, exchange, _, _) = exchangeData;
                if (exchange == null)
                {
                    _logger.Warn($"Биржа {exchangeId} не инициализирована");
                    return;
                }

                var exchangeName = exchange.name;

                var sections = new (Func<Task<DataTable>> dataGetter, string dataDescription, string tableName)[]
                {
                    (() => _exchangeService.GetMarketData(exchangeId), $"market data for {exchangeName}", "arbitrage.market_data"),
                    (() => _exchangeService.GetCurrencies(exchangeId), $"currency data for {exchangeName}", "arbitrage.currencies"),
                    (() => _exchangeService.GetTickers(exchangeId), $"ticker data for {exchangeName}", "arbitrage.trading_data")
                };

                for (var index = 0; index < sections.Length; index++)
                {
                    var (dataGetter, dataDescription, tableName) = sections[index];
                    await ProcessExchangeDataSectionAsync(
                        dataGetter,
                        dataDescription,
                        tableName,
                        stoppingToken);

                    if (index < sections.Length - 1)
                    {
                        await DelayBetweenRequestsAsync(stoppingToken);
                    }
                }

                _logger.Info($"Сбор данных завершён для {exchangeName}");
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"Ошибка при обработке биржи {exchangeId}");
            }
        }
        /// <summary>
        /// Делает паузу между запросами к биржам с учётом настроенной задержки.
        /// </summary>
        /// <param name="stoppingToken">Токен отмены для прерывания ожидания.</param>
        private async Task DelayBetweenRequestsAsync(CancellationToken stoppingToken)
        {
            if (_config.DelayBetweenRequestsMs <= 0)
            {
                return;
            }

            await Task.Delay(_config.DelayBetweenRequestsMs, stoppingToken);
        }
        /// <summary>
        /// Оборачивает выполнение и экспорт одного блока данных (рынки, валюты или тикеры).
        /// </summary>
        /// <param name="dataGetter">Функция, возвращающая данные для экспорта.</param>
        /// <param name="dataDescription">Человеко-понятное описание обрабатываемых данных.</param>
        /// <param name="tableName">Имя таблицы ClickHouse для экспорта.</param>
        /// <param name="stoppingToken">Токен отмены для прерывания экспорта.</param>
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
                    _logger.Warn($"{dataDescription} не получены");
                    return;
                }

                var success = await _dbService.Export(data, tableName, stoppingToken);
                if (success)
                {
                    _logger.Info($"{dataDescription} успешно экспортированы в {tableName}");
                }
                else
                {
                    _logger.Warn($"Не удалось экспортировать {dataDescription} в {tableName}");
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
