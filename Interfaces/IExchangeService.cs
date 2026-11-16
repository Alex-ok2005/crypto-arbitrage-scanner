using ccxt;
using ArbitrageScanner.Models;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace ArbitrageScanner.Interfaces
{
    /// <summary>
    /// Делегат для передачи обновлений стаканов из фоновых задач опроса бирж.
    /// </summary>
    /// <param name="exchangeId">Идентификатор биржи, которая сформировала стакан.</param>
    /// <param name="orderBook">Проверенный снимок стакана.</param>
    /// <param name="token">Токен отмены, переданный вызывающим кодом.</param>
    public delegate void putOrderBookHandler(string exchangeId, VerifyOrderBook orderBook, CancellationToken token);

    /// <summary>
    /// Описывает сервис доступа к биржам, их метаданным, тикерам, валютам и очередям запросов стаканов.
    /// </summary>
    public interface IExchangeService
    {
        /// <summary>
        /// Событие, возникающее при появлении нового снимка стакана.
        /// </summary>
        event putOrderBookHandler EventOrderBook;
        /// <summary>
        /// Список бирж с соответствующими комиссиями тейкера, потокобезопасными очередями и настройками URL спотовых рынков.
        /// </summary>
        Dictionary<string, (decimal fee, Exchange exchange, ExchangeSymbolQueue symbolQueue, ExchangeSpotUrlSettings spotUrlSettings)> Exchanges { get; }
        /// <summary>
        /// Получает описание рынков для указанной биржи.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи для запроса.</param>
        /// <returns>Таблица данных с метаданными рынков указанной биржи.</returns>
        Task<DataTable> GetMarketData(string exchangeId);
        /// <summary>
        /// Получает данные тикеров для указанной биржи.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи для запроса.</param>
        /// <returns>Таблица данных с актуальными тикерами.</returns>
        Task<DataTable> GetTickers(string exchangeId);
        /// <summary>
        /// Получает метаданные валют, включая сведения о сетях, для указанной биржи.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи для запроса.</param>
        /// <returns>Таблица данных с информацией о валютах.</returns>
        Task<DataTable> GetCurrencies(string exchangeId);
        /// <summary>
        /// Запрашивает снимок стакана для указанного торгового символа.
        /// </summary>
        /// <param name="exchangeId">Идентификатор биржи для запроса.</param>
        /// <param name="symbol">Торговый символ, по которому требуется стакан.</param>
        /// <returns>Данные стакана или <c>null</c>, если получить их не удалось.</returns>
        Task<VerifyOrderBook?> GetOrderBook(string exchangeId, string symbol);
    }
}
