using ArbitrageScanner.Models;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace ArbitrageScanner.Interfaces
{
    public interface IDBService
    {
        /// <summary>
        /// Экспортирует пакет строк данных в указанную таблицу ClickHouse.
        /// </summary>
        /// <param name="data">Подготовленная таблица данных для сохранения.</param>
        /// <param name="tableName">Целевая таблица с указанием схемы.</param>
        /// <param name="token">Токен отмены для остановки экспорта.</param>
        /// <returns><c>true</c> при успешном экспорте, иначе <c>false</c>.</returns>
        Task<bool> Export(DataTable data, string tableName, CancellationToken token);
        /// <summary>
        /// Удаляет отдельную запись сделки из указанной таблицы.
        /// </summary>
        /// <param name="trade">Экземпляр сделки для удаления.</param>
        /// <param name="tableName">Целевая таблица с указанием схемы.</param>
        /// <returns><c>true</c> при успешном удалении, иначе <c>false</c>.</returns>
        Task<bool> RemoveItem(ProfitTrade trade, string tableName);
        /// <summary>
        /// Очищает все таблицы ClickHouse, которые обслуживает сервис.
        /// </summary>
        /// <returns><c>true</c> при успешном выполнении команд очистки.</returns>
        Task<bool> TruncateTables();
        /// <summary>
        /// Получает прибыльные сделки, отфильтрованные по объёму, диапазону прибыли, белому списку бирж и свежести данных.
        /// </summary>
        /// <param name="minVolume">Минимальный объём в валюте котировки для сделки.</param>
        /// <param name="prcProfitRange">Включительный диапазон процентной прибыли.</param>
        /// <param name="whitelist">Разрешённые биржи; пустой массив означает любую биржу.</param>
        /// <param name="lagUpdated">Максимальное допустимое количество секунд с момента последнего обновления.</param>
        /// <returns>Массив кандидатов прибыльных сделок.</returns>
        Task<ProfitTrade[]> GetProfitTrades(long minVolume, (int minValue, int maxValue) prcProfitRange, string[] whitelist, int lagUpdated);
    }
}
