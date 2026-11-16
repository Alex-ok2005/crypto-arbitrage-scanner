using ArbitrageScanner.Interfaces;
using ArbitrageScanner.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NLog;
using System.Threading.Tasks;
namespace ArbitrageScanner
{
    /// <summary>
    /// Конфигурирует зависимости приложения и запускает фоновые сервисы сканера.
    /// </summary>
    internal class Program
    {
        /// <summary>
        /// Точка входа приложения, создающая хост, подключающая сервисы и запускающая фоновые задания.
        /// </summary>
        /// <param name="args">Аргументы командной строки, переданные билдеру хоста.</param>
        static async Task Main(string[] args)
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    var logger = LogManager.Setup()
                        .LoadConfigurationFromFile("nlog.config") 
                        .GetCurrentClassLogger();
                    services.AddSingleton(logger);

                    services.AddSingleton<IExchangeService, ExchangeService>();
                    services.AddSingleton<IDBService, ClickhouseDBService>();
                    services.AddSingleton<CollectorService>();

                    // Добавление фоновых сервисов
                    services.AddHostedService(provider => provider.GetRequiredService<CollectorService>());
                    services.AddHostedService<VerificationService>();
                })
                .Build();

            await host.RunAsync(); // Запуск хоста
        }
    }
}
