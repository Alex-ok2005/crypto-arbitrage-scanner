namespace ArbitrageScanner.Models
{
    /// <summary>
    /// Описывает правила формирования ссылки на спотовый рынок биржи.
    /// </summary>
    public class ExchangeSpotUrlSettings
    {
        /// <summary>
        /// Шаблон URL для страницы спота с подстановкой базовой и котируемой валюты.
        /// </summary>
        public string? Template { get; init; }

        /// <summary>
        /// Текстовое описание правила подстановки для шаблона.
        /// </summary>
        public string? Rule { get; init; }

        /// <summary>
        /// Резервный URL сайта биржи (например, для региона Россия).
        /// </summary>
        public string? FallbackUrl { get; init; }

        /// <summary>
        /// Создаёт набор настроек URL для спотового рынка.
        /// </summary>
        /// <param name="template">Шаблон URL с плейсхолдерами базовой и котируемой валюты.</param>
        /// <param name="rule">Описание правила подстановки значений.</param>
        /// <param name="fallbackUrl">Ссылка на сайт биржи для использования при отсутствии шаблона.</param>
        public ExchangeSpotUrlSettings(string? template, string? rule, string? fallbackUrl)
        {
            Template = template;
            Rule = rule;
            FallbackUrl = fallbackUrl;
        }
    }
}
