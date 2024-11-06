using Sixnet.Development.Data;
using Sixnet.Development.Data.Database;

namespace Sixnet.Database.PostgreSQL
{
    /// <summary>
    /// Defines postgresql bulk insertion options
    /// </summary>
    public class PostgreSqlBulkInsertionOptions : ISixnetBulkInsertionOptions
    {
        /// <summary>
        /// Gets or sets the data operation options
        /// </summary>
        public SixnetDataOperationOptions DataOperationOptions { get; set; }

        /// <summary>
        ///  Indicates whether wrap field and table name with quotes
        ///  Default is true
        /// </summary>
        public bool WrapWithQuotes { get; set; } = true;
    }
}
