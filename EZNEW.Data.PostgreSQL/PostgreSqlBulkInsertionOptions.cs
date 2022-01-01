using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Data.PostgreSQL
{
    /// <summary>
    /// Defines postgresql bulk insertion options
    /// </summary>
    public class PostgreSqlBulkInsertionOptions : IBulkInsertionOptions
    {
        /// <summary>
        ///  Indicates whether wrap field and table name with quotes
        ///  Default is true
        /// </summary>
        public bool WrapWithQuotes { get; set; } = true;
    }
}
