using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Data.PostgreSQL
{
    public class PostgreSQLBulkInsertOptions : IBulkInsertOptions
    {
        /// <summary>
        ///  Whether wrap field and table name with quotes
        ///  Default is true
        /// </summary>
        public bool WrapWithQuotes { get; set; } = true;
    }
}
