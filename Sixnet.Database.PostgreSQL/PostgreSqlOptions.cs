using System;
using System.Collections.Generic;
using System.Text;

namespace Sixnet.Database.PostgreSQL
{
    public class PostgreSqlOptions
    {
        /// <summary>
        /// Whether enable legacy timestamp behavior
        /// </summary>
       public bool EnableLegacyTimestampBehavior { get; set; }

        /// <summary>
        /// Whether disable datetime infinity conversions
        /// </summary>
       public bool DisableDateTimeInfinityConversions { get; set; }
    }
}
