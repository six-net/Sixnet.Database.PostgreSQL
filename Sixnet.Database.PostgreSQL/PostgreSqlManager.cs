using System.Data;
using Npgsql;
using Sixnet.Development.Data;
using Sixnet.Development.Data.Database;

namespace Sixnet.Database.PostgreSQL
{
    /// <summary>
    /// Database postgresql manager
    /// </summary>
    internal static class PostgreSqlManager
    {
        #region Fields

        /// <summary>
        /// Default query translator
        /// </summary>
        static readonly PostgreSqlDataCommandResolver DefaultResolver = new PostgreSqlDataCommandResolver();

        #endregion

        #region Get database connection

        /// <summary>
        /// Get database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns>Return database connection</returns>
        public static IDbConnection GetConnection(SixnetDatabaseServer server)
        {
            return SixnetDataManager.GetDatabaseConnection(server) ?? new NpgsqlConnection(SixnetDataManager.ResolveConnectionString(server));
        }

        #endregion

        #region Get command resolver

        /// <summary>
        /// Get command resolver
        /// </summary>
        /// <returns>Return a command resolver</returns>
        internal static PostgreSqlDataCommandResolver GetCommandResolver()
        {
            return DefaultResolver;
        }

        #endregion
    }
}
