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
        /// Gets current database server type
        /// </summary>
        internal const DatabaseServerType CurrentDatabaseServerType = DatabaseServerType.PostgreSQL;

        /// <summary>
        /// Key word prefix
        /// </summary>
        internal const string KeywordPrefix = "\"";

        /// <summary>
        /// Key word suffix
        /// </summary>
        internal const string KeywordSuffix = "\"";

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
        public static IDbConnection GetConnection(DatabaseServer server)
        {
            return DataManager.GetDatabaseConnection(server) ?? new NpgsqlConnection(server.ConnectionString);
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

        #region Wrap keyword

        /// <summary>
        /// Wrap keyword by the KeywordPrefix and the KeywordSuffix
        /// </summary>
        /// <param name="originalValue">Original value</param>
        /// <returns></returns>
        internal static string WrapKeyword(string originalValue)
        {
            return $"{KeywordPrefix}{originalValue}{KeywordSuffix}";
        }

        #endregion
    }
}
