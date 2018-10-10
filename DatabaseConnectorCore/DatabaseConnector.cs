using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace LyeltDatabaseConnector
{
    /// <summary>
    /// Conveniently wraps the capabilities of a SqlConnection, handling its opening and closing, and facilitating retrieval of SqlCommand objects.
    /// </summary>
    public class DatabaseConnector : IDisposable
    {
        private SqlConnectionStringBuilder _connBuilder;
        
        /// <summary>
        /// The SqlConnection object associated with the current <see cref="DatabaseConnector"/> instance. 
        /// </summary>
        public SqlConnection Connection { get; }

        /// <summary>
        /// Construct a DatabaseConnector object with the given connection string. 
        /// Use the public <see cref="DatabaseHelper.GetConnector"/> and <see cref="DatabaseHelper.GetConnector(string)"/> to obtain an object.
        /// </summary>
        /// <param name="connectionString">Database connection string</param>
        /// <exception cref="InvalidOperationException">Cconnection failed to open</exception>
        /// <exception cref="SqlException">Connection failed to open</exception>
        /// <exception cref="System.Configuration.ConfigurationException">Configuration error</exception>
        internal DatabaseConnector(string connectionString)
        {
            try
            {
                _connBuilder = new SqlConnectionStringBuilder(connectionString);
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"The following connection string is invalid: {connectionString}", ex);
            }

            Connection = new SqlConnection(_connBuilder.ConnectionString);
            Connection.Open();
        }

        /// <summary>
        /// Build a SqlCommand with the given text and arguments
        /// </summary>
        /// <param name="commandText">Text of the sql command</param>
        /// <param name="args">Any parameters associated with the command</param>
        /// <returns>SqlCommand with appropriate type, connection, and parameters</returns>
        public SqlCommand BuildTextCommand(string commandText, params object[] args)
        {
            ThrowIfInvalidCommand(commandText);
            ThrowIfInvalidParameters(args);
            return BuildCommand(commandText, CommandType.Text, args);
        }

        /// <summary>
        /// Build a SqlCommand with the given text and arguments, with a type of StoredProcedure
        /// </summary>
        /// <param name="commandText">Text of the stored procedure command</param>
        /// <param name="args">Any parameters associated with the command</param>
        /// <returns>SqlCommand with appropriate type, connection, and parameters</returns>
        public SqlCommand BuildStoredProcedureCommand(string commandText, params object[] args)
        {
            ThrowIfInvalidCommand(commandText);
            ThrowIfInvalidParameters(args);
            return BuildCommand(commandText, CommandType.StoredProcedure, args);
        }

        // Build the command and add the parameters
        private SqlCommand BuildCommand(string commandText, CommandType type, params object[] args)
        {
            var cmd = new SqlCommand(commandText, Connection);
            cmd.CommandType = type;

            for (int i = 0; i < args.Count(); i += 2)
            {
                cmd.Parameters.AddWithValue(args[i].ToString(), args[i + 1]);
            }

            return cmd;
        }

        // Test the list of parameters to ensure that they are valid
        private void ThrowIfInvalidParameters(params object[] args)
        {
            var argsList = args.ToList(); // Enumerate once

            if (argsList == null || argsList.Count == 0)
                return;

            if (argsList.Count % 2 != 0)
                throw new ArgumentException("Sql command arguments must have a matching name/value pair.", nameof(args));

            if (argsList
                .Where((x, i) => i % 2 == 0)    // All even parameters are the names
                .Any(a => string.IsNullOrWhiteSpace(a.ToString())))
                throw new ArgumentNullException(nameof(args), "Sql command argument name(s) cannot be null or empty.");
        }

        // Ensure we have valid command text
        private void ThrowIfInvalidCommand(string commandText)
        {
            if (string.IsNullOrWhiteSpace(commandText))
                throw new ArgumentNullException(commandText, "Sql command text cannot be null or empty.");
        }

        #region IDisposable
        public void Dispose()
        {
            Connection?.Close();
            Connection?.Dispose();
        }
        #endregion
    }
}
