using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace LyeltDatabaseConnector
{
    public static class DatabaseHelper
    {
        /// <summary>
        /// Default database connection string to use in all subsequent calls to <see cref="GetConnector"/>
        /// </summary>
        public static string DefaultConnectionString { get; set; }

        /// <summary>
        /// Return a <see cref="DatabaseConnector"/> using the provided <see cref="DefaultConnectionString"/>
        /// </summary>
        /// <returns>A <see cref="DatabaseConnector"/> object with an open SqlConnection</returns>
        /// <exception cref="InvalidOperationException">Default connection string not provided or connection failed to open</exception>
        /// <exception cref="SqlException">Connection failed to open</exception>
        /// <exception cref="System.Configuration.ConfigurationException">Configuration error</exception>
        public static DatabaseConnector GetConnector()
        {
            if (DefaultConnectionString == null)
            {
                throw new InvalidOperationException($"If you do not specify a connection string, you must set the {nameof(DefaultConnectionString)} property before requesting a database connector.");
            }

            return new DatabaseConnector(DefaultConnectionString);
        }

        /// <summary>
        /// Return a <see cref="DatabaseConnector"/>
        /// </summary>
        /// <param name="connectionString">The database connection string to use in the Sql Connection</param>
        /// <returns>A <see cref="DatabaseConnector"/> object with an open SqlConnection</returns>
        /// <exception cref="InvalidOperationException">Connection failed to open</exception>
        /// <exception cref="SqlException">Connection failed to open</exception>
        /// <exception cref="System.Configuration.ConfigurationException">Configuration error</exception>
        public static DatabaseConnector GetConnector(string connectionString)
        {
            return new DatabaseConnector(connectionString);
        }

        /// <summary>
        /// Gets the value of the specified column as a string
        /// </summary>
        /// <param name="reader">Sql data reader</param>
        /// <param name="field">Name of the databse column field to retrieve</param>
        /// <returns>String representation of the field</returns>
        /// <exception cref="InvalidCastException">The field could not be represented in the requested way</exception>
        /// <exception cref="IndexOutOfRangeException">Invalid field name</exception>
        /// <exception cref="System.Data.SqlTypes.SqlNullValueException">The field was null</exception>
        public static string GetString(this SqlDataReader reader, string field)
        {
            if (reader.IsDBNull(reader.GetOrdinal(field)))
                return default(string);

            return reader.GetString(reader.GetOrdinal(field));
        }

        /// <summary>
        /// Gets the value of the specified column as a decimal
        /// </summary>
        /// <param name="reader">Sql data reader</param>
        /// <param name="field">Name of the databse column field to retrieve</param>
        /// <returns>Decimal representation of the field</returns>
        /// <exception cref="InvalidCastException">The field could not be represented in the requested way</exception>
        /// <exception cref="IndexOutOfRangeException">Invalid field name</exception>
        /// <exception cref="System.Data.SqlTypes.SqlNullValueException">The field was null</exception>
        public static decimal GetDecimal(this SqlDataReader reader, string field)
        {
            if (reader.IsDBNull(reader.GetOrdinal(field)))
                return default(decimal);

            return reader.GetDecimal(reader.GetOrdinal(field));
        }

        /// <summary>
        /// Gets the value of the specified column as a double
        /// </summary>
        /// <param name="reader">Sql data reader</param>
        /// <param name="field">Name of the databse column field to retrieve</param>
        /// <returns>Double representation of the field</returns>
        /// <exception cref="InvalidCastException">The field could not be represented in the requested way</exception>
        /// <exception cref="IndexOutOfRangeException">Invalid field name</exception>
        /// <exception cref="System.Data.SqlTypes.SqlNullValueException">The field was null</exception>
        public static double GetDouble(this SqlDataReader reader, string field)
        {
            if (reader.IsDBNull(reader.GetOrdinal(field)))
                return default(double);

            return reader.GetDouble(reader.GetOrdinal(field));
        }

        /// <summary>
        /// Gets the value of the specified column as a float
        /// </summary>
        /// <param name="reader">Sql data reader</param>
        /// <param name="field">Name of the databse column field to retrieve</param>
        /// <returns>Float representation of the field</returns>
        /// <exception cref="InvalidCastException">The field could not be represented in the requested way</exception>
        /// <exception cref="IndexOutOfRangeException">Invalid field name</exception>
        /// <exception cref="System.Data.SqlTypes.SqlNullValueException">The field was null</exception>
        public static float GetFloat(this SqlDataReader reader, string field)
        {
            if (reader.IsDBNull(reader.GetOrdinal(field)))
                return default(float);

            return reader.GetFloat(reader.GetOrdinal(field));
        }

        /// <summary>
        /// Gets the value of the specified column as an integer
        /// </summary>
        /// <param name="reader">Sql data reader</param>
        /// <param name="field">Name of the databse column field to retrieve</param>
        /// <returns>Int representation of the field</returns>
        /// <exception cref="InvalidCastException">The field could not be represented in the requested way</exception>
        /// <exception cref="IndexOutOfRangeException">Invalid field name</exception>
        /// <exception cref="System.Data.SqlTypes.SqlNullValueException">The field was null</exception>
        public static int GetInt(this SqlDataReader reader, string field)
        {
            if (reader.IsDBNull(reader.GetOrdinal(field)))
                return default(int);

            return reader.GetInt32(reader.GetOrdinal(field));
        }

        /// <summary>
        /// Gets the value of the specified column as a DateTime
        /// </summary>
        /// <param name="reader">Sql data reader</param>
        /// <param name="field">Name of the databse column field to retrieve</param>
        /// <returns>DateTime representation of the field</returns>
        /// <exception cref="InvalidCastException">The field could not be represented in the requested way</exception>
        /// <exception cref="IndexOutOfRangeException">Invalid field name</exception>
        /// <exception cref="System.Data.SqlTypes.SqlNullValueException">The field was null</exception>
        public static DateTime GetDateTime(this SqlDataReader reader, string field)
        {
            if (reader.IsDBNull(reader.GetOrdinal(field)))
                return default(DateTime);

            return reader.GetDateTime(reader.GetOrdinal(field));
        }

        /// <summary>
        /// Gets the value of the specified column as an object
        /// </summary>
        /// <param name="reader">Sql data reader</param>
        /// <param name="field">Name of the databse column field to retrieve</param>
        /// <returns>Object representation of the field</returns>
        /// <exception cref="IndexOutOfRangeException">Invalid field name</exception>
        /// <exception cref="System.Data.SqlTypes.SqlNullValueException">The field was null</exception>
        public static object GetObject(this SqlDataReader reader, string field)
        {
            if (reader.IsDBNull(reader.GetOrdinal(field)))
                return default(object);

            return reader.GetValue(reader.GetOrdinal(field));
        }

        /// <summary>
        /// Gets the value of the specified column as a generic type
        /// </summary>
        /// <param name="reader">Sql data reader</param>
        /// <param name="field">Name of the databse column field to retrieve</param>
        /// <returns>Generic type representation of the field</returns>
        /// <exception cref="InvalidCastException">The field could not be represented in the requested way</exception>
        /// <exception cref="IndexOutOfRangeException">Invalid field name</exception>
        /// <exception cref="InvalidOperationException"></exception>
        /// <exception cref="System.Data.SqlTypes.SqlNullValueException">The field was null</exception>
        public static T GetField<T>(this SqlDataReader reader, string field)
        {
            if (reader.IsDBNull(reader.GetOrdinal(field)))
                return default(T);

            return reader.GetFieldValue<T>(reader.GetOrdinal(field));
        }
    }
}
