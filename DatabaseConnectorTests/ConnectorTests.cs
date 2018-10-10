using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using LyeltDatabaseConnector;
using System.Data.SqlClient;
using System.Data;

namespace DatabaseConnectorTests
{
    [TestFixture]
    public class ConnectorTests
    {
        private const string CONN_STR = @"Data Source=NICK-HOME-PC;Initial Catalog=Lyelt;Integrated Security=True";
        private const string TEST_SQL_1 = @"SELECT * FROM Budgets";
        private const string TEST_SQL_1_WITH_PARAM = @"SELECT * FROM Budgets WHERE Name = @name";
        private const string TEST_SQL_2 = @"SELECT * FROM TestTable";
        private const string TEST_SQL_2_WITH_PARAM = @"SELECT * FROM TestTable WHERE StringCol = @col";
        private const string TEST_SQL_3 = @"INSERT INTO TestTable (StringCol, RealCol, IntCol, DateCol) VALUES ('aaaa', 22.33, 3, GETUTCDATE())";
        private const string TEST_SQL_3_WITH_PARAM = @"INSERT INTO TestTable (StringCol, RealCol, IntCol, DateCol) VALUES (@string, @real, @int, @date)";
        private const string TEST_INVALID_SQL = @"SELECT * FROM FakeTableThatDoesntExist";
        private const string TEST_SP = @"spTest";

        [OneTimeSetUp]
        public void Setup()
        {
            DatabaseHelper.DefaultConnectionString = CONN_STR;
        }

        /// <summary>
        /// Get the DatabaseConnector object
        /// </summary>
        [Test]
        public void TestGetConnector()
        {
            Assert.DoesNotThrow(() =>
            {
                using (var dbc = DatabaseHelper.GetConnector());
            });
        }

        /// <summary>
        /// Ensure that passing an invalid connection string throws
        /// </summary>
        [Test]
        public void TestGetConnectorThrows()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                using (var dbc = DatabaseHelper.GetConnector("INVALID"));
            });
        }

        /// <summary>
        /// Ensure that passing a null or empty query throws
        /// </summary>
        [TestCase(null)]
        [TestCase("")]
        public void TestInvalidCommandThrows(string command)
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                using (var dbc = DatabaseHelper.GetConnector())
                using (var cmd = dbc.BuildTextCommand(command));
            });
            
            Assert.Throws<ArgumentNullException>(() =>
            {
                using (var dbc = DatabaseHelper.GetConnector())
                using (var cmd = dbc.BuildStoredProcedureCommand(command));
            });
        }

        /// <summary>
        /// Ensure that mismatched parameters to the sql query throws
        /// </summary>
        [Test]
        public void TestMismatchedParamsThrows()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                using (var dbc = DatabaseHelper.GetConnector())
                using (var cmd = dbc.BuildTextCommand(TEST_SQL_1_WITH_PARAM, "@name")) ;
            });

            Assert.Throws<ArgumentException>(() =>
            {
                using (var dbc = DatabaseHelper.GetConnector())
                using (var cmd = dbc.BuildStoredProcedureCommand(TEST_SP, "@name")) ;
            });
        }

        private void AssertValidCommand(DatabaseConnector dbc, SqlCommand cmd, string expectedText, CommandType expectedType)
        {
            Assert.IsNotNull(cmd);
            Assert.That(cmd.Connection.ClientConnectionId == dbc.Connection.ClientConnectionId);
            Assert.That(cmd.CommandText == expectedText);
            Assert.That(cmd.CommandType == expectedType);
        }

        private void AssertValidReader(SqlCommand cmd, string sql)
        {
            Assert.DoesNotThrow(() =>
            {
                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.NotNull(rdr);
                    Assert.That(rdr.HasRows);

                    while (rdr.Read())
                    {

                    }
                }
            }, $"Failed to execute reader for command: {sql}");
        }

        /// <summary>
        /// Ensure that we can build a sql text query
        /// </summary>
        [Test]
        public void TestBuildTextCommand()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            using (var cmd = dbc.BuildTextCommand(TEST_SQL_1))
            {
                AssertValidCommand(dbc, cmd, TEST_SQL_1, CommandType.Text);
            }
        }

        /// <summary>
        /// Ensure that we can build a sql text query with parameters
        /// </summary>
        [Test]
        public void TestBuildTextCommandWithParam()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            using (var cmd = dbc.BuildTextCommand(TEST_SQL_1_WITH_PARAM, "@name", "Nick's Budget"))
            {
                AssertValidCommand(dbc, cmd, TEST_SQL_1_WITH_PARAM, CommandType.Text);
            }
        }

        /// <summary>
        /// Ensure that we can build a stored procedure command
        /// </summary>
        [Test]
        public void TestBuildStoredProcedureCommand()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            using (var cmd = dbc.BuildStoredProcedureCommand(TEST_SP))
            {
                AssertValidCommand(dbc, cmd, TEST_SP, CommandType.StoredProcedure);
            }
        }

        /// <summary>
        /// Ensure that we can build a stored procedure command with parameters
        /// </summary>
        [Test]
        public void TestBuildStoredProcedureCommandWithParams()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            using (var cmd = dbc.BuildStoredProcedureCommand(TEST_SP, "@name", "Nick's Budget"))
            {
                AssertValidCommand(dbc, cmd, TEST_SP, CommandType.StoredProcedure);
            }
        }

        [Test]
        public void TestGetReader()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            {
                using (var cmd = dbc.BuildTextCommand(TEST_SQL_2))
                    AssertValidReader(cmd, TEST_SQL_2);

                using (var cmd = dbc.BuildTextCommand(TEST_SQL_2_WITH_PARAM, "@col", "abcd"))
                    AssertValidReader(cmd, TEST_SQL_2_WITH_PARAM);

                Assert.Throws<SqlException>(() =>
                {
                    using (var cmd = dbc.BuildTextCommand(TEST_INVALID_SQL))
                    using (var rdr = cmd.ExecuteReader())
                    {

                    }
                });
            }
        }

        [Test]
        public void TestExecuteQuery()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            {
                using (var cmd = dbc.BuildTextCommand(TEST_SQL_3))
                {
                    Assert.DoesNotThrow(() =>
                    {
                        int affected = cmd.ExecuteNonQuery();
                        Assert.Positive(affected);
                    });
                }

                using (var cmd = dbc.BuildTextCommand(TEST_SQL_3_WITH_PARAM, "@string", "tstString", "@int", 5, "@real", 123.456, "@date", DateTime.Now))
                {
                    Assert.DoesNotThrow(() =>
                    {
                        int affected = cmd.ExecuteNonQuery();
                        Assert.Positive(affected);
                    });
                }
            }
        }

        [Test]
        public void TestGetString()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            using (var cmd = dbc.BuildTextCommand(TEST_SQL_2))
            {
                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.DoesNotThrow(() =>
                    {
                        while (rdr.Read())
                        {
                            try
                            {
                                string str = rdr.GetString("StringCol");
                            }
                            catch (System.Data.SqlTypes.SqlNullValueException) { /* DBNull */ }
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<InvalidCastException>(() =>
                    {
                        while (rdr.Read())
                        {
                            string str = rdr.GetString("RealCol");
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<IndexOutOfRangeException>(() => 
                    {
                        while (rdr.Read())
                        {
                            string str = rdr.GetString("FakeCol");
                        }
                    });
                }
            }
        }

        [Test]
        public void TestGetDouble()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            using (var cmd = dbc.BuildTextCommand(TEST_SQL_2))
            {
                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.DoesNotThrow(() =>
                    {
                        while (rdr.Read())
                        {
                            try
                            {
                                double db = rdr.GetDouble("DoubleCol");
                            }
                            catch (System.Data.SqlTypes.SqlNullValueException) { /* DBNull */ }
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<InvalidCastException>(() =>
                    {
                        while (rdr.Read())
                        {
                            double db = rdr.GetDouble("StringCol");
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<IndexOutOfRangeException>(() =>
                    {
                        while (rdr.Read())
                        {
                            double db = rdr.GetDouble("FakeCol");
                        }
                    });
                }
            }
        }

        [Test]
        public void TestGetDecimal()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            using (var cmd = dbc.BuildTextCommand(TEST_SQL_2))
            {
                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.DoesNotThrow(() =>
                    {
                        while (rdr.Read())
                        {
                            try
                            {
                                decimal dc = rdr.GetDecimal("DecimalCol");
                            }
                            catch (System.Data.SqlTypes.SqlNullValueException) { /* DBNull */ }
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<InvalidCastException>(() =>
                    {
                        while (rdr.Read())
                        {
                            decimal dc = rdr.GetDecimal("StringCol");
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<IndexOutOfRangeException>(() =>
                    {
                        while (rdr.Read())
                        {
                            decimal dc = rdr.GetDecimal("FakeCol");
                        }
                    });
                }
            }
        }

        [Test]
        public void TestGetFloat()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            using (var cmd = dbc.BuildTextCommand(TEST_SQL_2))
            {
                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.DoesNotThrow(() =>
                    {
                        while (rdr.Read())
                        {
                            try
                            {
                                float fl = rdr.GetFloat("RealCol");
                            }
                            catch (System.Data.SqlTypes.SqlNullValueException) { /* DBNull */ }
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<InvalidCastException>(() =>
                    {
                        while (rdr.Read())
                        {
                            float fl = rdr.GetFloat("StringCol");
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<IndexOutOfRangeException>(() =>
                    {
                        while (rdr.Read())
                        {
                            float fl = rdr.GetFloat("FakeCol");
                        }
                    });
                }
            }
        }

        [Test]
        public void TestGetInt()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            using (var cmd = dbc.BuildTextCommand(TEST_SQL_2))
            {
                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.DoesNotThrow(() =>
                    {
                        while (rdr.Read())
                        {
                            try
                            {
                               int nt = rdr.GetInt("IntCol");
                            }
                            catch (System.Data.SqlTypes.SqlNullValueException) { /* DBNull */ }
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<InvalidCastException>(() =>
                    {
                        while (rdr.Read())
                        {
                            int nt = rdr.GetInt("StringCol");
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<IndexOutOfRangeException>(() =>
                    {
                        while (rdr.Read())
                        {
                            int nt = rdr.GetInt("FakeCol");
                        }
                    });
                }
            }
        }

        [Test]
        public void TestGetDateTime()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            using (var cmd = dbc.BuildTextCommand(TEST_SQL_2))
            {
                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.DoesNotThrow(() =>
                    {
                        while (rdr.Read())
                        {
                            try
                            {
                                DateTime dt = rdr.GetDateTime("DateCol");
                            }
                            catch (System.Data.SqlTypes.SqlNullValueException) { /* DBNull */ }
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<InvalidCastException>(() =>
                    {
                        while (rdr.Read())
                        {
                            DateTime dt = rdr.GetDateTime("StringCol");
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<IndexOutOfRangeException>(() =>
                    {
                        while (rdr.Read())
                        {
                            DateTime dt = rdr.GetDateTime("FakeCol");
                        }
                    });
                }
            }
        }

        [Test]
        public void TestGetObject()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            using (var cmd = dbc.BuildTextCommand(TEST_SQL_2))
            {
                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.DoesNotThrow(() =>
                    {
                        while (rdr.Read())
                        {
                            try
                            {
                                object ob = rdr.GetObject("StringCol");
                            }
                            catch (System.Data.SqlTypes.SqlNullValueException) { /* DBNull */ }
                        }
                    });
                }

                using (var rdr = cmd.ExecuteReader())
                {
                    Assert.Throws<IndexOutOfRangeException>(() =>
                    {
                        while (rdr.Read())
                        {
                            object fl = rdr.GetObject("FakeCol");
                        }
                    });
                }
            }
        }

        [Test]
        public void TestGetGeneric()
        {
            using (var dbc = DatabaseHelper.GetConnector())
            using (var cmd = dbc.BuildTextCommand(TEST_SQL_2))
            using (var rdr = cmd.ExecuteReader())
            {
                Assert.DoesNotThrow(() =>
                {
                    while (rdr.Read())
                    {
                        try
                        {
                            string st = rdr.GetField<string>("StringCol");
                            int nt = rdr.GetField<int>("IntCol");
                            decimal dc = rdr.GetField<decimal>("DecimalCol");
                            double db = rdr.GetField<double>("DoubleCol");
                            float fl = rdr.GetField<float>("RealCol");
                            DateTime dt = rdr.GetField<DateTime>("DateCol");
                        }
                        catch (System.Data.SqlTypes.SqlNullValueException) { /* DBNull */ }
                    }
                });
            }
        }
    }
}
