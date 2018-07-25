using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using YBB.DBUtils;

namespace TestProject1
{
    [TestClass()]
    public class AdoTest
    {
        private TestContext testContextInstance;

        /// <summary>
        ///获取或设置测试上下文，上下文提供
        ///有关当前测试运行及其功能的信息。
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        [TestMethod]
        public void ConnectionTestDB()
        {
            ADODB.Connection conn = null;
            AdoHelper helper = null;
            AdoHelper helperRef = null;

            AdoHelper newHelper = null;
            try
            {
                helper = AdoHelper.ThreadInstance("DSN={2};DB={2};UID={0};PWD={1};HOST=192.6.4.*;PORT=5900", DbProviderType.Odbc);
                conn = helper.Connection();
                //是否成功连接到数据库。
                Assert.AreEqual(conn.State, 1);
                helperRef = AdoHelper.ThreadInstance("DSN={2};DB={2};UID={0};PWD={1};HOST=192.6.4.*;PORT=5900", DbProviderType.Odbc);
                //线程实例是否有效
                Assert.AreEqual(helper, helperRef);
                using (newHelper = AdoHelper.ThreadInstance("DSN={2};DB={2};UID={0};PWD={1};HOST=192.6.4.*;PORT=5900", DbProviderType.Odbc))
                {
                    //一个新的helper
                    Assert.AreNotEqual(helper, newHelper);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format("{0}\n{1}", ex.Message, ex.ToString()));
                throw;
            }
            finally
            {
                helper.Dispose();
                //是否正常关闭了数据库。
                Assert.AreEqual(conn.State, 0);
            }
        }

        [TestMethod]
        public void TestDbRunSql()
        {
            ADODB.Connection conn = null;
            AdoHelper helper = null;
            try
            {
                using (helper = AdoHelper.ThreadInstance("", DbProviderType.Odbc))
                {
                    conn = helper.Connection();
                    Assert.AreEqual(conn.State, 1);
                    string sql = " ";
                    ADODB.Recordset rs = new ADODB.Recordset();
                    rs.Open(sql, conn, ADODB.CursorTypeEnum.adOpenUnspecified, ADODB.LockTypeEnum.adLockUnspecified, -1);
                    int a = int.Parse(rs.Fields[0].Value.ToString());
                    rs.Close();
                    Assert.IsTrue((a > 0));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format("{0}\n{1}", ex.Message, ex.ToString()));
                throw;
            }
            Assert.AreEqual(conn.State, 0);
        }
    }
}