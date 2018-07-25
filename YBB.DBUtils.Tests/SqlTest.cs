using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using YBB.DBUtils;

namespace YBB.DBUtils.Tests
{
    [TestClass]
    public class SqlTest
    {
        [TestMethod]
        public void TestSql()
        {
            AdoNetHelper helper = null;
            DbConnection conn = null;
            DbConnection conn2 = null;
            using (helper =  AdoNetHelper.ThreadInstance("data source=10.14.0.*;user id=sa;password=*;Initial Catalog=*;", DbProviderType.SqlServer))
            {
                //创建获取由OdbcHelper管理的连接。
                conn = helper.OpenNewConnection();
                Assert.AreEqual(conn.State, ConnectionState.Open);
                //创建一个新连接
                conn2 = helper.OpenNewConnection();
                Assert.AreEqual(conn2.State, ConnectionState.Open);
                Assert.AreNotEqual(conn, conn2);
                //测试ThreadInstance
                using (AdoNetHelper helper2 = AdoNetHelper.ThreadInstance("data source=10.14.0.*;user id=sa;password=*;Initial Catalog=*;", DbProviderType.SqlServer))
                {
                    Assert.AreEqual(helper, helper2);
                }
                //测试trans
                conn = helper.OpenNewConnection();
                DbTransaction trans = helper.BeginNewTrans(conn);
                string sql = "**";
                DbCommand cmd = new SqlCommand(sql, conn as SqlConnection);
                cmd.Transaction = trans;
                //用户权限设置有问题。
                cmd.ExecuteReader();
                cmd.Transaction.Commit();
            }
            //测试连接能否正常关闭。
            Assert.AreEqual(conn.State, ConnectionState.Closed);
            Assert.AreEqual(conn2.State, ConnectionState.Closed);
        }
    }
}