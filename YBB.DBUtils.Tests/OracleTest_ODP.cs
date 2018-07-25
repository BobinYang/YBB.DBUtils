using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Data;
using System.Data.Common;
using YBB.DBUtils;

namespace YBB.DBUtils.Tests
{
    [TestClass]
    public class OracleTest_ODP
    {
        [TestMethod]
        public void TestOracle()
        {
            AdoNetHelper helper = null;
            DbConnection conn = null;
            DbConnection conn2 = null;
            using (helper = AdoNetHelper.ThreadInstance("data source=10.14.0.*:1521/orcl;user id=*;password=*;", DbProviderType.Oracle_ManagedODP))
            {
                //������ȡ��OdbcHelper��������ӡ�
                conn = helper.OpenNewConnection();
                Assert.AreEqual(conn.State, ConnectionState.Open);
                //����һ��������
                conn2 = helper.OpenNewConnection();
                Assert.AreEqual(conn2.State, ConnectionState.Open);
                Assert.AreNotEqual(conn, conn2);
                //����ThreadInstance
                using (AdoNetHelper helper2 = AdoNetHelper.ThreadInstance("data source=10.14.0.*:1521/orcl;user id=*;password=*;", DbProviderType.Oracle_ManagedODP))
                {
                    Assert.AreEqual(helper, helper2);
                }
                //����trans
                conn = helper.OpenNewConnection();
                DbTransaction trans = helper.BeginNewTrans(conn);
                string sql = "update  **";
                helper.ExecuteNonQuery(trans, CommandType.Text, sql);
                trans.Commit();
            }
            //���������ܷ������رա�
            Assert.AreEqual(conn.State, ConnectionState.Closed);
            Assert.AreEqual(conn2.State, ConnectionState.Closed);
        }
    }
}