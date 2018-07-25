using System.Data.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using YBB.DBUtils;

namespace TestProject1
{
    /// <summary>
    ///这是 AdoNetHelperTest 的测试类，旨在
    ///包含所有 AdoNetHelperTest 单元测试
    ///</summary>
    [TestClass()]
    public class AdoNetHelperTest
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


        /// <summary>
        ///OpenNewConnection 的测试
        ///</summary>
        [TestMethod()]
        public void OpenNewConnectionTest()
        {
            AdoNetHelper target = CreateAdoNetHelper(); // TODO: 初始化为适当的值
            DbConnection expected = null; // TODO: 初始化为适当的值
            DbConnection actual;
            actual = target.OpenNewConnection();
            Assert.AreEqual(expected, actual);
            Assert.Inconclusive("验证此测试方法的正确性。");
        }

        internal virtual AdoNetHelper CreateAdoNetHelper()
        {
            // TODO: 实例化相应的具体类。
            AdoNetHelper target = AdoNetHelper.ThreadInstance("", DbProviderType.SqlServer);
            return target;
        }
    }
}