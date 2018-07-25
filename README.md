# YBB.DBUtils
数据库连接工具，兼容ADO、ADO.NET方式访问，支持Oracle（不需要安装客户端）、SQL Server、OleDb和ODBC等数据库。

//用法：
// 1、初始化线程缓存AdoNetHelper 类：

            public class ProviderBase
            {
                protected static AdoNetHelper Db;
 
                static ProviderBase()
                {
                    DbIMS = AdoNetHelper.ThreadInstance("data source=*:1521/orcl;user id=*;password=*;", DbProviderType.Oracle_ManagedODP);
                }
            }
 
//2、打开连接和创建命令，执行读数据

            string  sql = "select 1 from  **";
            DbConnection conn = null;
            try
            {
                conn  = Db.OpenNewConnection();
                DbCommand command = Db.CreateNewCommand(sql, conn);
                DbDataReader  dr = command.ExecuteReader(); //   Convert.ToInt32(command.ExecuteScalar()); //   DataTable dt1 = Db.ExecuteDataset(command);
                if (rd.HasRows)
                {
                    dr.Close();
                    return true;
                }
                // while (dr.Read())
                //{
                //       List1.Add(dr[0].ToString().Trim());
                //  }
                dr.Close();
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                Db.CloseConnection(conn);
            }

//3、带参数执行命令

            sql = "insert into table1 (column1,column2) values (:a,:b)";//sql server为@符号
            DbConnection conn = Db.Conn;
            using (DbCommand command = Db.CreateNewCommand(sql, conn))
            {
                command.Parameters.Add(Db.MakeInParam(":a", **));
                command.Parameters.Add(Db.MakeInParam(":b", **));
                command.ExecuteNonQuery();
            }

//4、利用事务处理更新操作。

            int rows = 0;
            DbConnection   conn  = Db.OpenNewConnection();
            DbTransaction trans = Db.BeginNewTrans(conn);
            DbCommand command = Db.CreateNewCommand(sql, trans);
            try
            {  
                sql = "delete **";
                command.CommandText = sql;
                rows = command.ExecuteNonQuery();

                sql = "update  **";
                command.CommandText = sql;
                rows = command.ExecuteNonQuery();
             
                trans.Commit();
            }
            catch (Exception)
            {
                if (trans != null)
                {
                    trans.Rollback();
                }
                throw;
            }
            return rows;

//5、插入操作返回最新值。

            string lastId = "";
            DbTransaction trans = null;
            try
            {
                DbConnection conn = Db.Conn;
                trans = Db.BeginNewTrans(conn);

                sql = "insert intotable1 (column1,column2) values (*,*) RETURNING ID into :recid ";

                command = Db.CreateNewCommand(sql, trans);
                DbParameter paralastId = Db.MakeOutParam(":recid", DbType.String, 20);
                paralastId.Direction = ParameterDirection.ReturnValue;
                command.Parameters.Add(paralastId);

                command.ExecuteNonQuery();
                command.Parameters.Clear();
                lastId = paralastId.Value.ToString();

                trans.Commit();
            }
            catch (Exception)
            {
                if (trans != null)
                {
                    trans.Rollback();
                }
                throw;
            }

//6、处理存储过程的输入输出参数。

            string sql = "sp***";
            DbConnection conn = Db.Conn;
            command = Db.CreateNewCommand(sql, conn);
            command.CommandType = CommandType.StoredProcedure;

            command.Parameters.Add(Db.MakeInParam("line", Line));
            command.Parameters.Add(Db.MakeInParam("model", PartNumber));

 
            DbParameter qty = Db.MakeOutParam("qty", 0);
            qty.Direction = ParameterDirection.Output;
            command.Parameters.Add(qty);

            command.ExecuteNonQuery();

            returnValue = Convert.ToInt32(((OracleDecimal)(command.Parameters["qty"].Value)).Value);
            command.Parameters.Clear();
            command.CommandType = CommandType.Text;
            
//7、类图如下：

![](https://github.com/BobinYang/YBB.DBUtils/blob/master/screenshots/ClassDiagram1.png?raw=true) 
