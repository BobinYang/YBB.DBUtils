using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace YBB.DBUtils
{
    /// <summary>
    ///
    /// </summary>
    public class SqlHelper : AdoNetHelper
    {
        #region ���캯��

        static SqlHelper()
        {
        }



        public SqlHelper(string connectionString, DbProviderType provider)
            : base(connectionString, provider)
        {
        }

        private SqlHelper()
        {
        }

        #endregion ���캯��

        #region AdoNetHelper ��Ա

        /// <summary>
        /// <see cref="OracleCommandBuilder.DeriveParameters"/>
        /// </summary>
        /// <param name="cmd"></param>
        public override void DeriveParameters(System.Data.IDbCommand cmd)
        {
            if (cmd is SqlCommand)
            {
                SqlCommandBuilder.DeriveParameters(cmd as SqlCommand);
            }
        }

        /// <summary>
        ///  return OracleClientFactory.Instance;
        /// </summary>
        //public override DbProviderFactory DbFactory
        //{
        //    get
        //    {
        //        return SqlClientFactory.Instance;
        //    }
        //}

        /// <summary>
        /// ����<see cref="OracleParameter"/>����
        /// </summary>
        /// <param name="ParamName"></param>
        /// <param name="Dbtype"></param>
        /// <param name="Size"></param>
        /// <returns></returns>
        public override DbParameter MakeParam(string ParamName, System.Data.DbType Dbtype, int Size)
        {
            if (Size > 0)
            {
                return new SqlParameter(ParamName, DbTypeConvert(Dbtype), Size);
            }
            return new SqlParameter(ParamName, DbTypeConvert(Dbtype));
        }

        /// <summary>
        /// ��DbTypeת����OracleTypeö�٣�ȷ��ö������֮��ת���İ�ȫ��
        /// ͬʱ�������ݿ��ֶ�����ͳһ�޸Ĵ����ľ޴��޸���
        /// http://msdn.microsoft.com/en-us/library/yk72thhd%28VS.80%29.aspx
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private SqlDbType DbTypeConvert(DbType type)
        {
            SqlDbType oracleType = SqlDbType.VarChar;
            string exception = String.Format("No mapping exists from DbType {0} to a known OracleType.", type);
            switch (type)
            {
                case DbType.AnsiString:
                    oracleType = SqlDbType.VarChar;
                    break;

                case DbType.AnsiStringFixedLength:
                    oracleType = SqlDbType.Char;
                    break;

                case DbType.Binary:
                case DbType.Guid:
                    oracleType = SqlDbType.UniqueIdentifier;
                    break;

                case DbType.Boolean:
                case DbType.Byte:
                    oracleType = SqlDbType.TinyInt;
                    break;

                case DbType.Currency:
                case DbType.Decimal:
                case DbType.Int64:
                    oracleType = SqlDbType.BigInt;
                    break;

                case DbType.Date:
                case DbType.DateTime:
                case DbType.Time:
                    oracleType = SqlDbType.DateTime;
                    break;

                case DbType.Double:
                    oracleType = SqlDbType.Float;
                    break;

                case DbType.Int16:
                    oracleType = SqlDbType.SmallInt;
                    break;

                case DbType.Int32:
                    oracleType = SqlDbType.Int;
                    break;

                case DbType.Object:
                    oracleType = SqlDbType.Image;
                    break;

                case DbType.Single:
                    oracleType = SqlDbType.Float;
                    break;

                case DbType.String:
                    oracleType = SqlDbType.NVarChar;
                    break;

                case DbType.StringFixedLength:
                    oracleType = SqlDbType.NChar;
                    break;

                default:
                    throw new ArgumentException(exception);
            }
            return oracleType;
        }

        #endregion AdoNetHelper ��Ա
    }
}