using System;
using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;

namespace YBB.DBUtils
{
    /// <summary>
    ///
    /// </summary>
    public class OracleHelper_ManagedODP : AdoNetHelper
    {
        static OracleHelper_ManagedODP()
        {
        }



        public OracleHelper_ManagedODP(string connectionString, DbProviderType provider)
            : base(connectionString, provider)
        {
        }

        private OracleHelper_ManagedODP()
        {
        }

        /// <summary>
        /// <see cref="OracleCommandBuilder.DeriveParameters"/>
        /// </summary>
        /// <param name="cmd"></param>
        public override void DeriveParameters(System.Data.IDbCommand cmd)
        {
            if (cmd is OracleCommand)
            {
                OracleCommandBuilder.DeriveParameters(cmd as OracleCommand);
            }
        }

        /// <summary>
        ///  return OracleClientFactory.Instance;
        /// </summary>
        //public override DbProviderFactory DbFactory
        //{
        //    get
        //    {
        //        return OracleClientFactory.Instance;
        //    }
        //}

        /// <summary>
        /// 构造<see cref="OracleParameter"/>对象
        /// </summary>
        /// <param name="ParamName"></param>
        /// <param name="Dbtype"></param>
        /// <param name="Size"></param>
        /// <returns></returns>
        public override DbParameter MakeParam(string ParamName, System.Data.DbType Dbtype, int Size)
        {
            if (Size > 0)
            {
                return new OracleParameter(ParamName, DbTypeConvert(Dbtype), Size);
            }
            return new OracleParameter(ParamName, DbTypeConvert(Dbtype));
        }

        /// <summary>
        /// 将DbType转换成OracleType枚举，确保枚举类型之间转换的安全性
        /// 同时避免数据库字段类型统一修改带来的巨大修改量
        /// http://msdn.microsoft.com/en-us/library/yk72thhd%28VS.80%29.aspx
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private OracleDbType DbTypeConvert(DbType type)
        {
            OracleDbType oracleType = OracleDbType.Varchar2;
            string exception = String.Format("No mapping exists from DbType {0} to a known OracleType.", type);
            switch (type)
            {
                case DbType.AnsiString:
                    oracleType = OracleDbType.Varchar2;
                    break;

                case DbType.AnsiStringFixedLength:
                    oracleType = OracleDbType.Char;
                    break;

                case DbType.String:
                    oracleType = OracleDbType.Varchar2;
                    break;

                case DbType.StringFixedLength:
                    oracleType = OracleDbType.Char;
                    break;

                case DbType.Binary:
                case DbType.Guid:
                    oracleType = OracleDbType.Raw;
                    break;

                case DbType.Boolean:
                case DbType.Byte:
                    oracleType = OracleDbType.Byte;
                    break;

                case DbType.Date:
                case DbType.DateTime:
                case DbType.Time:
                    oracleType = OracleDbType.Date;
                    break;

                case DbType.Int16:
                    oracleType = OracleDbType.Int16;
                    break;

                case DbType.Int32:
                    oracleType = OracleDbType.Int32;
                    break;

                case DbType.Int64:
                    oracleType = OracleDbType.Int64;
                    break;

                case DbType.Single:
                case DbType.Currency:
                    oracleType = OracleDbType.Single;
                    break;

                case DbType.Decimal:
                    oracleType = OracleDbType.Decimal;
                    break;

                case DbType.Double:
                    oracleType = OracleDbType.Double;
                    break;

                case DbType.Object:
                    oracleType = OracleDbType.Blob;
                    break;

                default:
                    throw new ArgumentException(exception);
            }
            return oracleType;
        }
    }
}