using System;

namespace YBB.DBUtils
{
    /// <summary>
    /// 方便可以dispose all，而不用理会具体
    /// </summary>
    public interface IThreadInstance : IDisposable
    {
    }
}