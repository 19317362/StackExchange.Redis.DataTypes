using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis.DataTypes.Samples
{
	public class MsgBufWrapper: CachingFramework.Redis.Contracts.ISerializer
	{
		private Redis.Extensions.MsgPack.MsgPackObjectSerializer serl = new Redis.Extensions.MsgPack.MsgPackObjectSerializer();

		public byte[] Serialize<T>(T value)
		{
			return serl.Serialize(value);
		}
		public T Deserialize<T>(byte[] value)
		{
			//return (T)Convert.ChangeType(Encoding.UTF8.GetString(value), typeof(T));
			return serl.Deserialize<T>(value);
		}
	}
}
