using StackExchange.Redis.DataTypes.Collections;

namespace StackExchange.Redis.DataTypes
{
	public interface IRedisTypeFactory
	{
		RedisDictionary<TValue> GetDictionary<TValue>(string name);
		RedisSet<T> GetSet<T>(string name);
		RedisList<T> GetList<T>(string name);
	}
}
