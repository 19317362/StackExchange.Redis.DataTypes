using StackExchange.Redis.DataTypes.Collections;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Protobuf;
using System;

namespace StackExchange.Redis.DataTypes
{
	public class RedisTypeFactory : IRedisTypeFactory
	{
		private readonly IDatabase database;
		private readonly StackExchangeRedisCacheClient CacheClient;
		public RedisTypeFactory(IConnectionMultiplexer connectionMultiplexer)
		{
			if (connectionMultiplexer == null)
			{
				throw new ArgumentNullException("connectionMultiplexer");
			}


			this.database = connectionMultiplexer.GetDatabase();

		}
		public RedisTypeFactory()
		{
			var serializer = new ProtobufSerializer();
			CacheClient = new StackExchangeRedisCacheClient(serializer);
			this.database = CacheClient.Database;
		}

		public RedisDictionary<TKey, TValue> GetDictionary<TKey, TValue>(string name)
		{
			return new RedisDictionary<TKey, TValue>(database, name);
		}

		public RedisSet<T> GetSet<T>(string name)
		{
			return new RedisSet<T>(database, name);
		}

		public RedisList<T> GetList<T>(string name)
		{
			return new RedisList<T>(database, name);
		}
	}
}
