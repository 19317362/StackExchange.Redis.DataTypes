using StackExchange.Redis.DataTypes.Collections;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Protobuf;
using StackExchange.Redis.Extensions.MsgPack;
using System;

namespace StackExchange.Redis.DataTypes
{
	public class RedisTypeFactory : IRedisTypeFactory
	{
		//private readonly IDatabase database;
		private readonly StackExchangeRedisCacheClient CacheClient;


		public RedisTypeFactory(bool msgbuf=true)
		{
			//var serializer = msgbuf ?  new ProtobufSerializer() : new MsgPackObjectSerializer();
			if(msgbuf)
			{
				CacheClient = new StackExchangeRedisCacheClient(
					 new MsgPackObjectSerializer()
					);

			}
			else
			{
				CacheClient = new StackExchangeRedisCacheClient(
						new ProtobufSerializer()
					);

			}

			//this.database = CacheClient.Database;
		}
		public TValue GetValue<TValue>(string key)
		{
			return CacheClient.Get<TValue>(key);
			//return database.StringGet(key);
		}
		public void StoreValue<TValue>(string key, TValue obj)
		{
			CacheClient.Add(key, obj);
		}
		public RedisDictionary<TValue> GetDictionary<TValue>(string name)
		{
			return new RedisDictionary<TValue>(CacheClient, name);
		}

		public RedisSet<T> GetSet<T>(string name)
		{
			return new RedisSet<T>(CacheClient, name);
		}

		public RedisList<T> GetList<T>(string name)
		{
			return new RedisList<T>(CacheClient, name);
		}
	}
}
