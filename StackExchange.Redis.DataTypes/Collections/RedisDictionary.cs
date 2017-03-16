﻿using StackExchange.Redis.Extensions.Core;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace StackExchange.Redis.DataTypes.Collections
{
	public class RedisDictionary<TKey, TValue> : IDictionary<TKey, TValue>, ICollection<KeyValuePair<TKey, TValue>>
	{
		private const string RedisKeyTemplate = "Dictionary:{0}";

		private static Exception KeyNotFoundException = new KeyNotFoundException("The given key was not present in the dictionary.");
		private static Exception KeyNullException = new ArgumentNullException("key", "Value cannot be null");
		private static Exception KeyAlreadyExistsException = new ArgumentException("An item with the same key has already been added.");

		//private readonly IDatabase CacheClient;
		private readonly string redisKey;
		private readonly StackExchangeRedisCacheClient CacheClient;

		public RedisDictionary(StackExchangeRedisCacheClient CacheClient, string name)
		{
			if (CacheClient == null)
			{
				throw new ArgumentNullException("CacheClient");
			}
			if (name == null)
			{
				throw new ArgumentNullException("name");
			}

			this.CacheClient = CacheClient;
			this.redisKey = string.Format(RedisKeyTemplate, name);
		}

		public void Add(TKey key, TValue value)
		{
			if (ContainsKey(key))
			{
				throw KeyAlreadyExistsException;
			}

			Set(key, value);
		}

		public bool TryAdd(TKey key, TValue value)
		{
			return Set(key, value);
		}

		public bool ContainsKey(TKey key)
		{
			if (IsKeyNull(key))
			{
				throw KeyNullException;
			}

			return CacheClient.HashExists(redisKey, key.ToRedisValue());
		}

		public ICollection<TKey> Keys
		{
			get
			{
				return CacheClient.HashKeys(redisKey).Select(key => key.To<TKey>()).ToList();
			}
		}

		public bool Remove(TKey key)
		{
			if (IsKeyNull(key))
			{
				throw KeyNullException;
			}

			return CacheClient.HashDelete(redisKey, key.ToRedisValue());
		}

		public bool TryGetValue(TKey key, out TValue value)
		{
			if (IsKeyNull(key))
			{
				throw KeyNullException;
			}

			//			value = default(TValue);
			// 			var redisValue = CacheClient.HashGet(redisKey, key.ToRedisValue());
			// 			if (redisValue.IsNullOrEmpty)
			// 			{
			// 				return false;
			// 			}
			//value = redisValue.To<TValue>();
			value = CacheClient.HashGet<TValue>(redisKey, key.ToRedisValue());
			if (value == null)
			{
				return false;
			}
			return true;
		}

		public ICollection<TValue> Values
		{
			get
			{
				return CacheClient.HashValues<TValue>(redisKey).ToList();
			}
		}

		public TValue this[TKey key]
		{
			get
			{
				TValue value;
				if (!TryGetValue(key, out value))
				{
					throw KeyNotFoundException;
				}
				return value;
			}
			set
			{
				Set(key, value);
			}
		}

		void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
		{
			Add(item.Key, item.Value);
		}

		public void Clear()
		{
			CacheClient.Remove(redisKey);
		}

		bool ICollection<KeyValuePair<TKey, TValue>>.Contains(KeyValuePair<TKey, TValue> item)
		{
			TValue value;
			bool keyExists = TryGetValue(item.Key, out value);
			return keyExists && object.Equals(item.Value, value);
		}

		void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int index)
		{
			if (array == null)
			{
				throw new ArgumentNullException("array");
			}
			if (index < 0 || index > array.Length)
			{
				throw new ArgumentOutOfRangeException("index");
			}
			if (array.Length - index < this.Count)
			{
				throw new ArgumentException("Destination array is not long enough to copy all the items in the collection. Check array index and length.");
			}

			foreach (var item in this)
			{
				array[index++] = item;
			}
		}

		public int Count
		{
			get
			{
				long count = CacheClient.HashLength(redisKey);
				if (count > int.MaxValue)
				{
					throw new OverflowException("Count exceeds maximum value of integer.");
				}
				return (int)count;
			}
		}

		bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly
		{
			get
			{
				return false;
			}
		}

		bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> item)
		{
			return Remove(item.Key);
		}

		public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
		{
// 			return CacheClient
// 						.HashScan(redisKey)
// 						.Select(he => new KeyValuePair<TKey, TValue>(he.Name.To<TKey>(), he.Value.To<TValue>()))
// 						.GetEnumerator();
			return CacheClient
						.HashGetAll<TKey>(redisKey)
						.Select(he =>
						new KeyValuePair<TKey, TValue>(he.Key.To<TKey>() , he.Value))
						.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		private bool Set(TKey key, TValue value)
		{
			//CacheClient.
			return CacheClient.HashSet(redisKey, key.ToRedisValue(), value.ToRedisValue());
		}

		private bool IsKeyNull(TKey key)
		{
			return !typeof(TKey).IsValueType && key == null;
		}
	}
}
