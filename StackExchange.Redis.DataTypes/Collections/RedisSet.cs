using StackExchange.Redis.Extensions.Core;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace StackExchange.Redis.DataTypes.Collections
{
	public class RedisSet<T> : ISet<T>, ICollection<T>
	{
		private const string RedisKeyTemplate = "Set:{0}";

		private readonly StackExchangeRedisCacheClient CacheClient;
		private readonly string redisKey;

		public RedisSet(StackExchangeRedisCacheClient cacheClient, string name)
		{
			if (cacheClient == null)
			{
				throw new ArgumentNullException("CacheClient");
			}
			if (name == null)
			{
				throw new ArgumentNullException("name");
			}

			this.CacheClient = cacheClient;
			this.redisKey = string.Format(RedisKeyTemplate, name);
		}

		public bool Add(T item)
		{
			return CacheClient.Database.SetAdd(redisKey, CacheClient.Serializer.Serialize( item) );
		}

		public long Add(IEnumerable<T> items)
		{
			if (items == null)
			{
				throw new ArgumentNullException("items");
			}

			//return CacheClient.SetAddAll<T>(redisKey, items.ToArray());
			foreach(var v in items)
			{
				Add(v);
			}
			return 0;
		}

		public void ExceptWith(IEnumerable<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			SetCombineAndStore(SetOperation.Difference, other);
		}

		public void ExceptWith(RedisSet<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			SetCombineAndStore(SetOperation.Difference, other);
		}

		public void IntersectWith(IEnumerable<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			SetCombineAndStore(SetOperation.Intersect, other);
		}

		public void IntersectWith(RedisSet<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			SetCombineAndStore(SetOperation.Intersect, other);
		}

		public bool IsProperSubsetOf(IEnumerable<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			return Count < other.Count() && IsSubsetOf(other);
		}

		public bool IsProperSubsetOf(RedisSet<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			return Count < other.Count && IsSubsetOf(other);
		}

		public bool IsProperSupersetOf(IEnumerable<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			return Count > other.Count() && IsSupersetOf(other);
		}

		public bool IsProperSupersetOf(RedisSet<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			return Count > other.Count && IsSupersetOf(other);
		}

		public bool IsSubsetOf(IEnumerable<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			return !this.Except(other).Any();
		}

		public bool IsSubsetOf(RedisSet<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			return !SetCombine(SetOperation.Difference, other).Any();
		}

		public bool IsSupersetOf(IEnumerable<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			return !other.Except(this).Any();
		}

		public bool IsSupersetOf(RedisSet<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			return !other.SetCombine(SetOperation.Difference, this).Any();
		}

		public bool Overlaps(IEnumerable<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			foreach (var item in other)
			{
				if (Contains(item))
				{
					return true;
				}
			}

			return false;
		}

		public bool Overlaps(RedisSet<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			return SetCombine(SetOperation.Intersect, other).Any();
		}

		public bool SetEquals(IEnumerable<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			return IsSubsetOf(other) && IsSupersetOf(other);
		}

		public bool SetEquals(RedisSet<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			return IsSubsetOf(other) && IsSupersetOf(other);
		}

		public void SymmetricExceptWith(IEnumerable<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			var otherSet = new RedisSet<T>(CacheClient, Guid.NewGuid().ToString());
			try
			{
				otherSet.Add(other);
				SymmetricExceptWith(otherSet);
			}
			finally
			{
				otherSet.Clear();
			}
		}

		public void SymmetricExceptWith(RedisSet<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			var intersectedSet = new RedisSet<T>(CacheClient, Guid.NewGuid().ToString());
			try
			{
				SetCombineAndStore(SetOperation.Intersect, intersectedSet, this, other);
				SetCombineAndStore(SetOperation.Union, other);
				SetCombineAndStore(SetOperation.Difference, intersectedSet);
			}
			finally
			{
				intersectedSet.Clear();
			}
		}

		public void UnionWith(IEnumerable<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			SetCombineAndStore(SetOperation.Union, other);
		}

		public void UnionWith(RedisSet<T> other)
		{
			if (other == null)
			{
				throw new ArgumentNullException("other");
			}

			SetCombineAndStore(SetOperation.Union, other);
		}

		void ICollection<T>.Add(T item)
		{
			Add(item);
		}

		public void Clear()
		{
			CacheClient.Database.KeyDelete(redisKey);
		}

		public bool Contains(T item)
		{
			return CacheClient.Database.SetContains(redisKey, CacheClient.Serializer.Serialize( item));
		}

		void ICollection<T>.CopyTo(T[] array, int index)
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
				long count = CacheClient.Database.SetLength(redisKey);
				if (count > int.MaxValue)
				{
					throw new OverflowException("Count exceeds maximum value of integer.");
				}
				return (int)count;
			}
		}

		bool ICollection<T>.IsReadOnly
		{
			get
			{
				return false;
			}
		}

		public bool Remove(T item)
		{
			return CacheClient.Database.SetRemove(redisKey, CacheClient.Serializer.Serialize( item));
		}

		public IEnumerator<T> GetEnumerator()
		{
			return CacheClient.SetMembers<T>(redisKey).GetEnumerator();

		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		private void SetCombineAndStore(SetOperation operation, IEnumerable<T> other)
		{
			var redisTempSet = new RedisSet<T>(CacheClient, Guid.NewGuid().ToString());
			try
			{
				redisTempSet.Add(other);
				SetCombineAndStore(operation, redisTempSet);
			}
			finally
			{
				redisTempSet.Clear();
			}
		}

		private void SetCombineAndStore(SetOperation operation, RedisSet<T> other)
		{
			SetCombineAndStore(operation, this, this, other);
		}

		private void SetCombineAndStore(SetOperation operation, RedisSet<T> destination, RedisSet<T> first, RedisSet<T> second)
		{
			CacheClient.Database.SetCombineAndStore(operation, destination.redisKey, first.redisKey, second.redisKey);
		}

		private RedisValue[] SetCombine(SetOperation operation, RedisSet<T> other)
		{
			return CacheClient.Database.SetCombine(operation, redisKey, other.redisKey);
		}
	}
}
