﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis.DataTypes.Collections;
using Microsoft.Practices.Unity;


namespace StackExchange.Redis.DataTypes.Samples
{
	class Program
	{
		static void Main(string[] args)
		{
			TestCachingFramework();
			// Create connectionMultiplexer. Creating connectionMultiplexer is costly so it is recommended to store and reuse it.
			//var connectionMultiplexer = ConnectionMultiplexer.Connect("localhost:8379,abortConnect=false"); // replace localhost with your redis db address

			// You can either create a RedisType by using RedisTypeFactory or by instantiating the desired type directly.
			//var redisTypeFactory = new RedisTypeFactory(connectionMultiplexer);

			var redisTypeFactory = new RedisTypeFactory();

			redisTypeFactory.StoreValue("test", new Person { ID = 1, Name = "WPS8848", Age = 20 });
			var kk = redisTypeFactory.GetValue<Person>("test");
			// Create a redis dictionary under the name of "Person".
			var redisDictionary = redisTypeFactory.GetDictionary<Person>("Person");

			// Adding items to dictionary
			redisDictionary.TryAdd("1", new Person { ID = 1, Name = "Steve", Age = 20 });
			redisDictionary.TryAdd("2", new Person { ID = 2, Name = "Mike", Age = 25 });
			redisDictionary.TryAdd("3", new Person { ID = 3, Name = "Lara", Age = 30 });

			// Iterate through dictionary
			foreach (var person in redisDictionary)
			{
				Console.WriteLine("ID: {0}, Name: {1}, Age: {2}", person.Value.ID, person.Value.Name, person.Value.Age);
			}

			Console.WriteLine();

			// Find a specific person
			var lara = redisDictionary.First(kvp => kvp.Value.Name == "Lara");
			Console.WriteLine("Lara's Age: {0}", lara.Value.Age);

			// Delete a person
			redisDictionary.Remove(lara.Key);

			// Delete the Person dictionary from redis
			redisDictionary.Clear();
			// Creating a redis list
			var redisList = redisTypeFactory.GetList<Int32>("Numbers");

			// Adding some numbers to redis list
			for (int i = 0; i < 10; i++)
			{
				redisList.Add(i);
			}

			Console.WriteLine();
			Console.WriteLine("List Members:");

			// Iterating through list
			foreach (var number in redisList)
			{
				Console.WriteLine(number);
			}

			// Delete the Numbers list from redis
			redisList.Clear();


			// Using a DI container...
			// 			var container = new UnityContainer();
			//
			// 			// Register connectionMultiplexer as a singleton instance
			// 			container.RegisterInstance<IConnectionMultiplexer>(connectionMultiplexer);
			// 			// Register RedisTypeFactory
			// 			container.RegisterType<IRedisTypeFactory, RedisTypeFactory>();
			// 			// Resolve an IRedisTypeFacoty
			// 			var factory = container.Resolve<IRedisTypeFactory>();
			//
			// 			// Get a redis set from factory
			// 			var redisSet = factory.GetSet<int>("NumbersSet");
			var redisSet = redisTypeFactory.GetSet<int>("NumbersSet");
			redisSet.Add(1);
			redisSet.Add(1);
			redisSet.Add(2);

			Console.WriteLine();
			Console.WriteLine("Set Members:");

			// Iterating through set
			foreach (var item in redisSet)
			{
				Console.WriteLine(item);
			}

			Console.WriteLine();
			Console.WriteLine("Press any key to exit...");
			//Console.Read();


		}
		static void TestCachingFramework()
		{//https://github.com/thepirat000/CachingFramework.Redis#serialization
			var context = new CachingFramework.Redis.Context("127.0.0.1:8379, connectRetry=10, abortConnect=false, allowAdmin=true"
				, new MsgBufWrapper()
				);
			var hash = context.Collections.GetRedisDictionary<int, Person>("Person:hash");
			hash.Add(1, new Person { ID = 1, Name = "Steve", Age = 20 });
			hash.Add(2, new Person { ID = 2, Name = "David", Age = 88 });
			var dv = hash.FirstOrDefault(L => L.Value.Name == "David");
			Console.WriteLine("dd {0} {1}", dv.Value.Name, dv.Value.Age);

			foreach(var v in hash)
			{
				Console.WriteLine("v {0} {1}", v.Value.Name, v.Value.Age);

			}
			//context.Cache.
		}
	}
}
