#region Copyright 
//======================================================================================= 
// Copyright (c) Nino Crudele. http://ninocrudele.me/ All rights reserved. 
//  
// LICENSED UNDER THE APACHE LICENSE, VERSION 2.0 (THE "LICENSE"); YOU MAY NOT USE THESE  
// FILES EXCEPT IN COMPLIANCE WITH THE LICENSE. YOU MAY OBTAIN A COPY OF THE LICENSE AT  
// http://www.apache.org/licenses/LICENSE-2.0 
// UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING, SOFTWARE DISTRIBUTED UNDER THE  
// LICENSE IS DISTRIBUTED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  
// KIND, EITHER EXPRESS OR IMPLIED. SEE THE LICENSE FOR THE SPECIFIC LANGUAGE GOVERNING  
// PERMISSIONS AND LIMITATIONS UNDER THE LICENSE. 
//======================================================================================= 
#endregion 
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace Sender
{
    class Program
    {
        //Globals
        //Num of messages
        static int messagesToSend = 0;
        //ms to sleep each message
        static int sleep = 0;
        //ms to sleep each message
        static string partitionKey = "0";
        //Groupname
        static string groupname = "0";

        static void Main(string[] args)
        {

            string ehConnectionString = "Endpoint=sb://[your namespace].servicebus.windows.net;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=[your secret]";
            //Create the connection string
            ServiceBusConnectionStringBuilder builder = new ServiceBusConnectionStringBuilder(ehConnectionString)
            {
                TransportType = TransportType.Amqp
            };
            //Create the EH sender
            string eventHubName = "[EVENTHUBNAME]";
            //OPTIONS 1
            eventHubClient = EventHubClient.CreateFromConnectionString(builder.ToString(), eventHubName);
            //for (int i = 0; i < 5; i++)
            //{
            //    EventData eventData = new EventData(Encoding.UTF8.GetBytes("test"));
            //    eventHubClient.SendAsync(eventData);
            //}
            //Check command params
            if (args.Length == 0)
            {
                Console.WriteLine("Combinations coud be [SendMultiplePartitionMessage] + [ Num of Messages to send] + [sleep time]  + [partitionkey] + [Groupname]");
                Environment.Exit(0);
            }
            messagesToSend = int.Parse(args[1]);

            Console.WriteLine("Create / Open EH {0}...", eventHubName);
            Console.WriteLine("Ready to send {0} messages... press button to start...", messagesToSend);

            switch (args[0])
            {
                case "SendMessageLoopSync":
                    //Send message using multiple round robin partition pattern
                    SendMessageLoopSync();
                    break;
                case "SendMessageLoopASync":
                    //Send message using multiple round robin partition pattern
                    SendMessageLoopASync();
                    break;
                case "SendMessageLoopASyncsimpleMessage":
                    //Send message using multiple round robin partition pattern
                    SendMessageLoopASyncsimpleMessage();
                    break;
                case "SendMessagesThread":
                    //Send message using multiple round robin partition pattern
                    SendMessagesThread(messagesToSend);
                    break;
                case "SendHighBatchMessagesThread":
                    //Send message using multiple round robin partition pattern
                    SendHighBatchMessagesThread(messagesToSend);
                    break;


                    
                default:
                    break;
            }


            Console.ReadLine();



        }

        static EventHubClient eventHubClient = null;
        static string senderId = "";
        private void InitEH()
        {
            string senderId = AppDomain.CurrentDomain.FriendlyName;
            string ehConnectionString = "Endpoint=sb://[your namespace].servicebus.windows.net;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=[your secret]";


            //Create the connection string
            ServiceBusConnectionStringBuilder builder = new ServiceBusConnectionStringBuilder(ehConnectionString)
            {
                TransportType = TransportType.Amqp
            };

            //Create the EH sender
            string eventHubName = "test123";

            //OPTIONS 1
            eventHubClient = EventHubClient.CreateFromConnectionString(builder.ToString(), eventHubName);


        }

        private static void SendMessagesThread(int messagesToSend)
        {

            //Log
            ConsoleWriteLine(string.Format("Start Time->{0}:{1}:{2}:{3}", DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTime.UtcNow.Millisecond), ConsoleColor.White);
            ConsoleWriteLine(string.Format("Sender name {0}.", AppDomain.CurrentDomain.FriendlyName), ConsoleColor.Yellow);

            //OPTIONS 2
            //NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(builder.ToString());
            //EventHubDescription ehDescription = namespaceManager.GetEventHub("hyis");

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            LogQueue<string> logQueue = new LogQueue<string>(messagesToSend, 1,eventHubClient);

            int numMessagesSent = 0;
            while (numMessagesSent < messagesToSend)
            {
                numMessagesSent++;
                logQueue.Enqueue("stringa");
                
            }
            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;
            int millisecondSpent = ts.Milliseconds;
            ConsoleWriteLine(string.Format("Total milliseconds {0} - Seconds {1}", millisecondSpent, (millisecondSpent / 1000)), ConsoleColor.Red);

            ConsoleWriteLine(string.Format("End Time->{0}:{1}:{2}:{3}", DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTime.UtcNow.Millisecond), ConsoleColor.Yellow);
            Console.ReadLine();

        }

        private static void SendHighBatchMessagesThread(int messagesToSend)
        {

            //Log
            ConsoleWriteLine("Ready To GO.", ConsoleColor.White);
            ConsoleWriteLine(string.Format("Start Time->{0}:{1}:{2}:{3}", DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTime.UtcNow.Millisecond), ConsoleColor.White);
            ConsoleWriteLine(string.Format("Sender name {0}.", AppDomain.CurrentDomain.FriendlyName), ConsoleColor.Yellow);

            //OPTIONS 2
            //NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(builder.ToString());
            //EventHubDescription ehDescription = namespaceManager.GetEventHub("hyis");

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            int numTotalMessagesSent = 0;


            for (int i = 0; i<10;i++)
            {
                int numMessagesSent = 0;
                numTotalMessagesSent += messagesToSend;
                LogQueue<string> logQueue1 = new LogQueue<string>(messagesToSend, 1, eventHubClient);

                while (numMessagesSent < messagesToSend)
                {
                    numMessagesSent++;
                    logQueue1.Enqueue("x");

                }
                

            }

            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;
            int millisecondSpent = ts.Milliseconds;
            ConsoleWriteLine(string.Format("Message sent {0}", numTotalMessagesSent), ConsoleColor.Red);
            ConsoleWriteLine(string.Format("Total milliseconds {0} - Seconds {1}", millisecondSpent, (millisecondSpent / 1000)), ConsoleColor.Red);

            ConsoleWriteLine(string.Format("End Time->{0}:{1}:{2}:{3}", DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTime.UtcNow.Millisecond), ConsoleColor.Yellow);
            Console.ReadLine();

        }

        private static void SendMessageLoopSync()
        {

            //Log
            ConsoleWriteLine(string.Format("Start Time->{0}:{1}:{2}:{3}", DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTime.UtcNow.Millisecond), ConsoleColor.White);
            ConsoleWriteLine(string.Format("Sender name {0}.", AppDomain.CurrentDomain.FriendlyName), ConsoleColor.Yellow);

            //OPTIONS 2
            //NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(builder.ToString());
            //EventHubDescription ehDescription = namespaceManager.GetEventHub("hyis");

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            int numMessagesSent = 0;
            while (numMessagesSent < messagesToSend)
            {
                numMessagesSent++;
                string trasactionID = Guid.NewGuid().ToString();
                SampleMessage sensorData = new SampleMessage() { TransactionID = trasactionID, MessageNumber = numMessagesSent, SenderId = senderId };
                //Serialization
                var serializedMessage = JsonConvert.SerializeObject(sensorData);
                EventData data = new EventData(Encoding.UTF8.GetBytes(serializedMessage));
                // Send the metric to Event Hub

                //data.PartitionKey = "0";

                eventHubClient.Send(data);

            }
            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;
            int millisecondSpent = ts.Milliseconds;
            ConsoleWriteLine(string.Format("Total milliseconds {0} - Seconds {1}", millisecondSpent, (millisecondSpent / 1000)), ConsoleColor.Red);

            ConsoleWriteLine(string.Format("End Time->{0}:{1}:{2}:{3}", DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTime.UtcNow.Millisecond), ConsoleColor.Yellow);
            Console.ReadLine();


        }


        private static void SendMessageLoopASync()
        {

            //Log
            ConsoleWriteLine(string.Format("Start Time->{0}:{1}:{2}:{3}", DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTime.UtcNow.Millisecond), ConsoleColor.White);
            ConsoleWriteLine(string.Format("Sender name {0}.", AppDomain.CurrentDomain.FriendlyName), ConsoleColor.Yellow);

            //OPTIONS 2
            //NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(builder.ToString());
            //EventHubDescription ehDescription = namespaceManager.GetEventHub("hyis");

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            int numMessagesSent = 0;
            while (numMessagesSent < messagesToSend)
            {
                numMessagesSent++;
                string trasactionID = Guid.NewGuid().ToString();
                SampleMessage sensorData = new SampleMessage() { TransactionID = trasactionID, MessageNumber = numMessagesSent, SenderId = senderId };
                //Serialization
                var serializedMessage = JsonConvert.SerializeObject(sensorData);
                EventData data = new EventData(Encoding.UTF8.GetBytes(serializedMessage));
                // Send the metric to Event Hub

                //data.PartitionKey = "0";

                eventHubClient.SendAsync(data);

            }
            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;
            int millisecondSpent = ts.Milliseconds;
            ConsoleWriteLine(string.Format("Total milliseconds {0} - Seconds {1}", millisecondSpent, (millisecondSpent / 1000)), ConsoleColor.Red);

            ConsoleWriteLine(string.Format("End Time->{0}:{1}:{2}:{3}", DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTime.UtcNow.Millisecond), ConsoleColor.Yellow);
            Console.ReadLine();


        }



        private static void SendMessageLoopASyncsimpleMessage()
        {

            //Log
            ConsoleWriteLine(string.Format("Start Time->{0}:{1}:{2}:{3}", DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTime.UtcNow.Millisecond), ConsoleColor.White);
            ConsoleWriteLine(string.Format("Sender name {0}.", AppDomain.CurrentDomain.FriendlyName), ConsoleColor.Yellow);

            //OPTIONS 2
            //NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(builder.ToString());
            //EventHubDescription ehDescription = namespaceManager.GetEventHub("hyis");

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            int numMessagesSent = 0;
            while (numMessagesSent < messagesToSend)
            {
                numMessagesSent++;
                EventData data = new EventData(Encoding.UTF8.GetBytes("stringa".ToString()));
                eventHubClient.SendAsync(data);

            }
            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;
            int millisecondSpent = ts.Milliseconds;
            ConsoleWriteLine(string.Format("Total milliseconds {0} - Seconds {1}", millisecondSpent, (millisecondSpent / 1000)), ConsoleColor.Red);

            ConsoleWriteLine(string.Format("End Time->{0}:{1}:{2}:{3}", DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTime.UtcNow.Millisecond), ConsoleColor.Yellow);
            Console.ReadLine();


        }


        public static void ConsoleWriteLine(string message, ConsoleColor color)
        {
            Console.ForegroundColor = color;
            Console.WriteLine(message);
            Console.ForegroundColor = ConsoleColor.White;
        }

    }
    [DataContract]
    public class SampleMessage
    {
        [DataMember]
        public string TransactionID { get; set; }
        [DataMember]
        public int MessageNumber { get; set; }
        [DataMember]
        public string SenderId { get; set; }
    }

    public class LogQueue<T> : ConcurrentQueue<T> where T : class
    {
        protected int _captureLimit;
        protected int _timeLimit;
        protected System.Timers.Timer _timer;
        protected int _onPublishExecuted;
        protected ReaderWriterLockSlim _locker;

        //EH
        protected EventHubClient _eventHubClient = null;
        
        public LogQueue(int capLimit, int timeLimit, EventHubClient eventHubClient)
        {
            Init(capLimit, timeLimit);
            _eventHubClient = eventHubClient;


        }

        public event Action<List<T>> OnPublish = delegate { };

        //Write the item
        public virtual new void Enqueue(T item)
        {
            //put in queue
            base.Enqueue(item);
            //If > caplimit the publish
            if (Count >= _captureLimit)
            {

                Publish();
            }
        }

        private void Init(int capLimit, int timeLimit)
        {
            _captureLimit = capLimit;
            _timeLimit = timeLimit;
            _locker = new ReaderWriterLockSlim();
            InitTimer();
        }

        protected virtual void InitTimer()
        {
            _timer = new System.Timers.Timer();
            _timer.AutoReset = false;
            _timer.Interval = _timeLimit * 1000;
            //_timer.Elapsed += new ElapsedEventHandler((s, e) =>
            //{
            //    Publish();
            //});
            _timer.Start();
        }

        protected virtual void Publish()
        {
            Task task = new Task(() =>
            {
                List<T> itemsToLog = new List<T>();
                try
                {
                    if (Interlocked.CompareExchange(ref _onPublishExecuted, 1, 0) > 0)
                    {
                        return;
                    }

                    //if (IsPublishing())
                    //    return;

                    _timer.Stop();


                    T item;

                    //Batch*********************************

                    EventData[] arrEventData = Array.ConvertAll(base.ToArray(), myelement => new EventData(Encoding.UTF8.GetBytes(myelement.ToString())));
                    Stopwatch stopWatch = new Stopwatch();
                    stopWatch.Start();
                    int a = arrEventData.GetLength(0);

                    _eventHubClient.SendBatch(arrEventData);
                    //_eventHubClient.SendBatchAsync(arrEventData);

                    stopWatch.Stop();
                    TimeSpan ts = stopWatch.Elapsed;
                    int millisecondSpent = ts.Milliseconds;
                    Console.WriteLine(string.Format("Total milliseconds to deliver in EH {0} - Seconds {1}", millisecondSpent, (millisecondSpent / 1000)), ConsoleColor.Red);
                    //Batch*********************************

                    //Single message***********************
                    //while (TryDequeue(out item))
                    //{
                    //    itemsToLog.Add(item);
                    //    EventData data = new EventData(Encoding.UTF8.GetBytes(item.ToString()));
                    //    //_eventHubClient.Send(data);
                    //    _eventHubClient.SendAsync(data);

                    //}
                    //Single message***********************

                }
                catch (ThreadAbortException tex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("General lev 1 Exception {0}", tex);
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("General lev 2 Exception {0}", ex);

                }
                finally
                {
                    Interlocked.Decrement(ref _onPublishExecuted);
                    OnPublish(itemsToLog);
                    CompletePublishing();
                }
            });
            task.Start();
        }

        private bool IsPublishing()
        {
            return (Interlocked.CompareExchange(ref _onPublishExecuted, 1, 0) > 0);
        }

        private void CompletePublishing()
        {
            _timer.Start();
            Interlocked.Decrement(ref _onPublishExecuted);
        }
    }

}
