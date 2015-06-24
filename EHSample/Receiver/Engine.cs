#region Copyright 
//======================================================================================= 
// Copyright (c) Nino Crudele.http://ninocrudele.me/ All rights reserved. 
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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Receiver
{
    class Engine
    {

        public string GetCreateEHAndConsumerGroup(string ConsumenrGroup, ref string eventHubName, ref string eventHubConnectionString, ref string storageConnectionString)
        {
            try
            {
                //Set connection strings
                eventHubConnectionString = "Endpoint=sb://[NAMESPACE].servicebus.windows.net/;SharedAccessKeyName=All;SharedAccessKey=[SharedAccessKey]";
                string eventHubConnectionStringAdmin = "Endpoint=sb://[NAMESPACE].servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=[SharedAccessKey]";
                
                eventHubName = "[EVENTHUBNAME]";
                string storageAccountName = "[STORAGEACCOUNTNAME]";
                string storageAccountKey = "[STORAGEACCOUNTKEY]";
                storageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                            storageAccountName, storageAccountKey);
                //Create connection string
                var builder = new ServiceBusConnectionStringBuilder(eventHubConnectionString)
                {
                    TransportType = Microsoft.ServiceBus.Messaging.TransportType.Amqp

                };

                ConsoleWriteLine("Create/Open the EH", ConsoleColor.Magenta);
                NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(eventHubConnectionStringAdmin);
                var evenhubDesc = new EventHubDescription(eventHubName);
                namespaceManager.CreateEventHubIfNotExists(evenhubDesc);

                if (ConsumenrGroup == null)
                {
                    var client = EventHubClient.CreateFromConnectionString(builder.ToString(), eventHubName);
                    EventHubConsumerGroup eventHubConsumerGroup = client.GetDefaultConsumerGroup();
                    return eventHubConsumerGroup.GroupName;

                }
                else
                {
                    namespaceManager.CreateConsumerGroupIfNotExists(eventHubName, ConsumenrGroup);
                    return ConsumenrGroup;
                }
            }
            catch (Exception ex)
            {
                ConsoleWriteLine("ERROR-" + ex.Message, ConsoleColor.Red);
                return null;
            }
        }

        /// <summary>
        /// Receive message in direct pattern
        /// </summary>
        public void Direct()
        {
            //DA FINIRE
            ConsoleWriteLine("Start Receiver [DIRECT MESSAGE]", ConsoleColor.Yellow);

            //Single
            bool receive = true;
            string myOffset;
            string connectionString = "Endpoint=sb://[NAMESPACE].servicebus.windows.net/;SharedAccessKeyName=All;SharedAccessKey=[SharedAccessKey]";
            
            //CONNN---
            //Create the connection string
            ServiceBusConnectionStringBuilder builder = new ServiceBusConnectionStringBuilder(connectionString)
            {
                TransportType = TransportType.Amqp
            };

            //Create the EH Client
            string eventHubName = "test123";
            EventHubClient eventHubClient = EventHubClient.CreateFromConnectionString(builder.ToString(), eventHubName);

            //muli partition sample
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(builder.ToString());
            EventHubDescription ehDescription = namespaceManager.GetEventHub("test123");
            //Use the default consumer group

            foreach (var partitionId in ehDescription.PartitionIds)
            {
                Thread myNewThread = new Thread(() => ReceiveDirectFromPartition(eventHubClient, partitionId));
                myNewThread.Start();

            }
            //////////////////////



            ConsoleWriteLine("Direct Receiver polling done...", ConsoleColor.DarkYellow);
        }

        private void ReceiveDirectFromPartition(EventHubClient eventHubClient, string partitionId)
        {
            EventHubConsumerGroup group = eventHubClient.GetDefaultConsumerGroup();
            //var receiver = group.CreateReceiver(eventHubClient.GetRuntimeInformation().PartitionIds[0]);

            //var receiver = group.CreateReceiver(partitionId);
            var receiver = group.CreateReceiver(partitionId, DateTime.UtcNow);

            ConsoleWriteLine(string.Format("Direct Receiver created... Partition {0}", partitionId), ConsoleColor.Yellow);
            while (true)
            {
                var message = receiver.Receive();
                string body = Encoding.UTF8.GetString(message.GetBytes());
                ConsoleWriteLine(String.Format("Received message offset: {0} \nbody: {1}", message.Offset, body), ConsoleColor.Green);
            }
        }

        /// <summary>
        /// Using EventProcessor in Sync pattern
        /// </summary>
        public void Abstractions(string eventHubConsumerGroup)
        {
            ConsoleWriteLine("Start Receiver [HIGH LEVEL ABSTRACTIONS ASYNC]", ConsoleColor.Yellow);
            try
            {
                string eventHubName = "";
                string eventHubConnectionString = "";
                string storageConnectionString = "";
                eventHubConsumerGroup = GetCreateEHAndConsumerGroup(eventHubConsumerGroup, ref eventHubName, ref eventHubConnectionString, ref storageConnectionString);


                try
                {
                    string eventProcessorHostName = Guid.NewGuid().ToString();
                    ConsoleWriteLine(string.Format("Start the EH {0} - EventProcessorHost name {1} - eventHubConsumerGroup {2}", eventHubName, eventProcessorHostName, eventHubConsumerGroup), ConsoleColor.Yellow);
                    EventProcessorHost eventProcessorHost = new EventProcessorHost(eventProcessorHostName, eventHubName, eventHubConsumerGroup, eventHubConnectionString, storageConnectionString);
                    eventProcessorHost.RegisterEventProcessorAsync<EventProcessor>().Wait();
                    Console.WriteLine("Receiving... Press enter key to stop EventProcessorHost.");
                    Console.ReadLine();
                }
                catch (Exception ex)
                {
                    ConsoleWriteLine(string.Format("Error: {0}", ex.Message), ConsoleColor.Yellow);
                }

            }
            catch (Exception ex)
            {
                ConsoleWriteLine(string.Format("Error: {0}", ex.Message), ConsoleColor.Yellow);

            }

        }


        /// <summary>
        /// Using EventProcessor in Async pattern
        /// </summary>
        public void AbstractionsAsync(string eventHubConsumerGroup)
        {
            ConsoleWriteLine("Start Receiver [HIGH LEVEL ABSTRACTIONS ASYNC]", ConsoleColor.Yellow);
            try
            {
                string eventHubName = "";
                string eventHubConnectionString = "";
                string storageConnectionString = "";
                eventHubConsumerGroup = GetCreateEHAndConsumerGroup(eventHubConsumerGroup, ref eventHubName, ref eventHubConnectionString, ref storageConnectionString);


                try
                {
                    string eventProcessorHostName = Guid.NewGuid().ToString();
                    ConsoleWriteLine(string.Format("Start the EH {0} - EventProcessorHost name {1} - eventHubConsumerGroup {2}", eventHubName, eventProcessorHostName, eventHubConsumerGroup), ConsoleColor.Yellow);
                    EventProcessorHost eventProcessorHost = new EventProcessorHost(eventProcessorHostName, eventHubName, eventHubConsumerGroup, eventHubConnectionString, storageConnectionString);
                    eventProcessorHost.RegisterEventProcessorAsync<EventProcessorAsync>().Wait();
                    Console.WriteLine("Receiving... Press enter key to stop EventProcessorHost.");
                    Console.ReadLine();
                }
                catch (Exception ex)
                {
                    ConsoleWriteLine(string.Format("Error: {0}", ex.Message), ConsoleColor.Yellow);
                }

            }
            catch (Exception ex)
            {
                ConsoleWriteLine(string.Format("Error: {0}", ex.Message), ConsoleColor.Yellow);

            }

        }



        /// <summary>
        /// logging
        /// </summary>
        /// <param name="message"></param>
        /// <param name="color"></param>
        public void ConsoleWriteLine(string message, ConsoleColor color)
        {
            Console.ForegroundColor = color;
            Console.WriteLine(message);
            Console.ForegroundColor = ConsoleColor.White;
        }

    }
    /// <summary>
    /// for multi partition checkpoint management
    /// </summary>
    public class MyCheckpointManager : ICheckpointManager
    {
        public Task CheckpointAsync(Lease lease, string offset, long sequenceNumber)
        {
            Console.WriteLine("------------------------------------------------------------------------");
            Console.WriteLine("ICheckpointManager->offset:{0}->sequenceNumber{1}", offset, sequenceNumber);
            Console.WriteLine("------------------------------------------------------------------------");
            lease.Offset = offset;
            lease.Owner = "Area51";
            lease.PartitionId = "1";
            lease.SequenceNumber = sequenceNumber;

            return Task.FromResult<object>(lease);
        }
    }
}
