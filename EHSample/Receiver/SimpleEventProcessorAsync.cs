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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Newtonsoft.Json;
using System.Runtime.Serialization;
using System.Diagnostics.Tracing;

namespace Receiver
{
    class EventProcessorAsync : IEventProcessor
    {
        //EH variables
        Stopwatch checkpointStopWatch;

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            ConsoleWriteLine(string.Format("Processor Shuting Down.  Partition '{0}', Reason: '{1}'.", context.Lease.PartitionId, reason.ToString()), ConsoleColor.Cyan);
            if (reason == CloseReason.Shutdown)
            {
                await context.CheckpointAsync();
            }
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            
            ConsoleWriteLine(string.Format("SimpleEventProcessor initialize.  Partition: '{0}', Offset: '{1}'", context.Lease.PartitionId, context.Lease.Offset), ConsoleColor.Magenta);
            this.checkpointStopWatch = new Stopwatch();
            this.checkpointStopWatch.Start();
            return Task.FromResult<object>(null);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            try
            {
                int messageCont = messages.Count();

                ConsoleWriteLine(string.Format("Message received: {0}",messageCont.ToString()), ConsoleColor.DarkCyan);

                    foreach(EventData eventData in messages)
                    {
                        string key = eventData.PartitionKey;
                        try
                        {
                        ConsoleWriteLine("Received done..." + eventData.Offset, ConsoleColor.White);
                        //var messageData = JsonConvert.DeserializeObject<SampleMessage>(Encoding.UTF8.GetString(eventData.GetBytes()));
                        //ConsoleWriteLine(String.Format("Message received from Sender: {0} - MessageNumber {1} - TransactionID {2} - PartitionKey: {3}", messageData.SenderId, messageData.MessageNumber, messageData.TransactionID, eventData.PartitionKey), ConsoleColor.Green);
                    }
                        catch { }

                        await context.CheckpointAsync();
                        lock (this)
                        {
                            this.checkpointStopWatch.Reset();
                        }
                        ConsoleWriteLine("CheckPoint done...", ConsoleColor.White);
                    }



                    
                


            }
            catch(Exception ex)
            {

                ConsoleWriteLine("Error -> ProcessEventsAsync -> " + ex.Message, ConsoleColor.Red);
  
            }
        }

        public static void ConsoleWriteLine(string message, ConsoleColor color)
        {
            Console.ForegroundColor = color;
            Console.WriteLine(message);
            Console.ForegroundColor = ConsoleColor.White;

        }



    }



}
