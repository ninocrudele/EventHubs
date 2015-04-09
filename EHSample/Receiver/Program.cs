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

/// <summary>
/// Nino Crudele
/// Microsoft MVP Application Integration (The Family) 7+
/// Working in Solidsoft Reply
/// Developer - Speaker - Technology and Software passionate
/// Totally focused on Integration Technologies
/// </summary>

/// presenta il programma mvp
/// BizTalk Crew
/// presenta lhub
/// code
/// presenta analytic
/// esempio secondo
/// stop
namespace Receiver
{
    class Program
    {
        enum receiveType { Direct, Abstractions, AbstractionsAsync, AbstractionsAsyncGroup }
        static void Main(string[] args)
        {
            //INIT Engine
            Engine engine = new Engine();

            //Check command params
            if (args.Length == 0)
            {
                Console.WriteLine("Enter the the receive pattern type [Direct, Abstractions, AbstractionsAsync, AbstractionsAsyncGroup] Abstractions.");
                Environment.Exit(0);
            }
                        
            //Log
            ConsoleWriteLine(string.Format("Start Time->{0}:{1}:{2}:{3}", DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, DateTime.UtcNow.Second, DateTime.UtcNow.Millisecond), ConsoleColor.Gray);
            ConsoleWriteLine(string.Format("Receiver name {0}.", AppDomain.CurrentDomain.FriendlyName), ConsoleColor.Gray);

            //Get receiver type
            string receiverType = args[0].ToString();
            ConsoleWriteLine(string.Format("Receive pattern type: {0}", receiverType), ConsoleColor.Yellow);
            //COMMAND PARAMENTERS CALLS

            //COMMAND Direct => Execute a simple direct receiver pattern
            if (receiverType == receiveType.Direct.ToString())
            {
                //Direct
                //while (true)
                //{
                //    Thread.Sleep(1000);
                //    //Thread T = new Thread(() => ReceiveDirectMessage());
                //    //T.Start();
                    engine.Direct();
                    ConsoleWriteLine("Direct Receiver polling done...", ConsoleColor.DarkYellow);
                //}
            }

            ////COMMAND Abstractions => Execute 
            if (receiverType == receiveType.Abstractions.ToString())
                //Abstraction 
                engine.Abstractions(null);

            //COMMAND AbstractionsAsync => Execute 
            if (receiverType == receiveType.AbstractionsAsync.ToString())
            //Abstraction 
            {
                engine.AbstractionsAsync(null);

            }
            //{
            //    for (int i = 0; i < 10; i++)
            //    {
            //        Thread T = new Thread(() => engine.AbstractionsAsync(null));
            //        T.Start();

            //    }
            //}
            ////COMMAND AbstractionsAsyncGroup => Execute 
            if (receiverType == receiveType.AbstractionsAsyncGroup.ToString())
            //Abstraction 
            {
                string group = args[1].ToString();
                engine.AbstractionsAsync(group);
            }



            //multithread
            //for (int receiver = 1; receiver <= numOfReceiver; receiver++)
            //{
            //    //Thread T = new Thread(() => sendSensorDataToEH(site, numOfMessages));
            //    //T.Start();
            //    Thread t = new Thread(new ParameterizedThreadStart(ReceiveMessage));
            //    t.Start(receiver);
            //}

            Console.ReadLine();


        }
        /// <summary>
        /// logging
        /// </summary>
        /// <param name="message"></param>
        /// <param name="color"></param>
        public static void ConsoleWriteLine(string message, ConsoleColor color)
        {
            Console.ForegroundColor = color;
            Console.WriteLine(message);
            Console.ForegroundColor = ConsoleColor.White;
        }
    }
}
