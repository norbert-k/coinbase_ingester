using Coinbase.Pro.Models;
using Coinbase.Pro.WebSockets;
using RabbitMQ.Client;
using System.Text;

namespace Coinbase.Client.Websocket.Sample

{
    class Program
    {
        static async Task Main()
        {
            Console.WriteLine("Subscribing to Websocket Events.");
            await SubscribeToWebsocketEvents();
        }

        public static async Task SubscribeToWebsocketEvents()
        {
            var socket = new CoinbaseProWebSocket();


            Console.WriteLine("Connecting to websocket feed");

            var result = await socket.ConnectAsync();
            if (!result.Success)
                throw new Exception("Connection error.");

            Console.WriteLine("Connected.");

            socket.RawSocket.Closed += WSClosed;
            socket.RawSocket.Error += WSError;
            socket.RawSocket.MessageReceived += MessageReceived;

            var sub = new Subscription
            {
                ProductIds = { "BTC-EUR" },
                Channels = { "ticker" }
            };

            Console.WriteLine("Subscribing...");
            await socket.SubscribeAsync(sub);

            Console.WriteLine("Subscribed.");

            // Websocket Keepalive
            await Task.Delay(TimeSpan.FromMinutes(1));
        }

        private static void WSClosed(object sender, EventArgs e)
        {
            Console.WriteLine("Websocket closed");
        }

        private static void WSError(object sender, SuperSocket.ClientEngine.ErrorEventArgs e)
        {
            Console.WriteLine("Websocket Error");
            Console.WriteLine(e);
        }

        // Message Listener
        private static void MessageReceived(object sender, WebSocket4Net.MessageReceivedEventArgs e)
        {
            Console.WriteLine("Message received.");
            if (WebSocketHelper.TryParse(e.Message, out var msg))
            {
                if (msg is TickerEvent data)
                {
                    Console.WriteLine($"Price: {data.Price}, Coin: {data.ProductId}, Time: {data.Time}");

                    // TODO: RabbitMQ Implementation

                    string message = $"Price: {data.Price}, Coin: {data.ProductId}, Time: {data.Time}";

                    var factory = new ConnectionFactory() { HostName = "localhost", Port = 5672, UserName = "guest", Password = "guest", VirtualHost = "/"};
                    using (var connection = factory.CreateConnection())
                    using (var channel = connection.CreateModel())
                    {
                        channel.QueueDeclare(queue: "exchange_queue",
                            durable: false,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);
                        
                        Console.WriteLine("Encoding message...");
                        
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchange: "ingestor_exchange",
                            
                            // TODO: Routingkey specification?
                            
                            routingKey: "",
                            basicProperties: null,
                            body: body);

                        Console.WriteLine($" [x] Sent {message}");
                    }

                    
                    
                }
            }
        }
    }
}