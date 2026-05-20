using System.Text;
using MQTTnet;
using MQTTnet.Packets;

namespace OnvifTestConsoleApp
{
    class Program
    {
        static string _lastResponse = string.Empty;

        static async Task Main()
        {
            Console.WriteLine("MQTT Sentry client for ONVIF AbsoluteMove action commands!");

            var asset = "demofactory-162-onvif-device-onvif";
            var responseTopic = $"onvif/command/response/{asset}";

            // Construct MQTT client
            var mqttFactory = new MqttClientFactory();

            using var mqttClient = mqttFactory.CreateMqttClient();

            var mqttClientOptions = new MqttClientOptionsBuilder()
                .WithTcpServer("192.168.3.196", 31883)
                .WithCleanSession(false)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .WithWillQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce)
                .Build();

            mqttClient.ApplicationMessageReceivedAsync += async e =>
            {
                Console.WriteLine($"### RECEIVED APPLICATION MESSAGE (at {DateTime.Now}) ###");
                Console.WriteLine($"+ Topic = {e.ApplicationMessage.Topic}");

                var response = e.ApplicationMessage.ConvertPayloadToString();

                if (_lastResponse == response)
                {
                    Console.WriteLine($"+ Payload is duplicate");
                }
                else
                {
                    Console.WriteLine($"+ Payload = {response}");
                    Console.WriteLine($"+ CorrelationData = {Encoding.UTF8.GetString(e.ApplicationMessage.CorrelationData)}");

                    for (int i = 0; i < e.ApplicationMessage.UserProperties.Count; i++)
                    {
                        var prop = e.ApplicationMessage.UserProperties[i];
                        Console.WriteLine($"+ UserProperty[{i}] = {prop.Name} : {prop.ReadValueAsString()}");
                    }
                }

                _lastResponse = response;
            };

            // Connect
            await mqttClient.ConnectAsync(mqttClientOptions, CancellationToken.None);

            Console.WriteLine("MQTT client connected.");

            await mqttClient.SubscribeAsync(
                new MqttTopicFilter
                {
                    Topic = responseTopic,
                });

            Console.WriteLine("Subscribed to topic.");

            double x = 0.0;
            double y = -0.5;
            var increment = 0.1;


            while (true)
            {
                var correlationData = Guid.NewGuid().ToString("N").Substring(0, 16);

                Console.WriteLine($"Correlation: {correlationData}");

                var topicAbsoluteMove = $"azure-iot-operations/asset-operations/{asset}/PTZ/AbsoluteMove";
                var payloadAbsoluteMove = @"
{
  ""AbsoluteMove"":{
    ""ProfileToken"": ""profile_1"",
    ""Position"": {
      ""PanTilt"": {
	      ""x"": XX, 
  	      ""y"": YY
      }
    } 
  }
}";

                payloadAbsoluteMove = payloadAbsoluteMove.Replace("XX", x.ToString("0.00"));
                payloadAbsoluteMove = payloadAbsoluteMove.Replace("YY", y.ToString("0.00"));

                await mqttClient.PublishAsync(
                    new MqttApplicationMessageBuilder()
                        .WithTopic(topicAbsoluteMove) // TOPIC
                        .WithCorrelationData(Encoding.UTF8.GetBytes(correlationData))
                        .WithContentType("application/json")
                        .WithMessageExpiryInterval(4)
                        .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .WithResponseTopic(responseTopic)
                        .WithPayload(payloadAbsoluteMove) // PAYLOAD
                        .Build(),
                    CancellationToken.None);
                Console.WriteLine($"Command send to {payloadAbsoluteMove}."); // TOPIC

                //                await Task.Delay(60000); // Wait for messages to be received before exiting
                //                Console.WriteLine("End of program.");

                await Task.Delay(5000);

                x = x + increment;

                if (x >= 0.2)
                {
                    increment = -0.1;
                }
                else if (x <= -0.4)
                {
                    increment = 0.1;
                }
            }
        }
    }
}