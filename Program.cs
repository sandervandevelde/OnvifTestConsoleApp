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
            Console.WriteLine("Hello, MQTT client for ONVIF commands!");

            var asset = "demofactory-162-onvif-device-onvif";
            var responseTopic = $"onvif/command/response/{asset}";

            var correlationData = Guid.NewGuid().ToString("N").Substring(0, 16);

            Console.WriteLine($"Correlation: {correlationData}");

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

            var topicGetProfiles = $"azure-iot-operations/asset-operations/{asset}/Media/GetProfiles";
            var payloadGetProfiles = "{}";

            var topicRelativeMove = $"azure-iot-operations/asset-operations/{asset}/PTZ/RelativeMove";
            // x != 0 and y != 0 at the same time
            // postive/negative value of x controls left/right movement, postive/negative value of y controls up/down movement
            var payloadRelativeMove = @"
{
  ""RelativeMove"":{
    ""ProfileToken"": ""profile_1"",
    ""Translation"": {
      ""PanTilt"": {
	      ""x"": -0.1, 
  	      ""y"": 0.0
      }
    } 
  }
}";


            var topicAbsoluteMove = $"azure-iot-operations/asset-operations/{asset}/PTZ/AbsoluteMove";
            var payloadAbsoluteMove = @"
{
  ""AbsoluteMove"":{
    ""ProfileToken"": ""profile_1"",
    ""Position"": {
      ""PanTilt"": {
	      ""x"": 0.2, 
  	      ""y"": -0.5
      }
    } 
  }
}";

            var topicContinuousMove = $"azure-iot-operations/asset-operations/{asset}/PTZ/ContinuousMove";
            // Timeout seems to be ignored by the ONVIF device
            var payloadContinuousMove = @"
{
  ""ContinuousMove"":{
    ""ProfileToken"": ""profile_1"",
    ""Timeout"": ""PT30S"",
    ""Velocity"": {
      ""PanTilt"": {
	      ""x"": 0.9, 
  	      ""y"": 0.0
      }
    } 
  }
}";

            //azure-iot-operations/asset-operations/demofactory-161-onvif-device-onvif/PTZ/GetStatus
            //{
            //    "GetStatus":{
            //        "ProfileToken": "profile_1"
            //    }
            //}

            await mqttClient.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic(topicAbsoluteMove) // TOPIC
                    .WithCorrelationData(Encoding.UTF8.GetBytes(correlationData))
                    .WithContentType("application/json")
                    .WithMessageExpiryInterval(60)
                    .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithResponseTopic(responseTopic)
                    .WithPayload(payloadAbsoluteMove) // PAYLOAD
                    .Build(),
                CancellationToken.None);
            Console.WriteLine($"Command send to {topicAbsoluteMove}."); // TOPIC

            await Task.Delay(60000); // Wait for messages to be received before exiting
            Console.WriteLine("End of program.");
        }
    }
}