package mqtthelpers

import (
	log "github.com/Sirupsen/logrus"
	"github.com/yosssi/gmq/mqtt"
	"github.com/yosssi/gmq/mqtt/client"
)

// PublishMessage - Publishes message to respective channel. This is Fire And Forget Style message being sent
func PublishMessage(mqttURI string, channel string, message string) {
	if (mqttURI == "") || (channel == "") || (message == "") {
		// fmt.Println("Please Start Worker with Required Arguments")
		log.Error("mqtthelpers: Missing args| mqttURI - %v | channel - %v | message - %v", mqttURI, channel, message)
		return
	}

	// Create an MQTT Client.
	cli := client.New(&client.Options{
		// Define the processing of the error handler.
		ErrorHandler: func(err error) {
			log.Error("mqtthelpers: Error %v", err)
		},
	})

	// Terminate the Client.
	defer cli.Terminate()

	// Connect to the MQTT Server.
	err := cli.Connect(&client.ConnectOptions{
		Network:  "tcp",
		Address:  mqttURI,
		ClientID: []byte("example-client"),
	})
	if err != nil {
		log.Error("mqtthelpers: Error Connecting to MQTT Server: %v", err)
		return
	}

	// Publish a message.
	pubErr := cli.Publish(&client.PublishOptions{
		QoS:       mqtt.QoS0,
		TopicName: []byte(channel),
		Message:   []byte(message),
	})
	if pubErr != nil {
		log.Error("mqtthelpers: Error publishing message: %v", pubErr)
	}

	// // Disconnect the Network Connection.
	// if disErr := cli.Disconnect(); disErr != nil {
	// 	// panic(err)
	// 	fmt.Println("WE HAVE ERR DIS: %v", disErr)
	// }

	// cli.Disconnect()
}
