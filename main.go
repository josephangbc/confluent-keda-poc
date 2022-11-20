package main

import (

	"github.com/gin-gonic/gin"

	"confluent-keda-poc/controllers"
)


func main() {
	// Set the router as the default one shipped with Gin
	router := gin.Default()

	// Kafka Setup
	go controllers.SetupKafkaProducer()
	go controllers.SetupKafkaConsumerParallel()

	// Setup route group for the API
	api := router.Group("/api")

	api.GET("/", controllers.MessagePongHandler)
	api.GET("/produce", controllers.ProduceMessage)
	api.POST("/produceto", controllers.ProduceMessageTo)
	api.POST("/generatehighcpu", controllers.GenerateHighCPU)


	// Start and run the server
	router.Run(":5000")
}
