package instrument

// Action represents the main action of the kafka transformer
type Action string

const (
	// OverallTime represents the overall process from consume to project
	OverallTime = Action("kafka_transformer_overall")
	// KafkaConsumerConsume represents the consume action
	KafkaConsumerConsume = Action("kafka_consumer_consume")
	// KafkaProducerProduce represents the produce action to kafka
	KafkaProducerProduce = Action("kafka_producer_produce")
	// TransformerTransform represents the transform action to kafka
	TransformerTransform = Action("transformer_transform")
	// ProjectorProject represents the project action
	ProjectorProject = Action("projector_project")
)
