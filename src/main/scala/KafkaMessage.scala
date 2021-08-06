case class KafkaMessage(
   partition: Int,
   offset: Long,
   value: String,
   timestamp: Long
)

