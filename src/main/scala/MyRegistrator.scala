import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}
import com.twitter.chill.Kryo

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[KafkaMessage])
  }
}
