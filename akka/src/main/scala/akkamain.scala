import akka._
import akka.actor._

class PingPong extends Actor {
  var t1 = System.nanoTime
  var mate: ActorRef = null
  def receive = {
    case ac: ActorRef =>
      println("mate " + ac)
      mate = ac
    case msg: String =>
      val c = msg.toInt
      mate ! (c + 1).toString
      if (c % 10000 == 0) {
        println((System.nanoTime - t1) / 1E6)
        t1 = System.nanoTime
      }
  }
}

object AkkaTest extends App {
  val as = ActorSystem()
  val ac1 = as.actorOf(Props(new PingPong).withDispatcher("dispatcher1"))
  val ac2 = as.actorOf(Props(new PingPong).withDispatcher("dispatcher2"))
  ac1 ! ac2
  ac2 ! ac1
  ac1 ! "0"
  ac2 ! "0"
}
