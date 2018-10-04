import babyactors._

class Counter(ctx: ActorContext) extends Actor(ctx) {
  var c = 0
  var t1 = System.nanoTime
  def receive: PartialFunction[String, Unit] = {
    case msg =>
      c += 1
      ctx.send(Message(self, "Hi self!"))
      if (c % 10000 == 0) {
        println((System.nanoTime - t1) / 1E6)
        t1 = System.nanoTime
      }
  }
}

class PingPong(mate: ActorRef, ctx: ActorContext) extends Actor(ctx) {
  var t1 = System.nanoTime
  def receive: PartialFunction[String, Unit] = {
    case msg =>
      val c = msg.toInt
      ctx.send(Message(mate, (c + 1).toString))
      if (c % 10000 == 0) {
        println((System.nanoTime - t1) / 1E6)
        t1 = System.nanoTime
      }
  }
}

class Master(ctx: ActorContext) extends Actor(ctx) {
  var todo = -1
  def receive: PartialFunction[String, Unit] = {
    case msg if msg.startsWith("work") =>
      val work = msg.drop(4).toInt
      todo = work
      (0 until work).foreach { i =>
        ctx.send(
          Message(ActorRef(i, DispatcherRef(4), ActorName(4)), "subtask" + i))
        Thread.sleep(1000)
      }
    case "done" =>
      todo -= 1
      if (todo == 0) {
        println("DONE")
      }
    case _ =>
  }
}

class Worker(master: ActorRef, ctx: ActorContext) extends Actor(ctx) {
  def receive: PartialFunction[String, Unit] = {
    case msg =>
      println("got " + msg)
      Thread.sleep(5000)
      println("sleep done")
      ctx.send(Message(master, "done"))

  }
}

class MainActor(ctx: ActorContext) extends Actor(ctx) {
  def receive: PartialFunction[String, Unit] = {
    case "start" =>
      ctx.send(Message(Hello.p1, "0"))
      ctx.send(Message(Hello.p2, "0"))
      ctx.send(Message(Hello.master, "work4"))

  }
}

object Hello extends App {
  println("start")
  val p1 = ActorRef(1, DispatcherRef(0), ActorName(2))
  val p2 = ActorRef(0, DispatcherRef(1), ActorName(1))
  val master = ActorRef(2, DispatcherRef(2), ActorName(3))

  // scala native has no reflection, this is an ugly workaround
  ActorSystem.register(ActorName(-1), (d: ActorContext) => new MainActor(d))
  ActorSystem.register(ActorName(0), (d: ActorContext) => new Counter(d))
  ActorSystem.register(ActorName(1), (d: ActorContext) => new PingPong(p1, d))
  ActorSystem.register(ActorName(2), (d: ActorContext) => new PingPong(p2, d))
  ActorSystem.register(ActorName(3), (d: ActorContext) => new Master(d))
  ActorSystem.register(ActorName(4), (d: ActorContext) => new Worker(master, d))

  val actorSystem = new ActorSystem(
    Message(ActorRef(0, DispatcherRef(0), ActorName(-1)), "start"))

  println("Bye")
}
