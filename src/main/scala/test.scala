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
      println("got start")
      // ctx.send(Message(ActorRef(2, DispatcherRef(1), ActorName(0)), "boo"))
      ctx.send(Message(Hello.p1, "0"))
      ctx.send(Message(Hello.p2, "0"))
    // ctx.send(Message(Hello.master, "work4"))

  }
}

object Hello extends App {
  println("start")
  val p1 = ActorRef(1, DispatcherRef(0), ActorName(2))
  val p2 = ActorRef(0, DispatcherRef(1), ActorName(1))
  // val master = ActorRef(2, DispatcherRef(0), ActorName(3))

  // scala native has no reflection, this is an ugly workaround
  ActorSystem.register(ActorName(-1), (d: ActorContext) => new MainActor(d))
  ActorSystem.register(ActorName(0), (d: ActorContext) => new Counter(d))
  ActorSystem.register(ActorName(1), (d: ActorContext) => new PingPong(p1, d))
  ActorSystem.register(ActorName(2), (d: ActorContext) => new PingPong(p2, d))
  // ActorSystem.register(ActorName(3), (d: ActorContext) => new Master(d))
  // ActorSystem.register(ActorName(4), (d: ActorContext) => new Worker(master, d))

  val actorSystem = new ActorSystem(
    Message(ActorRef(0, DispatcherRef(0), ActorName(-1)), "start"))

  import scalanative.posix.{unistd, fcntl}
  import scalanative.posix
  import scalanative.posix.fcntl._
  import scala.scalanative.native.{signal, CFunctionPtr, errno, Ptr, stdlib}
  import scala.scalanative.native
  import scala.scalanative.native._
  import scala.scalanative.native.string.memcpy
  import scalanative.runtime
  import scala.scalanative.posix.sys.{select, time, timeOps}
  import scalanative.posix.sys.stat._
  import scalanative.posix.sys.types._

  // val fd = native.Zone { implicit z =>
  //   mman.shm_open(native.toCString("./shm"),
  //                 O_CREAT | O_RDWR,
  //                 S_IRUSR | S_IWUSR)
  // }
  // unistd.ftruncate(fd, 8)
  // import mmanconst._
  // val sem = mman
  //   .mmap(null, 8, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
  //   .asInstanceOf[Ptr[native.CLong]]

  // val e = semaphore.sem_init(sem, 1, 0.toUInt)
  // println(e)
  // println(errno.errno)

  // unistd.close(fd);

  // val pid = unistd.fork()
  // if (pid == 0) {
  //   semaphore.sem_wait(sem)
  //   println("in semaphore child")
  //   throw new RuntimeException("ok")
  // } else {
  //   println("signal semaphore after 2 sec")
  //   Thread.sleep(2000)
  //   println("signal semaphore now")
  //   semaphore.sem_post(sem)
  // }

  val pipe = Pipe.allocate
  val pipe2 = Pipe.allocate
  val pid = unistd.fork()
  if (pid == 0) {
    val data = "Hi".getBytes("UTF-8")
    println("child - writing data")
    val count = pipe.write(data, 0, true)
    println(s"child - data written $count. exit child")
    val buffer = Array.ofDim[Byte](200)
    pipe2.read(buffer, 0, true)
    println("child - " + new String(buffer))
    System.exit(0)
  } else {
    val buffer = Array.ofDim[Byte](20)
    println("parent - reading data")
    val count = pipe.read(buffer, 0, true)
    println(s"Read data $count")
    println(new String(buffer))
    pipe2.write(buffer, 0, true)
  }
  println("Bye - parent")
}
