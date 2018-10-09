package babyactors

import scalanative.posix.{unistd, fcntl}
import scalanative.posix
import scala.scalanative.native.{signal, CFunctionPtr, errno, Ptr, stdlib}
import scala.scalanative.native
import scalanative.runtime
import scala.scalanative.posix.sys.{select, time, timeOps}

import scala.collection.mutable.{Queue, ListBuffer}

case class ActorRef(i: Int, dispatcher: DispatcherRef, name: ActorName)

case class ActorName(i: Int)

case class DispatcherRef(i: Int)

case class Message(recipient: ActorRef, payload: String)

object Message {
  def toFrame(m: Message): Array[Byte] = {
    val length = 16 + m.payload.length
    val buffer = java.nio.ByteBuffer.allocate(length)
    buffer.putInt(length - 4)
    buffer.putInt(m.recipient.i)
    buffer.putInt(m.recipient.dispatcher.i)
    buffer.putInt(m.recipient.name.i)
    buffer.put(m.payload.getBytes("UTF-8"))
    buffer.array
  }
  def fromFramePayload(frame: Array[Byte],
                       offset: Int,
                       length: Int): Message = {
    val buffer = java.nio.ByteBuffer.wrap(frame)
    buffer.position(offset)
    val actor = buffer.getInt
    val dispatcher = buffer.getInt
    val actorName = buffer.getInt
    val array = Array.ofDim[Byte](length - 12)
    buffer.get(array)
    Message(ActorRef(actor, DispatcherRef(dispatcher), ActorName(actorName)),
            new String(array, "UTF-8"))
  }
}

abstract class Actor(ctx: ActorContext) {
  def receive: PartialFunction[String, Unit]
  val self = ctx.self
}

class ActorSystem(message: Message) {
  private val multiplex = Impl.Multiplex.create(message)
}

object ActorSystem {
  private[babyactors] val registry =
    scala.collection.mutable.Map[ActorName, ActorContext => Actor]()
  def register(actorName: ActorName, makeActor: ActorContext => Actor) =
    registry.update(actorName, makeActor)
}

case class ActorContext(
    dispatcher: Dispatcher,
    self: ActorRef
) {
  def send(message: Message) = dispatcher.send(message)
}

class Dispatcher(private val multiplex: Impl.MultiplexPipes) {
  private val actors = scala.collection.mutable.Map[ActorRef, Actor]()

  private def next = multiplex.pipeFrom.blockRead

  def send(message: Message) =
    multiplex.pipeTo.blockWrite(message)

  while (true) {
    val message = next
    val recipient = message.recipient
    actors.get(recipient) match {
      case None =>
        val init =
          ActorSystem.registry(recipient.name)(ActorContext(this, recipient))
        actors.update(recipient, init)
        init.receive(message.payload)
      case Some(actor) =>
        actor.receive(message.payload)
    }
  }
}

private[babyactors] object Impl {

  private object Helpers {

    def setNonblock(fd: Int) = {
      val currentFlags = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
      fcntl.fcntl(fd, fcntl.F_SETFL, currentFlags | fcntl.O_NONBLOCK)
    }

    def mkPipe = {
      val link = Array(0, 0).asInstanceOf[runtime.IntArray].at(0)
      val ret = unistd.pipe(link)
      if (ret == -1) throw new RuntimeException("pipe failed")
      else link
    }

    def killall(pids: Seq[Int]) =
      pids.foreach(pid => signal.kill(pid, signal.SIGTERM))
  }
  import Helpers._

  case class DispatcherPipes(pipeTo: Pipe2.ShPipeWriteEnd,
                             pipeFrom: Pipe2.ShPipeReadEnd)
  case class MultiplexPipes(pipeTo: Pipe2.ShPipeWriteEnd,
                            pipeFrom: Pipe2.ShPipeReadEnd)

  object Multiplex {

    /**  Static field to hold a set of pids
      *
      * Used both by the multiplex and the main process (each its own copy)
      *
      */
    var pids = List[Int]()

    def create(firstMessage: Message): Unit = {

      val m = new Multiplex(firstMessage)

      def handler(s: Int) = s match {
        case s if s == signal.SIGTERM => killall(Multiplex.pids)
        case _                        => println("signal " + signal)
      }

      signal.signal(signal.SIGTERM, CFunctionPtr.fromFunction1(handler))

      m.loop

      throw new RuntimeException("multiplex.loop never returns")
    }

  }

  class Multiplex(firstMessage: Message) {
    val buffer = Queue[Message](firstMessage)
    val sendBuffer = Queue[(Message, DispatcherPipes)]()
    val actors = ListBuffer[(ActorRef, DispatcherPipes)]()

    def dispatchers =
      actors.map { case (actorRef, pipes) => (actorRef.dispatcher, pipes) }.distinct

    def allPipes =
      actors.iterator.map(_._2)

    def read(pipe: DispatcherPipes) =
      pipe.pipeFrom.nonBlockingRead.foreach { msg =>
        buffer.enqueue(msg)
      }

    def readAll() =
      allPipes.foreach(read)

    def tryWrite(message: Message, pipes: DispatcherPipes) = {
      val success = pipes.pipeTo.nonBlockingWrite(message)
      if (!success) {
        sendBuffer.enqueue((message, pipes))
      }
    }

    def createDispatcher: DispatcherPipes = {
      val pipeFromChild = babyactors.Pipe.allocate(7, 512)
      val pipeToChild = babyactors.Pipe.allocate(7, 512)

      val forkedPid = unistd.fork()
      if (forkedPid == -1) throw new RuntimeException("fork failed")
      else if (forkedPid == 0) {
        // child

        new Dispatcher(
          multiplex =
            MultiplexPipes(pipeTo = Pipe2.ShPipeWriteEnd(pipeFromChild),
                           pipeFrom = Pipe2.ShPipeReadEnd(pipeToChild)))

        throw new RuntimeException("fork child never returns")
      } else {
        // parent

        val pipes = DispatcherPipes(pipeTo = Pipe2.ShPipeWriteEnd(pipeToChild),
                                    pipeFrom =
                                      Pipe2.ShPipeReadEnd(pipeFromChild))
        Multiplex.pids = forkedPid :: Multiplex.pids
        pipes

      }
    }

    def processMessage(m: Message) = m match {
      case Message(recipient, _) =>
        actors.find(_._1 == recipient) match {
          case Some((_, dispatcher)) => tryWrite(m, dispatcher)
          case None =>
            dispatchers.find(_._1 == recipient.dispatcher) match {
              case Some((_, dispatcherPipes)) =>
                actors.append((recipient, dispatcherPipes))
                tryWrite(m, dispatcherPipes)
              case None =>
                val newPipes = createDispatcher
                actors.append((recipient, newPipes))
                tryWrite(m, newPipes)
            }
        }

    }

    def processAll() =
      buffer.dequeueAll(_ => true).foreach(processMessage)

    def tryWriteAll() =
      sendBuffer.dequeueAll(_ => true).foreach {
        case (m, pipe) => tryWrite(m, pipe)
      }

    val readSet = stdlib.malloc(8).asInstanceOf[Ptr[select.fd_set]]
    val writeSet = stdlib.malloc(8).asInstanceOf[Ptr[select.fd_set]]

    def loop = while (true) {
      tryWriteAll()
      processAll()
      // selectIo()
      readAll()
    }

  }

  object Pipe2 {
    case class ShPipeReadEnd(from: babyactors.Pipe.Pipe) {
      val buffer = Array.ofDim[Byte](100)
      def blockRead: Message = {

        var count = from.read(buffer, 0, block = true).get
        val bb = java.nio.ByteBuffer.wrap(buffer)
        val length = bb.getInt
        while (count < length + 4) {
          count = from.read(buffer, count, block = true).get
        }
        Message.fromFramePayload(buffer, 4, length)
      }

      def nonBlockingRead: Option[Message] = {

        var count = from.read(buffer, 0, block = false)
        if (count.isEmpty) None
        else {
          val bb = java.nio.ByteBuffer.wrap(buffer)
          val length = bb.getInt
          while (count.get < length + 4) {
            count = from.read(buffer, count.get, block = true)
          }
          Some(Message.fromFramePayload(buffer, 4, length))
        }
      }

    }
    case class ShPipeWriteEnd(to: babyactors.Pipe.Pipe) {
      def blockWrite(message: Message): Unit = {
        val buffer = Message.toFrame(message)
        var count = 0
        val len = buffer.length
        while (count < len) {
          val written = to.write(buffer, count, true).get
          count += written
        }

      }
      def nonBlockingWrite(message: Message): Boolean = {
        val buffer = Message.toFrame(message)
        var count = 0
        val len = buffer.length
        var notReady = false
        while (count < len && !notReady) {
          val written = to.write(buffer, count, false)
          if (written.isEmpty) {
            if (count == 0) {
              notReady = true
            } else {
              println("continue to write")
            }
          }
          count += written.getOrElse(0)
        }
        if (notReady) false
        else true

      }
    }
  }

}
