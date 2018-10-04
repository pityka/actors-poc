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
  def fromFramePayload(frame: Array[Byte], length: Int): Message = {
    val buffer = java.nio.ByteBuffer.wrap(frame)
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
  private val localMailbox = scala.collection.mutable.Queue[Message]()

  private def next =
    localMailbox.dequeueFirst(_ => true).getOrElse(multiplex.blockRead)

  def send(message: Message) =
    if (actors.contains(message.recipient)) localMailbox.enqueue(message)
    else multiplex.blockWrite(message)

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

  case class DispatcherPipes(pipes: NonBlockingPipes)

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
    val sendBuffer = Queue[(Message, NonBlockingPipes)]()
    val actors = ListBuffer[(ActorRef, DispatcherPipes)]()

    def dispatchers =
      actors.map { case (actorRef, pipes) => (actorRef.dispatcher, pipes) }.distinct

    def allPipes =
      actors.iterator.map(_._2.pipes)

    def read(pipe: NonBlockingPipes) =
      pipe.nonBlockingRead.foreach { msg =>
        buffer.enqueue(msg)
      }

    def readAll() =
      selectedReads.foreach(read)

    def tryWrite(message: Message, pipes: NonBlockingPipes) = {
      val success = pipes.nonBlockingWrite(message)
      if (!success) {
        sendBuffer.enqueue((message, pipes))
      }
    }

    def createDispatcher: DispatcherPipes = {
      val pipeFromChild = mkPipe
      val pipeToChild = mkPipe

      val forkedPid = unistd.fork()
      if (forkedPid == -1) throw new RuntimeException("fork failed")
      else if (forkedPid == 0) {
        // child
        unistd.close(pipeFromChild(0))
        unistd.close(pipeToChild(1))

        new Dispatcher(
          multiplex = MultiplexPipes(pipeTo = pipeFromChild(1),
                                     pipeFrom = pipeToChild(0)))

        throw new RuntimeException("fork child never returns")
      } else {
        // parent

        unistd.close(pipeFromChild(1))
        unistd.close(pipeToChild(0))

        val fdFrom = pipeFromChild(0)
        val fdTo = pipeToChild(1)

        val pipes = NonBlockingPipes(to = fdTo, from = fdFrom)
        Multiplex.pids = forkedPid :: Multiplex.pids
        DispatcherPipes(pipes)

      }
    }

    def processMessage(m: Message) = m match {
      case Message(recipient, _) =>
        actors.find(_._1 == recipient) match {
          case Some((_, dispatcher)) => tryWrite(m, dispatcher.pipes)
          case None =>
            dispatchers.find(_._1 == recipient.dispatcher) match {
              case Some((_, dispatcherPipes)) =>
                actors.append((recipient, dispatcherPipes))
                tryWrite(m, dispatcherPipes.pipes)
              case None =>
                val newPipes = createDispatcher
                actors.append((recipient, newPipes))
                tryWrite(m, newPipes.pipes)
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

    def selectedReads = allPipes.filter { pipe =>
      select.FD_ISSET(pipe.from, readSet) > 0
    }

    def selectIo(): Unit = {
      val watchedReads = allPipes.map(_.from).toList
      val watchedWrites = sendBuffer.toList.map(_._2.to).toList

      select.FD_ZERO(readSet)
      select.FD_ZERO(writeSet)
      watchedReads.foreach(fd => select.FD_SET(fd, readSet))
      watchedWrites.foreach(fd => select.FD_SET(fd, writeSet))
      val maxFD = math.max(if (watchedReads.isEmpty) 0 else watchedReads.max,
                           if (watchedWrites.isEmpty) 0 else watchedWrites.max)
      select.select(maxFD + 1, readSet, writeSet, null, null)

    }

    def loop = while (true) {
      tryWriteAll()
      processAll()
      selectIo()
      readAll()
    }

  }

  case class MultiplexPipes(pipeTo: Int, pipeFrom: Int) {
    val is = new LibCReadInputStream(pipeFrom, 100)
    def blockRead: Message = {
      val length = java.nio.ByteBuffer.allocate(4)
      length.put(is.read.toByte)
      length.put(is.read.toByte)
      length.put(is.read.toByte)
      length.put(is.read.toByte)
      length.rewind
      val l = length.getInt
      val buffer = Array.ofDim[Byte](l)
      var count = is.read(buffer)
      while (count < buffer.length) {
        count += is.read(buffer, count, buffer.length - count)
      }
      Message.fromFramePayload(buffer, l)
    }
    def blockWrite(m: Message) = {
      val frame = Message.toFrame(m)
      val os = new LibCWriteOutputStream(pipeTo)
      os.write(frame, 0, frame.length)
    }
  }

  case class NonBlockingPipes(to: Int, from: Int) {

    def setNonblock(fd: Int) = {
      val currentFlags = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
      fcntl.fcntl(fd, fcntl.F_SETFL, currentFlags | fcntl.O_NONBLOCK)
    }

    setNonblock(to)
    setNonblock(from)

    val lengthBuffer = Array.ofDim[Byte](4)
    var payloadBuffer = Array.ofDim[Byte](1024)

    def nonBlockingRead: Option[Message] = {

      def readFully(array: Array[Byte], offset: Int, len: Int) = {
        var count = offset
        var ref = array.asInstanceOf[runtime.ByteArray].at(0)

        while (count < len) {
          val c = unistd.read(from, ref, len - count)
          if (c < 0) {
            if (errno.errno == posix.errno.EWOULDBLOCK) {
              println(s"continue read from $from")
            } else {
              throw new RuntimeException(errno.toString)
            }

          }
          count += math.max(0, c)
          ref += math.max(0, c)
        }
      }
      val lRef = lengthBuffer.asInstanceOf[runtime.ByteArray].at(0)
      val count = unistd.read(from, lRef, 4)
      if (count < 0) None
      else {
        readFully(lengthBuffer, count, 4)
        val length = java.nio.ByteBuffer.wrap(lengthBuffer).getInt
        if (payloadBuffer.size < length) {
          payloadBuffer = Array.ofDim[Byte](length)
        }
        readFully(payloadBuffer, 0, length)
        Some(Message.fromFramePayload(payloadBuffer, length))
      }

    }
    def nonBlockingWrite(message: Message): Boolean = {
      val buffer = Message.toFrame(message)
      var count = 0
      var ref = buffer.asInstanceOf[runtime.ByteArray].at(0)
      val len = buffer.length
      var notReady = false
      while (count < len && !notReady) {
        val written = unistd.write(to, ref, len - count)
        if (written == -1) {
          if (count == 0) {
            notReady = true
          } else {
            println("continue to write")
          }
        }
        ref = ref + written
        count += written
      }
      if (notReady) false
      else true

    }
  }

  class LibCWriteOutputStream(fileDescriptor: Int)
      extends java.io.OutputStream {

    override def close = {
      unistd.close(fileDescriptor)
    }

    override def write(buffer: Array[Byte], off: Int, len: Int): Unit = {
      var count = 0
      var ref = buffer.asInstanceOf[runtime.ByteArray].at(off)
      while (count < len) {
        val written = unistd.write(fileDescriptor, ref, len - count)
        if (written == -1) { throw new RuntimeException("write failed") }
        ref = ref + written
        count += written
      }
    }

    def write(b: Int): Unit = {
      val _ = b
      throw new RuntimeException("use write(byte[] b)")
    }

  }

  class LibCReadInputStream(fileDescriptor: Int, bufferSize: Int)
      extends java.io.InputStream {
    private val buffer = Array.ofDim[Byte](bufferSize)
    private var indexInBuffer = buffer.size
    private var maxIndex = buffer.size - 1
    override def close = unistd.close(fileDescriptor)
    def read: Int =
      if (indexInBuffer > maxIndex) {
        val ref = buffer.asInstanceOf[runtime.ByteArray].at(0)
        val count = unistd.read(fileDescriptor, ref, buffer.length)
        if (count < 0) throw new java.io.IOException("")
        else if (count == 0) {
          -1
        } else {
          maxIndex = count - 1
          indexInBuffer = 1
          buffer(0) & 0xFF
        }
      } else {
        val r = buffer(indexInBuffer)
        indexInBuffer += 1
        r & 0xFF
      }
  }

}
