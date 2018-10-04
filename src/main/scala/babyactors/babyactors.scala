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
  private val multiplex = Impl.createRoot(message)
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

class Dispatcher(private val pipeIn: Pipe2.ShPipeReadEnd,
                 private val pipeUp: Option[Pipe2.ShPipeWriteEnd],
                 val self: DispatcherRef,
                 private val pipeInWrite: Option[Pipe2.ShPipeWriteEnd]) {
  def root = pipeUp.isEmpty
  private val actors = scala.collection.mutable.Map[ActorRef, Actor]()
  private val knownDispatchers =
    scala.collection.mutable.Map[DispatcherRef, Pipe2.ShPipeWriteEnd]()

  private def next = pipeIn.blockRead

  def send(message: Message): Unit =
    knownDispatchers.get(message.recipient.dispatcher) match {
      case Some(writeEnd)           => writeEnd.blockWrite(message)
      case None if pipeUp.isDefined => pipeUp.get.blockWrite(message)
      case None if message.recipient.dispatcher == self =>
        pipeInWrite.get.blockWrite(message)
      case None =>
        // println(self + " create " + message.recipient.dispatcher)
        val writeEnd = Impl
          .createDispatcher(message.recipient.dispatcher, pipeInWrite.get)
        knownDispatchers.update(message.recipient.dispatcher, writeEnd)
        writeEnd.blockWrite(message)
    }

  def actorReceive(message: Message) = {
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

  while (true) {
    val message = next
    // println(self + " " + message)
    val recipient = message.recipient
    val dispatcher = recipient.dispatcher
    if (pipeUp.isDefined && dispatcher != self)
      throw new RuntimeException("routing error")
    else {
      if (dispatcher == self) actorReceive(message)
      else {
        send(message)
      }
    }

  }
}

private[babyactors] object Impl {

  private object Helpers {

    def killall(pids: Seq[Int]) =
      pids.foreach(pid => signal.kill(pid, signal.SIGTERM))
  }
  import Helpers._

  /**  Static field to hold a set of pids
    */
  var pids = List[Int]()

  def createRoot(firstMessage: Message): Unit = {

    val pipe = babyactors.Pipe.allocate(7, 512)
    val read = Pipe2.ShPipeReadEnd(pipe)
    val write = Pipe2.ShPipeWriteEnd(pipe)
    write.blockWrite(firstMessage)

    def handler(s: Int) = s match {
      case s if s == signal.SIGTERM => killall(pids)
      case _                        => println("signal " + signal)
    }

    signal.signal(signal.SIGTERM, CFunctionPtr.fromFunction1(handler))

    val dispatcher = new Dispatcher(read, None, DispatcherRef(0), Some(write))

    throw new RuntimeException("multiplex.loop never returns")
  }

  def createDispatcher(ref: DispatcherRef,
                       pipeUp: Pipe2.ShPipeWriteEnd): Pipe2.ShPipeWriteEnd = {
    val pipe = babyactors.Pipe.allocate(7, 512)
    val read = Pipe2.ShPipeReadEnd(pipe)
    val write = Pipe2.ShPipeWriteEnd(pipe)

    val forkedPid = unistd.fork()
    if (forkedPid == -1) throw new RuntimeException("fork failed")
    else if (forkedPid == 0) {
      // child

      new Dispatcher(read, Some(pipeUp), ref, None)

      throw new RuntimeException("fork child never returns")
    } else {
      // parent

      pids = forkedPid :: pids
      write

    }
  }

}

object Pipe2 {
  case class ShPipeReadEnd(from: babyactors.Pipe.Pipe) {
    val buffer = Array.ofDim[Byte](1024)
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
