package babyactors

import scalanative.posix.{unistd, fcntl}
import scalanative.posix
import scalanative.posix.fcntl._
import scala.scalanative.native.{signal, CFunctionPtr, errno, Ptr, stdlib}
import scala.scalanative.native
import scala.scalanative.native.string.memcpy
import scalanative.runtime
import scala.scalanative.posix.sys.{select, time, timeOps}
import scalanative.posix.sys.stat._
import scalanative.posix.sys.types._

object Pipe {
  import native._
  import Nat._
  type _1024 = Digit[_1, Digit[_0, Digit[_2, _4]]]
  val capacity = 1024
  type PipeT =
    native.CStruct4[semaphore.sem_t,
                    semaphore.sem_t,
                    Int,
                    native.CArray[Byte, _1024]]
  private val Size_PipeT = native.sizeof[PipeT]
  println("size of pipet" + Size_PipeT)

  def deallocate(filename: String) = native.Zone { implicit z =>
    mman.shm_unlink(native.toCString(filename))
  }

  def allocate(filename: String): Pipe = native.Zone { implicit z =>
    val fd =
      mman.shm_open(native.toCString(filename),
                    O_CREAT | O_RDWR | O_CREAT,
                    S_IRUSR | S_IWUSR)
    if (fd == -1) {
      throw new RuntimeException("failed shm_open " + errno.errno)
    }
    val e = unistd.ftruncate(fd, Size_PipeT)
    println("ftruncate" + e)
    import mmanconst._
    val ptr = mman
      .mmap(null, Size_PipeT, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
    println("mmap" + errno.errno)
    unistd.close(fd)
    val pipeT = ptr.asInstanceOf[Ptr[PipeT]]
    new Pipe(pipeT)
  }

  // def open(filename: String): Pipe = native.Zone { implicit z =>
  //   val fd =
  //     mman.shm_open(native.toCString(filename), O_RDWR, S_IRUSR | S_IWUSR);
  //   import mmanconst._
  //   val pipeT: Ptr[PipeT] = mman
  //     .mmap(null, Size_PipeT, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
  //     .asInstanceOf[Ptr[PipeT]]
  //   unistd.close(fd);
  //   new Pipe(pipeT)

  // }

  class Pipe(private val m: Ptr[PipeT]) {
    private def semFull = m._1
    private def semEmpty = m._2
    private def bufferSize = !m._3
    private def bufferSize_=(c: Int) = !m._3 = c
    private def buffer: Ptr[Byte] = (m._4)._1

    println(
      "initialized semaphores " + semaphore.sem_init(semEmpty, 1, 1.toUInt))
    println(
      "initialized semaphores " + semaphore.sem_init(semFull, 1, 0.toUInt))

    private def wait(sem: Ptr[semaphore.sem_t], block: Boolean): Boolean = {
      if (block) {
        println("wait 1")

        val r = semaphore.sem_wait(sem)
        println("wait 2")
        if (r == -1 && errno.errno == posix.errno.EINTR)
          throw new RuntimeException("interrupt")

        false
      } else {
        println("wait 4")
        val r = semaphore.sem_trywait(sem)
        println("wait 5")
        if (r == -1 && errno.errno == posix.errno.EINTR)
          throw new RuntimeException("interrupt")
        else if (r == -1 && errno.errno == posix.errno.EAGAIN)
          true
        else false
      }
    }

    def write(data: Array[Byte], offset: Int, block: Boolean): Option[Int] = {
      println("w 1")
      val quit = wait(semEmpty, block)
      println("w 2")
      if (quit) None
      else {
        println("w 3")

        val copied = scala.math.min(data.length - offset, capacity)
        println("w 3a" + copied)

        memcpy(dest = buffer,
               src = data.asInstanceOf[runtime.ByteArray].at(offset),
               count = copied)
        println("w 3b")
        bufferSize = copied
        println("w 4")
        println("w 4a")
        println(buffer(0))
        println(buffer(1))
        println("w signal")
        semaphore.sem_post(semFull)
        println("w 5")

        Some(copied)
      }

    }

    def read(data: Array[Byte], offset: Int, block: Boolean): Option[Int] = {
      println("r 1")
      val quit = wait(semFull, block)
      println("r 2")

      if (quit) None
      else {
        println("r 3")

        if (errno.errno == posix.errno.EINTR)
          throw new RuntimeException("interrupt")

        val copied = bufferSize
        if (offset + copied >= data.length) {
          throw new RuntimeException("buffer too small")
        }
        println("r 4")

        memcpy(dest = data.asInstanceOf[runtime.ByteArray].at(offset),
               src = buffer,
               count = copied)
        println("r 5")
        println(data.deep)
        semaphore.sem_post(semEmpty)
        println("r 6")

        Some(copied)

      }
    }

  }

}

object mmanconst {
  val PROT_NONE = 0
  val PROT_READ = 1
  val PROT_WRITE = 2
  val PROT_EXEC = 4
  val MAP_SHARED = 1
}

// @native.link("rt")
@native.extern
object mman {

  def shm_open(name: native.CString,
               oflag: native.CInt,
               mode: mode_t): native.CInt =
    native.extern
  def shm_unlink(name: native.CString): native.CInt = native.extern

  def mmap(addr: Ptr[Byte],
           length: size_t,
           prot: native.CInt,
           flags: native.CInt,
           fd: native.CInt,
           offset: off_t): Ptr[Byte] = native.extern
}

@native.link("pthread")
@native.extern
object semaphore {
  import native._
  import Nat._
  type _32 = Digit[_3, _2]
  type sem_t = native.CArray[Byte, _32]
  def sem_wait(sem: Ptr[sem_t]): native.CInt = native.extern
  def sem_trywait(sem: Ptr[sem_t]): native.CInt = native.extern
  def sem_post(sem: Ptr[sem_t]): native.CInt = native.extern
  def sem_init(sem: Ptr[sem_t],
               pshared: native.CInt,
               value: native.CUnsignedInt): native.CInt = native.extern
}
