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
  // type PipeT = Ptr[Byte]
  // native.CStruct4[semaphore.sem_t,
  //                 semaphore.sem_t,
  //                 Int,
  //                 native.CArray[Byte, _256]]
  // private val Size_PipeT = native.sizeof[PipeT]

  def allocate(elements: Int, blockSize: Int): Pipe = {
    val capacity = elements * blockSize
    val size = sizeof[semaphore.sem_t] * 2 + sizeof[native.CInt] + capacity
    import mmanconst._
    val ptr = mman
      .mmap(null,
            size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_ANONYMOUS | MAP_POPULATE,
            -1,
            0)

    if (ptr.cast[native.CUnsignedLong] == MAP_FAILED) {
      throw new RuntimeException("mmap failed " + errno.errno + " " + size)
    }

    val pipeT = ptr //.asInstanceOf[Ptr[PipeT]]
    new Pipe(pipeT, capacity, elements, blockSize)
  }

  class Pipe private[babyactors] (private val m: Ptr[Byte],
                                  private val capacity: Int,
                                  private val elements: Int,
                                  private val blockSize: Int) {
    private def semFull: Ptr[semaphore.sem_t] = m.cast[Ptr[semaphore.sem_t]]
    private def semEmpty: Ptr[semaphore.sem_t] =
      (m + sizeof[semaphore.sem_t]).cast[Ptr[semaphore.sem_t]]
    private def bufferSize: Int = m(sizeof[semaphore.sem_t] * 2)
    private def bufferSize_=(c: Int) = {
      val m2 = (m + sizeof[semaphore.sem_t] * 2).cast[Ptr[Int]]
      m2(0) = c
    }
    private def buffer: Ptr[Byte] =
      m + sizeof[semaphore.sem_t] * 2 + sizeof[native.CInt]

    var in = 0
    var out = 0

    if (semaphore.sem_init(semEmpty, 1, elements.toUInt) != 0) {
      throw new RuntimeException("semaphore init fail " + errno.errno)
    }
    if (semaphore.sem_init(semFull, 1, 0.toUInt) != 0) {
      throw new RuntimeException("semaphore init fail " + errno.errno)
    }
    bufferSize = 0

    private def wait(sem: Ptr[semaphore.sem_t], block: Boolean): Boolean = {
      if (block) {
        var i = 0
        val max = 100000
        var go = false
        // sem_trywait keeps in userspace, therefore it is worth to spin a while
        while (!go && i < max) {
          val r = semaphore.sem_trywait(sem)
          if (r == 0) {
            go = true
          }
          i += 1
        }
        if (!go) {
          val r = semaphore.sem_wait(sem)
          if (r == -1 && errno.errno == posix.errno.EINTR)
            throw new RuntimeException("interrupt")
        }
        false
      } else {
        val r = semaphore.sem_trywait(sem)
        if (r == -1 && errno.errno == posix.errno.EINTR)
          throw new RuntimeException("interrupt")
        else if (r == -1 && errno.errno == posix.errno.EAGAIN)
          true
        else false
      }
    }

    def write(data: Array[Byte], offset: Int, block: Boolean): Option[Int] = {
      val quit = wait(semEmpty, block)
      if (quit) None
      else {

        val copied = scala.math.min(data.length - offset, blockSize)

        memcpy(dest = buffer,
               src = data.asInstanceOf[runtime.ByteArray].at(offset),
               count = copied)
        bufferSize = copied
        semaphore.sem_post(semFull)

        Some(copied)
      }

    }

    def read(data: Array[Byte], offset: Int, block: Boolean): Option[Int] = {
      val quit = wait(semFull, block)

      if (quit) None
      else {

        if (errno.errno == posix.errno.EINTR)
          throw new RuntimeException("interrupt")

        val copied = bufferSize
        if (offset + copied >= data.length) {
          throw new RuntimeException("buffer too small")
        }

        memcpy(dest = data.asInstanceOf[runtime.ByteArray].at(offset),
               src = buffer,
               count = copied)
        semaphore.sem_post(semEmpty)
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
  val MAP_ANONYMOUS = 32
  val MAP_POPULATE = 131072
  import native._
  val MAP_FAILED = -1L.toULong
}

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
