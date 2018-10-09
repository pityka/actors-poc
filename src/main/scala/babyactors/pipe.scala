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
  def allocate(elements: Int, blockSize: Int): Pipe = {
    val size = sizeof[semaphore.sem_t] * 3 + elements * blockSize + elements * sizeof[
      native.CInt]
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

    val pipeT = ptr
    new Pipe(pipeT, elements, blockSize)
  }

  class Buffer(private val m: Ptr[Byte]) extends AnyVal {
    def size: Int = m.cast[Ptr[Int]].apply(0)
    def size_=(c: Int) = (m.cast[Ptr[Int]])(0) = c
    def buffer = (m + sizeof[native.CInt])
    def write(data: Array[Byte], offset: Int, blockSize: Int): Int = {
      val copied = scala.math.min(data.length - offset, blockSize)

      memcpy(dest = buffer,
             src = data.asInstanceOf[runtime.ByteArray].at(offset),
             count = copied)
      size = copied
      copied
    }

    def read(data: Array[Byte], offset: Int, blocksize: Int): Int = {
      val copied = size
      if (offset + copied >= data.length) -1
      else {
        memcpy(dest = data.asInstanceOf[runtime.ByteArray].at(offset),
               src = buffer,
               count = copied)
        copied
      }

    }

  }

  class Pipe private[babyactors] (private val m: Ptr[Byte],
                                  private val elements: Int,
                                  val blockSize: Int) {
    private def semFull: Ptr[semaphore.sem_t] = m.cast[Ptr[semaphore.sem_t]]
    private def semEmpty: Ptr[semaphore.sem_t] =
      (m + sizeof[semaphore.sem_t]).cast[Ptr[semaphore.sem_t]]
    private def semLock: Ptr[semaphore.sem_t] =
      (m + 2 * sizeof[semaphore.sem_t]).cast[Ptr[semaphore.sem_t]]

    private def slot(i: Int) =
      new Buffer(
        m + sizeof[semaphore.sem_t] * 3 + i * (sizeof[native.CInt] + blockSize))

    var in = 0
    var out = 0

    (0 until elements).foreach { i =>
      slot(i).size = 0
    }

    if (semaphore.sem_init(semEmpty, 1, elements.toUInt) != 0) {
      throw new RuntimeException("semaphore init fail " + errno.errno)
    }
    if (semaphore.sem_init(semFull, 1, 0.toUInt) != 0) {
      throw new RuntimeException("semaphore init fail " + errno.errno)
    }
    if (semaphore.sem_init(semLock, 1, 1.toUInt) != 0) {
      throw new RuntimeException("semaphore init fail " + errno.errno)
    }

    def write(data: Array[Byte], offset: Int, block: Boolean): Option[Int] = {
      val quit = wait(semEmpty, block)
      if (quit) None
      else {

        wait(semLock, true)
        val copied = slot(in).write(data, offset, blockSize)
        in += 1
        if (in == elements) {
          in = 0
        }

        semaphore.sem_post(semLock)
        semaphore.sem_post(semFull)

        Some(copied)
      }

    }

    def read(data: Array[Byte], offset: Int, block: Boolean): Option[Int] = {
      val quit = wait(semFull, block)

      if (quit) None
      else {

        wait(semLock, true)
        val copied = slot(out).read(data, offset, blockSize)
        out += 1
        if (out == elements) {
          out = 0
        }

        semaphore.sem_post(semLock)
        semaphore.sem_post(semEmpty)
        Some(copied)

      }
    }

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
