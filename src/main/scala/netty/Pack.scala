package lila.ws
package netty

import io.netty.channel.{ EventLoopGroup, ServerChannel }
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel

import scala.util.control.NonFatal

// Adapted from https://github.com/ReactiveMongo/ReactiveMongo/blob/5560470ee409da827feee161c16631042e194263/driver/src/main/scala/core/netty/Pack.scala#L15
private final class Pack(
    val eventLoopGroupFactory: Int => EventLoopGroup,
    val channelClass: Class[? <: ServerChannel]
)

private object Pack:
  private val kqueuePkg: String = "io.netty.channel.kqueue"
  private val epollPkg: String = "io.netty.channel.epoll"

  def instance: Pack =
    List(epoll, kqueue).flatten.find(usable).getOrElse(nio)

  private def usable(pack: Pack): Boolean =
    try
      pack.eventLoopGroupFactory(1).shutdownGracefully()
      true
    catch
      case _: Throwable =>
        false

  private def nio: Pack =
    new Pack(nThreads => new NioEventLoopGroup(nThreads), classOf[NioServerSocketChannel])

  private def epoll: Option[Pack] =
    try
      Some(Class.forName(s"${epollPkg}.EpollServerSocketChannel")).map { cls =>
        val chanClass = cls.asInstanceOf[Class[? <: ServerChannel]]
        val groupClass = Class
          .forName(s"${epollPkg}.EpollEventLoopGroup")
          .asInstanceOf[Class[? <: EventLoopGroup]]

        val groupCtor = groupClass.getDeclaredConstructor(classOf[Int])
        new Pack(
          nThreads => groupCtor.newInstance(Int.box(nThreads)),
          chanClass
        )
      }
    catch
      case NonFatal(_) =>
        None

  private def kqueue: Option[Pack] =
    try
      Some(Class.forName(s"${kqueuePkg}.KQueueServerSocketChannel")).map { cls =>
        val chanClass = cls.asInstanceOf[Class[? <: ServerChannel]]
        val groupClass = Class
          .forName(s"${kqueuePkg}.KQueueEventLoopGroup")
          .asInstanceOf[Class[? <: EventLoopGroup]]
        val groupCtor = groupClass.getDeclaredConstructor(classOf[Int])
        new Pack(
          nThreads => groupCtor.newInstance(Int.box(nThreads)),
          chanClass
        )
      }
    catch
      case NonFatal(_) =>
        None
