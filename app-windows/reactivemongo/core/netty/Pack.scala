package reactivemongo.core.netty

import reactivemongo.io.netty.channel.{ Channel, EventLoopGroup }
import reactivemongo.io.netty.channel.nio.NioEventLoopGroup
import reactivemongo.io.netty.channel.socket.nio.NioSocketChannel
import reactivemongo.util.LazyLogger

private[core] final class Pack(
    val eventLoopGroup: () => EventLoopGroup,
    val channelClass: Class[_ <: Channel]
):
  override def equals(that: Any): Boolean = that match
    case other: Pack => other.channelClass.getName == channelClass.getName
    case _ => false

  override def hashCode: Int = channelClass.getName.hashCode

  override def toString = s"NettyPack(${channelClass.getName})"

private[core] object Pack:

  private val logger = LazyLogger("reactivemongo.core.netty.Pack")

  def apply(): Pack =
    val pack = new Pack(() => new NioEventLoopGroup(), classOf[NioSocketChannel])
    logger.info(s"Instantiated $pack")
    pack
