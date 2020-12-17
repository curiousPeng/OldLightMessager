# LightMessager
一个针对RabbitMQ的简单封装类

### 基本概念

##### exchange
一条消息的转发是由*exchange*的类型和具体的转发规则（*bindings*）共同决定的
+ **类型**
    + ***direct***：msg的routekey跟queue绑定的routekey一致，则直接转发
    + ***fanout***：忽略routekey，挂多少个queue就转发多少个msg，类似于广播
    + ***topic***：可以看成是direct的高级版本，因为这里routekey不光是一致就转发，还可以是满足某种pattern就可以转发
    + ***headers***：忽略routekey，更高级的一种形式

+ **重要的属性**
    + *name*
    + *durability*（一个durable的exchange可以从broker的重启中存活下来）
    + *auto-delete* 
    + *arguments*（optional; used by plugins and broker-specific features such as message TTL, queue length limit, etc）

##### queue
+ **重要的属性**
    + *name*
    + *durability*（一个durable的queue可以从broker的重启中存活下来）
    + *auto-delete*
    + *arguments*

##### message
+ **可设置的属性**
    + *Content type*
    + *Content encoding* 
    + *Routing key* 
    + *Delivery mode*（persistent or not）
    + *Message priority* 
    + *Message publishing timestamp*
    + *Expiration period*
    + *Publisher application id*等
  
+ 将一条消息发送至一个durable的queue并不能使该消息persistent，唯一能决定一条消息是否persistent的是其delivery mode属性值
  
+ 同时注意mandatory和persistent的区别，前者会在消息无法送达的情况下触发basic.return事件，后者则是会让消息持久化至磁盘


### 线程模型
在线程模型上，主要考虑两个类型`Connection`和`Channle`

在当前版本的c#客户端中，一个connection对应着一个线程，抽象的是一个tcp连接，而多个channel通过多路复用共用一个connection

一种常见的策略是应用层面一个线程分配一个独立的channel通过共用一个connection与服务端进行交互

> Connections and Channels are meant to be long-lived. 

在channel层面更容易出现各种问题导致channel关闭（connection相对来说要稳定一些），因此考虑对channel池化进而达到复用的目的比较可取

### 自动恢复

rabbitmq自带有automatic recovery特性，能在网络发生异常时进行自我恢复。这包括连接的恢复和网络拓扑（topology）（queues、exchanges、bindings and consumers）的恢复（注意）。

通常来说可能导致automatic recovery的事件有：
+ An I/O exception is thrown in connection's I/O loop
+ A socket read operation times out
+ Missed server heartbeats are detected
+ Any other unexpected exception is thrown in connection's I/O loop

注意，任何channel-level的异常并不会导致recovery动作，这是因为它被当作是应用层面的异常来对待了。

rabbitmq对于connection的恢复有一定的缺陷：
+ 首先，
    > When a connection is down or lost, it takes time to detect.

    一个失效的连接需要一定的时间才能被发现，因此在这段时间中发送的消息就需要额外的手段来保证其不被丢失（*publisher confirms*）

+ 其次，
    > recovery begins after a configurable delay
    
    rabbitmq默认情况下每隔5秒重试一次恢复连接，重试的时候如果试图发送一条消息，这将会触发一个exception，应用层可能需要处理该异常以保证发送的消息不会丢失掉（也建议配合*publisher confirms*来做处理）

### 可靠的消息送达
两个方向上的投递需要考虑可靠送达：
+ **publisher -> broker**
  + 设置durability for exchanges, queues and persistent for messages
  
  + 使用rabbitmq自带的*publisher confirms*机制，配合上一定的消息重发策略，比如：
	+ resend if your connection is lost or some other crash occurs before you receive confirmation of receipt

	+ 超时机制。请注意此时多次重试的超时时间间隔最好可以double一下，以避免dos攻击
  
  + 一种情况是，broker拒绝了该条消息（basic.nack），这表明broker因为某种原因当前不能处理该条消息，并拒绝为该条消息的发送负责。此时，需要publisher自己来负责该条消息的后续处理，可能重发，也可能就此作废等。

+ **broker -> consumer**
  这个方向上可以使用*consumer ack*机制，要小心的是，
    > Acknowledgements with stale delivery tags will not be sent. Applications that use manual acknowledgements and automatic recovery must be capable of handling redeliveries.

    + rabbitmq会针对重传的消息设置`redelivered`标志值，这意味着consumer可能在之前有处理过该条消息；为什么是可能，原因在于该条消息可能刚发送出去，还在传递过程中，整个连接就断开了！

    + 网络异常的情况中连接执行recovery，此时delivery tags都会过期失效，rabbitmq客户端并不会发送带有过期tag的ack消息。这又会进一步导致rabbitmq broker重发所有没有收到ack确认的消息，因此consumer一定要能够处理重复达到的消息才行

	+ rabbitmq提供了一个属性值`redelivered`，用它来标识一条消息是否是重传的。但不幸的是如前面所说，当该属性值为`true`的时候并不能说明客户端已经处理过该条消息；不过可以确信的是如果值为`false`，那么这条消息一定没有被处理过！

	+ 采用nack告知rabbitmq node时要注意`requeue/redelivery loop`这种情况的发生，常见的解决办法是跟踪当前消息的`redelivery`次数，要么做延迟requeueing，要么标记状态直接丢弃

### Roadmap
+ 可靠消息投递
+ 延时发送
+ 效率改进
	+ QoS, multiple



















