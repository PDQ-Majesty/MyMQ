package com.pdq.mq.mqserver;

import com.pdq.mq.common.*;
import com.pdq.mq.mqserver.core.BasicProperties;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Description: 这个类表示 TCP服务器
 * @Auther: pdq
 */
public class BrokerServer {
    //接收客户端的连接
    private ServerSocket serverSocket = null;
    //虚拟主机
    private VirtualHost virtualHost = new VirtualHost("default");
    //此处的 key 是 channelId, value 为对应的 Socket 对象
    private ConcurrentHashMap<String, Socket> sessions = new ConcurrentHashMap<>();
    //每个客户端连接复用一个 DataOutputStream.
    //该流会被"请求响应线程"与"消息推送线程池的多个线程"并发写入,
    //必须配合 synchronized(流对象) 保证 type+length+payload 三段的原子性, 否则帧会交错撕裂
    private ConcurrentHashMap<Socket, DataOutputStream> outputStreams = new ConcurrentHashMap<>();

    //使用线程池来处理多个客户端的连接
    private ExecutorService workPool = null;
    //这个属性来控制服务器的启停
    //使用volatile修饰符,保证线程之间的可见性
    private volatile boolean runnable = true;

    public BrokerServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    //启动服务
    public void start() {
        System.out.println("[BrokerServer] 启动!");
        workPool = Executors.newCachedThreadPool();
        try {
            while (runnable) {
                //接收客户端的连接
                Socket clientSocket = serverSocket.accept();
                //把处理连接的逻辑丢给线程池
                workPool.submit(() -> {
                    processConnection(clientSocket);
                });
            }
        } catch (Exception e) {
            System.out.println("[BrokerServer] 服务器终止运行!");
        }

    }

    //终止服务,主要用来进行单元测试
    public void stop() throws IOException {
        runnable = false;
        //把线程池中的任务都放弃. 让线程都销毁.
        serverSocket.close();
        workPool.shutdownNow();
    }

    private void processConnection(Socket clientSocket) {
        // 解析客户端的请求,拿到request中的payload(即请求的参数)
        try (InputStream inputStream = clientSocket.getInputStream();
             OutputStream outputStream = clientSocket.getOutputStream()) {
            //这里需要按照特定格式进行解析,使用DataOutputStream和DataInputSteam进行四个字节的解析
            try (DataInputStream dataInputStream = new DataInputStream(inputStream);
                 DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                //登记共享的输出流, 使推送线程池与响应写入使用同一个流对象(加锁同一把锁)
                outputStreams.put(clientSocket, dataOutputStream);
                //一个TCP连接可能有很多请求和响应,需要不断处理请求和响应
                while (true) {
                    //解析请求
                    Request request = readRequest(dataInputStream);
                    //计算请求
                    Response response = process(request, clientSocket);
                    //发送响应
                    writeResponse(response, dataOutputStream);
                }
            }
        } catch (EOFException | SocketException e) {
            // 对于这个代码, DataInputStream 如果读到 EOF , 就会抛出一个 EOFException 异常.
            // 需要借助这个异常来结束循环
            System.out.println("[BrokerServer] connection 关闭! 客户端的地址: " + clientSocket.getInetAddress().toString()
                    + ":" + clientSocket.getPort());
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("[BrokerServer] 连接异常断开!");
        } finally {
            //连接结束, 移除共享输出流登记
            outputStreams.remove(clientSocket);
            //连接处理完成,关闭socket
            try {
                clientSocket.close();
                // 一个 TCP 连接中, 可能包含多个 channel. 需要把当前这个 socket 对应的所有 channel 也顺便清理掉.
                clearClosedSession(clientSocket);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("[BrokerServer] socket关闭失败!");
            }
        }

    }

    private void clearClosedSession(Socket clientSocket) {
        //清理当前socket的所有channel
        //记录当前socket的所有channelId
        List<String> toDeleteChannelId = new ArrayList<>();
        for(Map.Entry<String,Socket> entry : sessions.entrySet()) {
            if(entry.getValue() == clientSocket) {
                toDeleteChannelId.add(entry.getKey());
            }
        }
        for (String channelId: toDeleteChannelId) {
            sessions.remove(channelId);
        }
        System.out.println("[BrokerServer] 当前socket所有channel清除完成");
    }

    private Request readRequest(DataInputStream dataInputStream) throws IOException {
        int type = dataInputStream.readInt();
        int length = dataInputStream.readInt();
        byte[] payload = new byte[length];
        dataInputStream.readFully(payload);
        //构建Request对象
        Request request = new Request();
        request.setType(type);
        request.setLength(length);
        request.setPayload(payload);
        return request;
    }

    private Response process(Request request, Socket clientSocket) throws IOException, ClassNotFoundException {
        BasicArguments basicArguments = (BasicArguments) BinaryTool.fromBytes(request.getPayload());
        boolean ret;
        try {
            ret = dispatch(request, basicArguments, clientSocket);
        } catch (RuntimeException e) {
            //非受检异常(ClassCastException/NPE 等)若向上逃逸, 服务端就不会写出响应,
            //而客户端 waitResult() 没有超时机制, 会永久阻塞在等待响应上.
            //这里兜底捕获并回一个 ok=false 的响应, 保证任何非法请求都不会挂死客户端.
            e.printStackTrace();
            System.out.println("[BrokerServer] 请求处理异常! type=" + request.getType());
            ret = false;
        }
        //构造响应
        BasicReturns basicReturns = new BasicReturns();
        basicReturns.setChannelId(basicArguments.getChannelId());
        basicReturns.setRid(basicArguments.getRid());
        basicReturns.setOk(ret);
        Response response = new Response();
        byte[] payload = BinaryTool.toBytes(basicReturns);
        response.setType(request.getType());
        response.setLength(payload.length);
        response.setPayload(payload);
        System.out.println("[Response] rid=" + basicReturns.getRid() + ", channelId=" + basicReturns.getChannelId()
                + ", type=" + response.getType() + ", length=" + response.getLength());
        return response;
    }

    //把请求分派逻辑独立成方法, 便于在 process 中统一兜底非受检异常
    private boolean dispatch(Request request, BasicArguments basicArguments, Socket clientSocket) throws IOException, ClassNotFoundException {
        boolean ret = true;
        //判断请求类型
        if (request.getType() == 0X1) {
            //创建channel
            sessions.put(basicArguments.getChannelId(), clientSocket);
        } else if (request.getType() == 0x2) {
            //删除channel
            sessions.remove(basicArguments.getChannelId());
        } else if (request.getType() == 0x3) {
            //创建交换机
            ExchangeDeclareArguments arguments = (ExchangeDeclareArguments) basicArguments;
            ret = virtualHost.exchangeDeclare(arguments.getExchangeName(), arguments.getExchangeType(), arguments.isDurable(), arguments.isAutoDelete(), arguments.getArguments());
        } else if (request.getType() == 0x4) {
            //删除交换机
            ExchangeDeleteArguments arguments = (ExchangeDeleteArguments) basicArguments;
            ret = virtualHost.exchangeDelete(arguments.getExchangeName());
        } else if (request.getType() == 0x5) {
            //创建队列
            QueueDeclareArguments arguments = (QueueDeclareArguments) basicArguments;
            ret = virtualHost.queueDeclare(arguments.getQueueName(), arguments.isDurable(),arguments.isAutoDelete(), arguments.isExclusive(), arguments.getArguments());
        } else if (request.getType() == 0x6) {
            //删除队列
            QueueDeleteArguments arguments = (QueueDeleteArguments) basicArguments;
            ret = virtualHost.queueDelete(arguments.getQueueName());
        } else if (request.getType() == 0x7) {
            //创建绑定
            QueueBindArguments arguments = (QueueBindArguments) basicArguments;
            ret = virtualHost.queueBind(arguments.getExchangeName(), arguments.getQueueName(), arguments.getBindingKey());
        } else if (request.getType() == 0x8) {
            //解除绑定: 客户端 Channel.queueUnbind 发送的是 QueueUnbindArguments,
            //它与 QueueBindArguments 是兄弟类(都直接继承 BasicArguments), 不能互相强转
            QueueUnbindArguments arguments = (QueueUnbindArguments) basicArguments;
            ret = virtualHost.queueUnbind(arguments.getExchangeName(), arguments.getQueueName());
        } else if (request.getType() == 0x9) {
            //订阅消息
            BasicPublishArguments arguments = (BasicPublishArguments) basicArguments;
            ret = virtualHost.basicPublish(arguments.getExchangeName(), arguments.getRoutingKey(), arguments.getBasicProperties(), arguments.getBody());
        } else if (request.getType() == 0xa) {
            //消费消息
            BasicConsumeArguments arguments = (BasicConsumeArguments) basicArguments;
            ret = virtualHost.basicConsume(arguments.getConsumeTag(), arguments.getQueueName(), arguments.isAutoAck(), new Consumer() {
                @Override
                public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MqException, IOException {
                    //这个回调函数要做的工作, 就是把服务器收到的消息可以直接推送回对应的消费者客户端
                    //先找到目标客户端
                    //consumerTag其实就是channelId,根据consumerTag的参数值在sessions里查询
                    Socket clientSocket = sessions.get(consumerTag);
                    if (clientSocket == null || clientSocket.isClosed()) {
                        throw new MqException("[BrokerServer] 订阅消息的客户端已经关闭!");
                    }
                    //构造响应参数
                    SubScribeReturns subScribeReturns = new SubScribeReturns();
                    subScribeReturns.setRid("");//由于是发送消息,不需要客户端的请求,只要有订阅的消息就会发送给目标客户端,所以不需要进行匹配
                    subScribeReturns.setOk(true);
                    subScribeReturns.setChannelId(consumerTag);
                    subScribeReturns.setBasicProperties(basicProperties);
                    subScribeReturns.setBody(body);
                    subScribeReturns.setConsumerTag(consumerTag);
                    byte[] payload = BinaryTool.toBytes(subScribeReturns);
                    Response response = new Response();
                    //0xc 表示服务器给消费者客户端推送的消息数据.
                    response.setType(0xc);
                    response.setLength(payload.length);
                    response.setPayload(payload);
                    //复用该连接的共享输出流并加锁:
                    //推送由线程池多个线程并发执行, 与请求响应写入共用同一条 TCP 流,
                    //不保证 type+length+payload 原子写入会导致客户端读到撕裂的帧
                    DataOutputStream sharedStream = outputStreams.get(clientSocket);
                    if (sharedStream == null) {
                        throw new MqException("[BrokerServer] 客户端连接已关闭, 无法推送! consumerTag=" + consumerTag);
                    }
                    synchronized (sharedStream) {
                        writeResponse(response, sharedStream);
                    }
                }
            });
        } else if (request.getType() == 0xb) {
            //手动确认消息
            BasicAckArguments arguments = (BasicAckArguments) basicArguments;
            ret = virtualHost.basicAck(arguments.getQueueName(), arguments.getMessageId());
        } else {
            System.out.println("[BrokerServer] type参数不合法! type=" + request.getType());
        }
        return ret;
    }

    private void writeResponse(Response response, DataOutputStream dataOutputStream) throws IOException {
        //把响应写入目标客户端
        //加锁保证一帧的三段内容原子写出, 与推送线程并发写安全(可重入锁, 外层已加锁时再次进入无碍)
        synchronized (dataOutputStream) {
            dataOutputStream.writeInt(response.getType());
            dataOutputStream.writeInt(response.getLength());
            dataOutputStream.write(response.getPayload());
            //刷新缓冲区
            dataOutputStream.flush();
        }
    }


}
