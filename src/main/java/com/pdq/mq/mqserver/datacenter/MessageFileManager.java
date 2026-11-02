package com.pdq.mq.mqserver.dao;

import com.pdq.mq.common.BinaryTool;
import com.pdq.mq.common.MqException;
import com.pdq.mq.mqserver.core.MSGQueue;
import com.pdq.mq.mqserver.core.Message;

import java.io.*;
import java.util.LinkedList;
import java.util.Scanner;

/**
 * @Description: 这个类是针对Message的存储管理，存储在文件中
 * @Auther: pdq
 */
//Message的信息存储在MSGQueue中
    //队列存储目录：
    //queueName:
    //          queue_data.txt
    //          queue_stat.txt
    //其中queue_data.txt表示message在queue中的数据部分
    //queue_stat.txt表示message在queue中的统计部分（正文长度）
public class MessageFileManager {

    public void init() {
        //这里不做任何操作，只是为了代码风格统一
    }

    public static class Stat {
        public int totalCount;//总消息数
        public int valCount;//有效消息数
    }
    //文件存储需要有存储路径
    public String getQueueDir(String queueName) {
        return "./data/" + queueName;
    }
    public String getQueueDataPath(String queueName) {
        return getQueueDir(queueName)+"/"+"queue_data.txt";
    }
    public String getQueueStatPath(String queueName) {
        return getQueueDir(queueName)+"/"+"queue_stat.txt";
    }

    //Message数据信息的读写&统计信息的读写

    //统计信息的读写
    private Stat readQueueStat(String queueName) throws IOException {
        try(InputStream inputStream = new FileInputStream(getQueueStatPath(queueName))) {
            Stat stat = new Stat();
            Scanner scan = new Scanner(inputStream);
            stat.totalCount = scan.nextInt();
            stat.valCount = scan.nextInt();
            return stat;
        }
    }

    //写操作会覆盖掉原有旧数据
    private void writeQueueStat(String queueName,Stat stat) throws RuntimeException {
        try(OutputStream outputStream = new FileOutputStream(getQueueStatPath(queueName))) {
            PrintWriter writer = new PrintWriter(outputStream);
            writer.write(stat.totalCount+"\t"+stat.valCount);
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //创建队列相应的文件和目录
    public void createQueueFiles(String queueName) throws IOException {
        //1.创建队列目录
        File queueFile = new File(getQueueDir(queueName));
        if(!queueFile.exists()) {
            boolean ok = queueFile.mkdirs();
            if(!ok) {
                System.out.println("创建队列目录失败! queueFile=" +queueFile.getAbsolutePath());
            }
        }
        //2.创建队列数据文件queue_data.txt
        File dataFile = new File(getQueueDataPath(queueName));
        if(!dataFile.exists()) {
            boolean ok = dataFile.createNewFile();
            if(!ok) {
                System.out.println("创建队列目录失败! dataFile=" +dataFile.getAbsolutePath());
            }

        }
        //3.创建队列统计文件queue_stat.txt
        File statFile = new File(getQueueStatPath(queueName));
        if(!statFile.exists()) {
            boolean ok = statFile.createNewFile();
            if(!ok) {
                System.out.println("创建队列目录失败! statFile=" +statFile.getAbsolutePath());
            }
        }
        //4.给消息统计文件设定初始值 0/t0
        Stat stat = new Stat();
        stat.totalCount = 0;
        stat.valCount = 0;
        writeQueueStat(queueName,stat);
    }

    // 删除队列的目录和文件.
    // 队列也是可以被删除的. 当队列删除之后, 对应的消息文件啥的, 自然也要随之删除
    public void destroyQueueFiles(String queueName) {
        //先删除该队列目录下的文件
        File dataFile = new File(getQueueDataPath(queueName));
        boolean ok1 = dataFile.delete();
        File statFile = new File(getQueueStatPath(queueName));
        boolean ok2 = statFile.delete();
        //再删除队列目录
        File queueDir = new File(getQueueDir(queueName));
        boolean ok3 = queueDir.delete();
        if(!ok1||!ok2||!ok3) {
            System.out.println("删除队列目录和文件失败！queueDir="+queueDir.getAbsolutePath());
        }else {
            System.out.println("删除队列目录和文件成功！");
        }
    }

    // 检查队列的目录和文件是否存在.
    // 比如后续有生产者给 broker server 生产消息了, 这个消息就可能需要记录到文件上(取决于消息是否要持久化)
    public boolean checkQueueFilesExists(String queueName) {
        File dirFile = new File(getQueueDir(queueName));
        if(!dirFile.exists()) {
            return false;
        }
        File dataFile = new File(getQueueDataPath(queueName));
        if(!dataFile.exists()) {
            return false;
        }
        File statFile = new File(getQueueStatPath(queueName));
        if(!statFile.exists()) {
            return false;
        }
        return true;
    }

    //生产者向brokerServer发送消息，把消息存在相应队列中,存在文件中
    public void sendMessage(MSGQueue queue, Message message) throws MqException, IOException {

        //判断该队列对应的数据文件是否存在
        if(!checkQueueFilesExists(queue.getName())) {
            //消息数据文件不存在
            throw new MqException("[MessageFileManager] 该队列消息数据文件不存在.queueName="+queue.getName());
        }
        synchronized (queue) {
            //按照约定的消息存储格式，前四个字节表示该消息的长度，紧接着是消息的具体内容

            //offsetBeg和offsetEnd两个参数是在消息写入的时候进行设置的，方便后续从内存对象中读取来确定消息在文件中的位置
            //dataFile.length()获取当前数据文件的长度，单位字节
            File dataFile = new File(getQueueDataPath(queue.getName()));
            message.setOffsetBeg(dataFile.length()+4);
            message.setOffsetEnd(dataFile.length()+4+BinaryTool.toBytes(message).length);
            //上面先把offsetBeg和offsetEnd两个字段赋值，再把message对象转为二进制字节
            byte[] messageBinary = BinaryTool.toBytes(message);

            try(OutputStream outputStream = new FileOutputStream(getQueueDataPath(queue.getName()),true)) {
                try(DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                    //1.先把消息的长度写入，占四个字节
                    dataOutputStream.writeInt(messageBinary.length);
                    //2.把消息正文写入
                    dataOutputStream.write(messageBinary);
                }
            }
            //更新统计信息文件
            Stat stat = readQueueStat(queue.getName());
            stat.totalCount += 1;
            stat.valCount += 1;
            writeQueueStat(queue.getName(),stat);
        }
    }
    //删除消息,从文件中删除消息，使用逻辑删除
    public void deleteMessage(MSGQueue queue,Message message) throws IOException, ClassNotFoundException, MqException {
        //实现逻辑删除，即把isValid设置为0x0,0x0表示无效,0x1表示有效
        //1.读取message对象中的offsetBeg和offsetEnd来确定该message在数据文件中的位置
        //通过使用RandomAccessFile做到随机访问，可以把光标移到offsetBeg处，向后读取message长度，获取message
        //参数"rw"表示以读写模式打开此文件,可读可写
        synchronized (queue) {
            try(RandomAccessFile randomAccessFile = new RandomAccessFile(getQueueDataPath(queue.getName()),"rw")) {
                //把光标移到offsetBeg处
                randomAccessFile.seek(message.getOffsetBeg());
                //用offsetEnd-offsetBeg个单位长度的容器接收二进制消息
                int len = (int) (message.getOffsetEnd()-message.getOffsetBeg());
                byte[] messageBinary = new byte[len];
                //读取消息,并把读取的消息，存到messageBinary容器中
                randomAccessFile.readFully(messageBinary);
                //把读取的反序列化成Message对象
                Message msg = (Message) BinaryTool.fromBytes(messageBinary);
                //判断该对象当前是否有效
                if(msg.getIsValid() == 0x0) {
                    throw new MqException("[MessageFileManager] 该消息当前已经是无效消息,queueName="+queue.getName());
                }
                //修改msg中的isValid,0x0表示无效
                msg.setIsValid((byte) 0x0);
                //把修改后的msg对象重新序列化，再次放回队列中
                byte[] msgBinary = BinaryTool.toBytes(msg);
                //校验序列化后的长度与最开始读取的长度是否一致
                if(msgBinary.length != len) {
                    throw new MqException("[MessageFileManager] 序列化长度不一致, 原长度="
                            + len + ", 新长度=" + messageBinary.length);
                }
                //先重新把光标移到offsetBeg处,再写入.因为在取的过程中，光标已经移到了offsetEnd处,需要重新定位
                randomAccessFile.seek(message.getOffsetBeg());
                randomAccessFile.write(msgBinary);
            }
            //同步内存中对象的isValid的值
            message.setIsValid((byte) 0x0);
            //更新统计文件
            Stat stat = readQueueStat(queue.getName());
            if(stat.valCount>0) {
                stat.valCount -= 1;
                writeQueueStat(queue.getName(),stat);
            }else{
                throw new MqException("[MessageFileManager] 有效消息数<=0.queueName=" +queue.getName());
            }
        }

    }

    // 使用这个方法, 从文件中, 读取出所有的消息内容, 加载到内存中(具体来说是放到一个链表里)
    // 这个方法, 准备在程序启动的时候, 进行调用.
    // 这里使用一个 LinkedList, 主要目的是为了后续进行头删操作.
    // 这个方法的参数, 只是一个 queueName 而不是 MSGQueue 对象. 因为这个方法不需要加锁, 只使用 queueName 就够了.
    // 由于该方法是在程序启动时调用, 此时服务器还不能处理请求呢~~ 不涉及多线程操作文件.
    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, ClassNotFoundException, MqException {
        LinkedList<Message> messageLists = new LinkedList<>();
        try(InputStream inputStream = new FileInputStream(new File(getQueueDataPath(queueName)))) {
            try(DataInputStream dataInputStream = new DataInputStream(inputStream)) {
                //表示当前光标位置
                long curOffset = 0;
                while(true) {
                    //对readInt()方法进行捕获异常，此处的“异常”并非真的异常,而是判断读取文件结束的标志,表示文件读取完毕
                    int len = 0;
                    try {
                        //先读取前四个字节，获取消息长度
                        len = dataInputStream.readInt();
                    }catch (EOFException eof) {
                        System.out.println("文件读取完毕！");
                        break;
                    }
                    //校验len的合法性
                    if(len<=0) {
                        throw new MqException("[MessageFileManager] 非法长度len="+len+",queueName="+queueName);
                    }
                    //创建一个长度为len的容器接收读取到的消息
                    byte[] buffer = new byte[len];
                    //从队列数据文件中读取消息,readFully 保证读满len个长度, 读不满会抛 EOFException, 代表文件损坏
                    dataInputStream.readFully(buffer);
                    //把读取到的二进制消息转化为message对象
                    Message message = (Message) BinaryTool.fromBytes(buffer);
                    //给message对象中的offsetBeg和offsetEnd字段赋值
                    message.setOffsetBeg(curOffset+4);
                    message.setOffsetEnd(curOffset+4+len);
                    //检验该消息是否为有效消息
                    if(message.getIsValid() == (byte)0x1) {
                        //有效消息,把该消息对象放入链表中
                        messageLists.add(message);
                    }
                    //走到这，该消息可能是有效也可能是无效，无论有效还是无效都要更新光标位置
                    curOffset += (4+len);
                }
            }
        }
        return messageLists;
    }
    //检查该队列是否满足GC的条件
    //策略：队列总消息数>2000，并且有效消息数<50%
    public boolean checkGC(MSGQueue queue) throws IOException {
        Stat stat = readQueueStat(queue.getName());
        if(stat.totalCount>=2000 && (double)stat.valCount/(double) stat.totalCount <0.5) {
            //满足GC条件
            return true;
        }
        return false;
    }
    //GC垃圾回收
    //策略：队列总消息数>2000，并且有效消息数<50%
    //先创建一个queue_data_new.txt文件
    //把queue_data.txt文件中的有效消息复制到queue_data_new.txt文件中
    //把queue_data.txt文件删除后，把queue_data_new.txt文件重命名回queue_data.txt
    //更新该队列中的统计信息
    public void gcQueue(MSGQueue queue) throws MqException, IOException, ClassNotFoundException {
        synchronized (queue) {
            //GC可能比较消耗时间。记录一下GC的时间
            long gcBeg = System.currentTimeMillis();
            //创建queue_data_new.txt目录文件
            File newFile = new File(getQueueDir(queue.getName())+"/queue_data_new.txt");
            if(newFile.exists()) {
                //目录文件已经存在，说明上一次GC没有处理干净,可能程序gc了一半就崩溃了
                throw new MqException("[MessageFileManager] 目录文件已经存在,newFile="+newFile.getAbsolutePath());
            }

            boolean ok = newFile.createNewFile();
            if(!ok) {
                //创建queue_data_new.txt文件失败
                throw new MqException("{MessageFileManager] 创建queue_data_new.txt文件失败 文件路径："+newFile.getAbsolutePath());
            }
            //获取queue_data.txt中所有有效消息
            LinkedList<Message> messages = loadAllMessageFromQueue(queue.getName());

            //记录当前的文件长度
            long curFileLength = 0;
            //把有效消息复制到queue_data_new.txt文件中
            for (Message msg:messages) {
                //把每一个消息进行序列化
                byte[] msgBinary = BinaryTool.toBytes(msg);
                //更新每个message的offsetBeg和offsetEnd
                msg.setOffsetBeg(curFileLength+4);
                msg.setOffsetEnd(curFileLength+4+ msgBinary.length);
                try(DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(newFile,true))) {
                    //按照约定的存储格式，前四个字节为消息长度，先把前四个字节写入
                    dataOutputStream.writeInt(msgBinary.length);
                    //然后写入消息内容
                    dataOutputStream.write(msgBinary);
                }
                curFileLength = msg.getOffsetEnd();
            }
            //删除旧文件queue_data.txt
            File oldFile = new File(getQueueDataPath(queue.getName()));
            ok = oldFile.delete();
            if(!ok) {
                throw new MqException("[MessageFileManager] 文件删除失败 filePath="+oldFile.getAbsolutePath());
            }
            //把新创建的queue_data_new.txt重命名回queue_data.txt
            ok = newFile.renameTo(oldFile);
            if(!ok) {
                throw new MqException("[MessageFileManager] 文件重命名失败!");
            }
            //更新统计文件,queue_stat.txt
            Stat stat = new Stat();
            stat.totalCount = messages.size();
            stat.valCount = messages.size();
            writeQueueStat(queue.getName(),stat);

            System.out.println("GC结束,,,,,,,,,,,,,,,,,");
            //GC结束的时间
            long gcEnd = System.currentTimeMillis();
            //GC总共用时
            System.out.println("GC成功，总用时="+(gcEnd-gcBeg)+"ms");
        }
        }

}
