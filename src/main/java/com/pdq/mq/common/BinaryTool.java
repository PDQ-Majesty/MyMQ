package com.pdq.mq.common;

import java.io.*;

/**
 * @Description: 序列化和反序列化
 * @Auther: pdq
 */
public class BinaryTool {
    //将对象进行序列化
    //这里使用java原生序列化
    public static byte[] toBytes(Object object) throws IOException {
        try(ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                objectOutputStream.writeObject(object);
                return byteArrayOutputStream.toByteArray();
            }
        }
    }
    //将字节流数组进行反序列化
    public static Object fromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        try(ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes)) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
                return objectInputStream.readObject();
            }
        }
    }
}
