package com.cwc.threading;

public class ThreadManager {
    public static void main(String[] args) {
        Thread1 threadObj = new Thread1();
        Thread t1 = new Thread(threadObj);
        t1.start();
    }
}
