package org.example;



public class Main {
    public static void main(String[] args) {
        Producer1 prod=new Producer1("product_visit");
        prod.start();
        Stream str=new Stream("product_visit","visit_test");
        str.start();
        JoinStream joistr=new JoinStream("visit_test","product","test");
        joistr.start();


    }
}