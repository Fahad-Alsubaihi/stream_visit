
package org.example;

        import java.util.ArrayList;
        import java.util.Random;

public class events_list {


    public ArrayList<Product> ra;


    events_list(){

        ra=new ArrayList<>(10);


        ra.add(new Product("556668193","854"));
        ra.add(new Product("531234567","492"));
        ra.add(new Product("554465433","981"));
        ra.add(new Product("511345668","468"));
        ra.add(new Product("515545779","193"));
        ra.add(new Product("524345996","746"));
        ra.add(new Product("546345228","619"));
        ra.add(new Product("582344337","264"));
        ra.add(new Product("599345994","374"));
        ra.add(new Product("551135922","513"));




    }


    public Product getrand() {
        Random rand = new Random();

        return ra.get(rand.nextInt(ra.size()));

    }
}
class Product {
    String customer_mobile_number;
    String product_id;

    Product(String customer_mobile_number,String product_id){

        this.customer_mobile_number=customer_mobile_number;
        this.product_id=product_id;

    }}