package com.cn.nrdp.sparksqlapi;

/**
 * 〈一句话功能简述〉<br>
 * 〈〉
 *
 * @author yujiang
 * @create 2019/1/3
 * @since 1.0.0
 */

public class Peoinformation {
    /**
     * \"Alex\", \"浙江\", 39, 230.00"
     */
    private String name;
    private String addr;
    private int age;
    private int price;

    @Override
    public String toString() {
        return "Peoinformation{" +
                "name='" + name + '\'' +
                ", addr='" + addr + '\'' +
                ", age=" + age +
                ", price=" + price +
                '}';
    }
}
