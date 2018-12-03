package com.stopbot.dstream.common;

import java.io.Serializable;

/**
 * Class to process data like:
 * 
 * {"ip": "172.10.0.145", "unix_time": 1540473077, "type": "view", "category_id": 1000}
 *  
 */
public class Click implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 8054988422501396392L;
    private String ip;
    private long unix_time;
    private String type;
    private int category_id;

    /**
     * 
     * @param str unix_time, category_id, ip, type(click/view)
     */
    public Click(String str) {
        String[] ar = str.split(",");
        this.unix_time = Long.valueOf(ar[0]);
        this.category_id = Integer.valueOf(ar[1]);
        this.ip = ar[2];
        this.type = ar[3];
    }

    public Click(Click c) {
        this.unix_time = c.getUnix_time();
        this.category_id = c.getCategory_id();
        this.ip = c.getIP();
        this.type = c.getType();
    }

    @Override
    public String toString() {
        return String.format("%d, %s, ratio:%s, categ_cnt:%d",
                this.getUnix_time(), this.getIP(), this.getType(), this.getCategory_id());
    }

    public String getIP() {
        return ip;
    }

    public void setIP(String ip) {
        this.ip = ip;
    }

    public long getUnix_time() {
        return this.unix_time;
    }

    public void setUnix_time(long v) {
        this.unix_time = v;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String v) {
        this.type = v;
    }

    public int getCategory_id() {
        return this.category_id;
    }

    public void setCategory_id(int v) {
        this.category_id = v;
    }
}
