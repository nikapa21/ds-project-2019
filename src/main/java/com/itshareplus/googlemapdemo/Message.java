package com.itshareplus.googlemapdemo;

import java.io.Serializable;

public class Message implements Serializable{

	Data data;
	String msg;

	public Message(Data data, String msg){
		this.data = data;
		this.msg = msg;
	}

	public Data getData() {
		return data;
	}

	public void setData(Data data) {
		this.data = data;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}
}

