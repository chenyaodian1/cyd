package com.es;

public class Person {
	public Person(){
		 
    }
    public Person(String name , int age){
        this.name = name;
        this.age = age;
    }
    private String name;
    private long age;
    private long salary;
    private String id;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public long getAge() {
		return age;
	}
	public void setAge(long age) {
		this.age = age;
	}
	public long getSalary() {
		return salary;
	}
	public void setSalary(long salary) {
		this.salary = salary;
	}
	private int isAdult;
 
    public int getIsAdult() {
		return isAdult;
	}
	public void setIsAdult(int isAdult) {
		this.isAdult = isAdult;
	}
	public String getName() {
        return name;
    }
 
    public void setName(String name) {
        this.name = name;
    }
 

}
