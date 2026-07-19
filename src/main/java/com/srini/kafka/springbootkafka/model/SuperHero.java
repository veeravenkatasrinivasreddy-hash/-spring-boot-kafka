package com.srini.kafka.springbootkafka.model;

// this class is a shape of the data being sent-its fields,types and how to convert it to/fro json.
import java.io.Serializable;
// implements serializable means this objects state can be converted into a byte stream.
public class SuperHero implements Serializable
{
    // we have declared private so that we can use them by getters and setters eg encapsulation. without getters and setters this variables can only be used in the SuperHero class only.
    private String name;
    private String superName;
    private String profession;
    private int age;
    private boolean canFly;
    // no argumnet constructor.
    public SuperHero()
    {
    }
    // parametrized constructor.
    public SuperHero(String name, String superName, String profession, int age, boolean canFly)
    {
        this.name = name;
        this.superName = superName;
        this.profession = profession;
        this.age = age;
        this.canFly = canFly;

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSuperName() {
        return superName;
    }

    public void setSuperName(String superName) {
        this.superName = superName;
    }

    public String getProfession() {
        return profession;
    }

    public void setProfession(String profession) {
        this.profession = profession;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public boolean isCanFly() {
        return canFly;
    }

    public void setCanFly(boolean canFly) {
        this.canFly = canFly;
    }
    // purely for reading the logs without that  it will be like alphanumeric with memory address.
    @Override
    public String toString()
    {
        return "SuperHero [name=" + name + ", superName=" + superName + ", profession=" + profession + ", age=" + age + " , canFly=" +canFly +"]";
    }


}

