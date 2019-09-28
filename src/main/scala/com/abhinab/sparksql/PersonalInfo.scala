package com.abhinab.sparksql

/**
  * Created by abhin on 9/15/2017.
  */
case class PersonalInfo1(id:Int, fName:String,lName:String,city:String,age:Int)
case class PersonalInfo(fName:String,lName:String,city:String,age:Int)
case class StudentInfo(id:Int,fName:String,lName:String,schoolName:String,cls:String)
case class MarkInfo(id:Int,FM:Int,SM:Int)