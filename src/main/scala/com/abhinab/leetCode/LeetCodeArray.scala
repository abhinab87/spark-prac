package com.abhinab.leetCode

import scala.collection.mutable.ListBuffer
import java.nio.charset.StandardCharsets

/**
  * Created by abhin on 8/4/2018.
  */
object LeetCodeArray {

  def main(args: Array[String]): Unit = {
    val x = StandardCharsets.UTF_8
    val sar = Array(3, 2, 5, 7, 3, 7)
    val sar1 = Array(2, 4, 6, 7, 8, 9, 10, 10)
    val sortedAr = Array(2, 4, 6, 7, 8, 9, 10, 15)
    val msr = Array(-2, 1, -3, 4, 1, 2, -1, -5, 4)
    val bar = Array(0, 1, 1, 1, 0, 1, 1)
    val sortAr2 = Array(1, 3)
    val findMissingElem = Array(1,2,1)
    val moveZero = Array(0,1,2,0,4)
    println("After Removing Duplicates new Length"+removeDuplicates(sar1))
    println(sar1.length+" After Removing 10 new Length "+removeElement(sar1, 10))
    println(twoSum(sar, 8).toList)
    println(searchInsert(sortedAr, 1))
    println("max subArray is " + maxSubArray(msr, 9))
    println("Max consecutive 1s in a binary Array is " + findMaxConsecutiveOne(bar).max)
    println("Missing element in the array is "+findMissingElementsInArray(findMissingElem).toList)
    println("Resultant Array after moving zeros "+moveZeros(moveZero).toList)
  }

  def twoSum(num: Array[Int], target: Int): Array[Int] = {
    var tgt = new ListBuffer[Int]()
    var mp = Map[Int, Int]()
    num.map { x =>
      if (mp.getOrElse(target - x, null) != null) {
        tgt += mp.get(target - x).get
        tgt += num.indexOf(x)
      } else {
        mp += (x -> num.indexOf(x))
      }
    }
    tgt.toArray
  }

  def removeDuplicates(num: Array[Int]): Int = {
    var tgt = new ListBuffer[Int]
    if (num.length == 0) num.length
    else {
      num.map(x => if (x == num(num.indexOf(x) + 1)) tgt += x)
      num.length - tgt.length + 1
    }
  }

  def removeElement(num: Array[Int], elm: Int): Int = {
    var tgt = new ListBuffer[Int]
    if (num.length == 0) num.length
    else {
      num.map(x => if (x == num(num.indexOf(x) + 1)) tgt += x)
      num.length - tgt.length
    }
  }

  def searchInsert(num: Array[Int], target: Int): Int = {
    if (num.length == 0) 0
    else {
      var low = 0
      var high = num.length - 1
      while (low <= high) {
        var mid = (low + high) / 2
        if (num(mid) == target) mid
        else if (num(mid) > target) high = mid - 1
        else low = mid + 1
      }
      low
    }
  }

  //TODO return the sequence
  def maxSubArray(num: Array[Int], length: Int): Int = {
    var lb = new ListBuffer[Int]
    lb += num(0)
    var max = num(0)
    for (i <- 1 until length) {
      if (lb(i - 1) > 0) lb += num(i) + lb(i - 1) else lb += num(i)
      max = math.max(max, lb(i))
    }
    max
  }

  //TODO complete this
  def mergeSortedArrays(nums1: Array[Int], nums2: Array[Int]): Array[Int] = {
    var i = nums1.length - 1
    var j = nums2.length - 1
    val k = i + j - 1
    //println(i+":::"+j)
    val lb = new ListBuffer[Int]
    while (i > -1 && j > -1) {
      if (nums1(i) > nums2(j)) {
        lb += nums1(i - 1)
        i -= 1
      } else {
        lb += nums2(j - 1)
        j -= 1
      }
    }
    while (j > -1) lb += nums2(j - 1)
    lb.toArray
  }

  def findMaxConsecutiveOne(num: Array[Int]) = {
    var count = 0
    val d = num.map { x =>
      if (x == 0) count = 0 else count += 1
      count
    }
    d
  }
  def findMissingElementsInArray(num:Array[Int]): Array[Int]={
    var lb = new ListBuffer[Int]
    for(i <- 0 until num.length){
      var elem = math.abs(num(i)) -1
      if(num(elem) > 0 ) num(elem) = -num(elem)
    }
    for(i <- 0 until num.length) if(num(i) > 0) lb += i +1
    lb.toArray
  }

  def moveZeros(nums:Array[Int]): Array[Int]={
    var j = 0
    nums.map { x =>
      if (x != 0) {
        nums(j) = x
        j +=1
      }
    }
    val x = nums.length - j
    for(i <- 0 until x) {
      nums(j) = 0
      j += 1
    }
    nums
  }

  def rotateArray(nums:Array[Int], num:Int): Array[Int]= {
    val ss = "asd"
    ss.tail
    nums
  }
}