package com.scalaDemo

import scala.collection._
import org.scalatest.{Assertions, FunSpec, Matchers}
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class AppTest extends FunSpec with Matchers {

  describe("An ArrayStack") {

  it("should pop values in last-in-first-out order") {
  val stack = new mutable.ArrayStack[Int]
  stack.push(1)
  stack.push(2)
  assert(stack.pop() === 2)
  assert(stack.pop() === 1)
}

  it("should throw RuntimeException if an empty array stack is popped") {
  val emptyStack = new mutable.ArrayStack[Int]
  intercept[RuntimeException] {
  emptyStack.pop()
}
}
}
}