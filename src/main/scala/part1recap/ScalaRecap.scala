package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {
  // values and variables
  val aBoolean: Boolean = true

  // expressions
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  val theUnit = println("Hello Scala") // Unit = "no meaningful value" = void in other languages

  // functions
  def myFunction(x: Int) = 42

  // OOP
  class Animal

  class Cat extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch !")
  }

  // Singleton pattern
  object MySingleton

  // companions
  object Carnivore

  // generics
  trait MyList[+A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functionnal Programming
  val incrementerNonLambda: Function1[Int, Int] = new Function[Int, Int] {
    override def apply(x: Int): Int = x + 1
  }

  val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(42)

  // HOF
  val processedList = List(1, 2, 3).map(incrementer)

  // Pattern Matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException
  }
  catch {
    case _: NullPointerException => "some returned value"
    case _ => "something else"
  }

  // Future
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(value) => println(s"I've found the value: ${value}")
    case Failure(exception) => println(s"I have failed: ${exception}")
  }

  // Partial functions
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  // Implicits
  // auto-injection by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 43

  implicit val implicitInt = 67

  val implicitCall = methodWithImplicitArgument

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet = println(s"Hi my name is $name")
  }

  implicit def fromStringToPerson(name: String) = Person(name)

  "Bob".greet // fromStringToPerson("Bob").greet

  // implicit conversions - implicit classes
  implicit class Dog(name: String) {
    def bark = println("Bark !")
  }

  "Lassie".bark

  /* Way the compiler figures out which implicit to inject is done by looking into 3 areas
  1 - Local scope
  2 - imported scope
  3 - companion objects of the types involved in the method call
   */
}
