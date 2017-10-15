package basics

import org.scalatest.{Matchers, FlatSpec}

class IngestionTest extends FlatSpec with Matchers {
  "42" should "equal to 42" in {


    assert(42 === 42)
  }
}
