// Copyright 2015,2016 Commonwealth Bank of Australia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commbank.grimlock

import commbank.grimlock.framework.utility._

class TestQuote extends TestGrimlock {

  "A Quote" should "escape a special character" in {
    Quote(",").escape("foo,bar,baz") shouldBe "\"foo,bar,baz\""
  }

  it should "not escape regular characters" in {
    Quote(",").escape("foo;bar;baz") shouldBe "foo;bar;baz"
  }

  it should "always escape a special character" in {
    Quote(",", all=true).escape("foo,bar,baz") shouldBe "\"foo,bar,baz\""
  }

  it should "always escape regular characters" in {
    Quote(",", all=true).escape("foo;bar;baz") shouldBe "\"foo;bar;baz\""
  }
}

class TestReplace extends TestGrimlock {

  "A Replace" should "escape a special character" in {
    Replace(",").escape("foo,bar,baz") shouldBe "foo\\,bar\\,baz"
  }

  it should "not escape regular characters" in {
    Replace(",").escape("foo;bar;baz") shouldBe "foo;bar;baz"
  }

  it should "substitute a special character" in {
    Replace(",", "|").escape("foo,bar,baz") shouldBe "foo|bar|baz"
  }

  it should "not substitute regular characters" in {
    Replace(",", "|").escape("foo;bar;baz") shouldBe "foo;bar;baz"
  }
}

