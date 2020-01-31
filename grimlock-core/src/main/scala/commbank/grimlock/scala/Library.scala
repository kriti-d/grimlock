// Copyright 2019,2020 Commonwealth Bank of Australia
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

package commbank.grimlock.scala.environment

import commbank.grimlock.framework.environment.{ Library => FwLibrary }
import commbank.grimlock.library.transform.{ CutRules => FwCutRules }

import commbank.grimlock.scala.transform.CutRules

/** Implements all library functions/data. */
case object Library extends FwLibrary[Context] {
  val rules: FwCutRules[Context.E] = CutRules
}

