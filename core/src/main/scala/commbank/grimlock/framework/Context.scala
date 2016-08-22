// Copyright 2016 Commonwealth Bank of Australia
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

package commbank.grimlock.framework.environment

/** Base trait for capturing all operating context related state. */
trait Context { }

/** Specify distributed data types. */
trait DistributedData {
  /** Type of the underlying (raw) data structure (i.e. TypedPipe or RDD). */
  type U[_]
}

/** Specify user data types. */
trait UserData {
  /** Type of 'wrapper' around user provided data. */
  type E[_]
}

/** Specify environment types. */
trait Environment {
  /** Type of the operating context. */
  type C <: Context
}

