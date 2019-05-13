// Copyright 2018 Confluent Inc.
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
//
// Refer to LICENSE for more information.

using System;

namespace Confluent.Kafka
{
  /// <summary>
  /// Define a Filter based on a header
  /// </summary>
  public class FilterByHeader
  {
    /// <summary>
    /// The key of the specific header to test
    /// </summary>
    public string Key { get; set; }
    /// <summary>
    /// Do we keep the message if key not present in the header
    /// </summary>
    public bool KeepMessageIfHeaderNotDefined { get; set; }
    /// <summary>
    /// A predicate base on the header to determine if we keep the message
    /// </summary>
    public Predicate<IHeader> ShouldKeepMessage { get; set; }
  }
}
