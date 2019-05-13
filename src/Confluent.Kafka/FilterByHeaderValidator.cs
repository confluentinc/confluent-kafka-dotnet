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
using System.Collections.Generic;
using System.Linq;

namespace Confluent.Kafka
{
  /// <summary>
  /// Compute if message should be kept based on filterByHeader List
  /// </summary>
  public class FilterByHeaderValidator
  {
    private readonly List<FilterByHeader> _filterByHeaders;

    /// <summary>
    /// ctor
    /// </summary>
    /// <param name="filterByHeaders"> list of filter by header</param>
    public FilterByHeaderValidator(IEnumerable<FilterByHeader> filterByHeaders)
    {
      if (filterByHeaders == null)
        throw new ArgumentNullException(nameof(filterByHeaders));

      _filterByHeaders = filterByHeaders?.ToList();
    }

    /// <summary>
    /// compute if message should be kept based on the header
    /// </summary>
    /// <param name="headers">header of the message</param>
    /// <returns></returns>
    public bool ShouldKeepMessage(Headers headers)
    {
      foreach (var filterByHeader in _filterByHeaders)
      {
        var header = headers.FirstOrDefault(h => h.Key == filterByHeader.Key);
        if (header == null && filterByHeader.KeepMessageIfHeaderNotDefined)
        {
          return true;
        }
        else
        {
          try
          {
            if (filterByHeader.ShouldKeepMessage(header))
              return true;
          }
          catch { }
        }
      }

      return false;
    }
  }
}
