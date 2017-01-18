// Copyright 2016-2017 Confluent Inc.
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

open System
open System.Text
open System.Collections.Generic
open Confluent.Kafka
open Confluent.Kafka.Serialization

[<EntryPoint>]
let main argv =
    let conf = new Dictionary<string, Object>()
    conf.Add("bootstrap.servers", argv.[0])
    use producer = new Producer<Null, string>(conf, null, new StringSerializer(Encoding.UTF8))
    let dr = producer.ProduceAsync(argv.[1], null, argv.[2])
    dr.Wait()
    0
