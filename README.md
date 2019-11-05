# BroadwayKafka

A Kafka connector for [Broadway](https://github.com/plataformatec/broadway).

Documentation can be found at [https://hexdocs.pm/broadway_kafka](https://hexdocs.pm/broadway_kafka).

## Installation

Add `:broadway_kafka` to the list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:broadway_kafka, "~> 0.1.0"}
  ]
end
```

## Usage

Configure Broadway's producer using `BroadwayKafka.Producer`:

```elixir
  defmodule MyBroadway do
    use Broadway

    def start_link(_opts) do
      Broadway.start_link(__MODULE__,
        name: __MODULE__,
        producer: [
          module: {BroadwayKafka.Producer, [
            hosts: [localhost: 9092],
            group_id: "group_1",
            topics: ["test"],
          ]},
          stages: 10
        ],
        processors: [
          default: [
            stages: 10
          ]
        ]
      )
    end

    def handle_message(_, message, _) do
      IO.inspect(message.data, label: "Got message")
      message
    end
  end
```

## License

Copyright 2019 Plataformatec

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
