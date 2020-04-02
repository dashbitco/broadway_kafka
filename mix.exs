defmodule BroadwayKafka.MixProject do
  use Mix.Project

  @version "0.1.2"
  @description "A Kafka connector for Broadway"

  def project do
    [
      app: :broadway_kafka,
      version: @version,
      elixir: "~> 1.7",
      name: "BroadwayKafka",
      description: @description,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:broadway, "~> 0.6.0"},
      {:brod, "~> 3.9.2"},
      {:ex_doc, ">= 0.19.0", only: :docs}
    ]
  end

  defp docs do
    [
      main: "BroadwayKafka.Producer",
      source_ref: "v#{@version}",
      source_url: "https://github.com/dashbitco/broadway_kafka"
    ]
  end

  defp package do
    %{
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/dashbitco/broadway_kafka"}
    }
  end
end
