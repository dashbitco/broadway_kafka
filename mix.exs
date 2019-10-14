defmodule BroadwayKafka.MixProject do
  use Mix.Project

  def project do
    [
      app: :broadway_kafka,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, git: "https://github.com/plataformatec/broadway.git", branch: "ms-process-partition"},
      {:brod, "~> 3.8"}
    ]
  end
end
