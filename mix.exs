defmodule BroadwayKafka.MixProject do
  use Mix.Project

  def project do
    [
      app: :broadway_kafka,
      version: "0.1.0",
      elixir: "~> 1.5",
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
      {:broadway, git: "https://github.com/plataformatec/broadway.git"},
      # {:brod, "~> 3.9.1"}
      {:brod, git: "https://github.com/klarna/brod.git", commit: "cd55b352e194bf41408b6c344cec84a105a73d94"}
    ]
  end
end
