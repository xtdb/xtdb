defmodule Elixr.MixProject do
  use Mix.Project

  def project do
    [
      app: :elixr,
      version: "0.1.0",
      elixir: "~> 1.17",
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
      {:postgrex, "~> 0.20.0"},
      {:jason, ">= 1.4.4"},
      {:uuid, "~> 1.1"}
    ]
  end
end
