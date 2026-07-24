VERSION  0.7

all:
    BUILD \
        --build-arg ELIXIR_BASE=1.16.3-erlang-24.3.4.2-alpine-3.20.8 \
        --build-arg ELIXIR_BASE=1.16.3-erlang-26.2.5.11-alpine-3.23.3 \
        +integration-test


setup-base:
    ARG ELIXIR_BASE=1.16.3-erlang-24.3.4.2-alpine-3.20.8
    FROM hexpm/elixir:$ELIXIR_BASE
    RUN apk add --no-progress --update build-base
    RUN mix local.rebar --force
    RUN mix local.hex --force
    ENV ELIXIR_ASSERT_TIMEOUT=10000
    WORKDIR /src/broadway_kafka


integration-test-base:
    FROM +setup-base
    RUN apk add --no-progress --update docker docker-compose git


integration-test:
    FROM +integration-test-base
    COPY mix.exs mix.lock .formatter.exs docker-compose.yml ./
    RUN mix deps.get

    COPY --dir lib test ./

    # then run the tests
    WITH DOCKER --compose docker-compose.yml
        RUN set -e; \
	    mix test --only integration
    END
