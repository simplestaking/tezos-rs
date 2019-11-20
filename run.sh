#!/usr/bin/env bash

help() {
  echo -e "Usage: \e[97m$0\e[0m \e[32mMODE\e[0m"
  echo -e "Possible values for \e[32mMODE\e[0m are:"
  echo -e " \e[32mnode\e[0m       Start tezedge node. You can specify additional arguments here."
  echo    "              # $0 node -v"
  echo -e " \e[32mrelease\e[0m    Start tezedge node in release mode. You can specify additional arguments here."
  echo    "            Release mode means that the node binary is compiled with '--release' flag."
  echo    "              # $0 release -v"
  echo -e " \e[32mdocker\e[0m     Run tezedge node as a docker container. It is possible to specify additional commandline arguments."
  echo    "              # $0 docker -t 4 -T 12"
  echo    "              # $0 docker --help"
}

build_all() {
  # cargo build profile commandline argument
  case $1 in
    debug)
      # nothing to do here
      shift
      ;;
    release)
      CARGO_PROFILE_ARG="--release"
      shift
      ;;
    *)
      echo "Invalid function argument"
      exit 1
      ;;
  esac

  # this is required for the most linux distributions
  export SODIUM_USE_PKG_CONFIG=1
  cargo build || exit 1
}

run_node() {

  # Default light-node commandline arguments:
  #
  # -d | --tezos-data-dir
  TEZOS_DIR=/tmp/tezedge
  # -c | --config-file-path
  CONFIG_FILE=./light_node/etc/tezedge/tezedge.config
  # -B | --bootstrap-db-path
  BOOTSTRAP_DIR=bootstrap_db  
  # -i | --identity
  IDENTITY_FILE=./light_node/etc/tezedge/identity.json
  # -n | --network
  NETWORK=babylonnet
  # rest of the commandline arguments will end up here
  args=()


  # set compilation profile and cargo commandline argument
  PROFILE=""
  case $1 in
    debug)
      PROFILE="debug"
      shift
      ;;
    release)
      CARGO_PROFILE_ARG="--release"
      PROFILE="release"
      shift
      ;;
    *)
      echo "Invalid function argument"
      exit 1
      ;;
  esac

  shift # shift past <MODE>

  # The following loop is used to replace default light-node commandline parameters.
  # It also allows to specify additional commandline parameters.
  # Supports '--arg=val' and '--arg val' syntax of the commandline arguments.
  while [ "$#" -gt 0 ]; do
    case $1 in
      -d=*|--tezos-data-dir=*)
        TEZOS_DIR="${1#*=}"
        shift
        ;;
      -d|--tezos-data-dir)
        shift
        TEZOS_DIR="$1"
        shift
        ;;
      -c=*|--config-file=*)
        CONFIG_FILE="${1#*=}"
        shift
        ;;
      -c|--config-file)
        shift
        CONFIG_FILE="$1"
        shift
        ;;
      -B=*|--bootstrap-db-path=*)
        BOOTSTRAP_DIR="${1#*=}"
        shift
        ;;
      -B|--bootstrap-db-path)
        shift
        BOOTSTRAP_DIR="$1"
        shift
        ;;
      -n=*|--network=*)
        NETWORK="${1#*=}"
        shift
        ;;
      -n|--network)
        shift
        NETWORK="$1"
        shift
        ;;
      *)
        args+=("$1")
        shift
        ;;
    esac
  done


  # cleanup data directory
  rm -rf "$BOOTSTRAP_DIR" && mkdir "$BOOTSTRAP_DIR"
  rm -rf "$TEZOS_DIR" && mkdir "$TEZOS_DIR"

  # protocol_runner needs 'libtezos.so' to run
  export LD_LIBRARY_PATH="${BASH_SOURCE%/*}/tezos/interop/lib_tezos/artifacts:${BASH_SOURCE%/*}/target/$PROFILE"
  # start node
  cargo run --bin light-node -- --config-file "$CONFIG_FILE" \
                                --tezos-data-dir "$TEZOS_DIR" \
                                --identity-file "$IDENTITY_FILE" \
                                --bootstrap-db-path "$BOOTSTRAP_DIR" \
                                --network "$NETWORK" \
                                --protocol-runner ./target/debug/protocol-runner "${args[@]}"
}

run_docker() {
  shift # shift past <MODE>

  # build docker
  cd ./docker/run/ || exit 1
  docker build -t tezedge-run .
  # run docker
  docker run -i -t tezedge-run "$@"
}

case $1 in

  node)
    printf "\033[1;37mRunning Tezedge node in DEBUG mode\e[0m\n"
    build_all "debug"
    run_node "debug" "$@"
    ;;

  release)
    printf "\033[1;37mRunning Tezedge node in RELEASE mode\e[0m\n"
    build_all "release"
    run_node "release" "$@"
    ;;

  docker)
    printf "\033[1;37mRunning Tezedge node in docker\e[0m\n"
    run_docker "$@"
    ;;

  -h|--help)
    help
    ;;

  *)
    echo "run '$0 --help' to get more info"
    ;;

esac

