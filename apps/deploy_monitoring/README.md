# Deploy Monitoring

Runs and monitors the whole or parts of the tezedge stack (node, debugger, explorer) inside docker contianers with optional alerts sent to a slack channel. The benefits of running the node with the deploy monitoring module is that it gathers resource consumption data, which are then displayed on the resource page and is capable of monitoring and changing the deployed docker imag. This means in case of a new image available, the deploy monitoring stops the containers and pulls the newer images. 

### Prerequisites

- Installed docker and docker-compose

- Installed rust and all the packages required for building tezedge from source. Refer to [this part of the readme](https://github.com/tezedge/tezedge#prerequisites-installation)

- (Optional) Set up slack channel with an app that has webhooks and file:write privilages. 

### Build

Change into the tezedge root directory

```
cd tezedge
```

Use this command to build the deploy_monitoring module
```
SODIUM_USE_PKG_CONFIG=1 cargo build --bin deploy-monitoring --release
```

### Runing the tezedge node with tezedge explorer using the deploy monitoring module

Nohup and sudo do not work well together, so before you use nohup with sudo, use sudo with an arbitrary command, so it won't promtp the password when using nohup.

```
sudo ls
```

Then you can run: 

```
nohup sudo TEZOS_NETWORK=mainnet \
HOSTNAME=develop.dev.tezedge.com \
TEZEDGE_VOLUME_PATH="/absolute/path/to/tezedge/volume/directory" \
LD_LIBRARY_PATH=./tezos/sys/lib_tezos/artifacts \
./target/release/deploy-monitoring \
--compose-file-path apps/deploy_monitoring/docker-compose.tezedge_and_explorer.yml \
--tezedge-only \
--disable-debugger \
--tezedge-alert-threshold-memory 10000 \
--tezedge-alert-threshold-synchronization 120 \
--slack-url https://hooks.slack.com/services/XXXXXXXXX/XXXXXXXXXXX/XXXXXXXXXXXXXXXXXXXXXXXX \
--slack-token "Bearer xoxb-xxxxxxxxxxxx-xxxxxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxxxx" \
--slack-channel-name monitoring-channel > deploy-monitoring.out &
```
Every run needs a few environmental variables:

- `TEZOS_NETWORK`: one of the tezedge networks, e.g.: mainnet
- `HOSTNAME`: the local ip address or hostname
- `TEZEDGE_VOLUME_PATH`: **ABSOLUTE** path to the directory that stores or will store the tezedge data. This will be mounted in the docker container. *Note: do not use relative paths, it will create unexpected behaviour*
- `LD_LIBRARY_PATH`: required because the deploy_monitoring module is dependant on the shell module (this will be reworked in the future).


The configuration used for this run:

- `compose-file-path`: (Required) Path to the compose file that defines the container structure.
- `tezedge-only`: (Optional) Runs only the tezedge node (otherwise the ocaml node is run as well alongside).
- `disable-debugger`: (Optional) Won't launch the debugger and the memory profiler.
- `tezedge-alert-threshold-memory`: (Optional) Sets an alert threshold in MB for memory consumption. Defaults to 4096.
- `tezedge-alert-threshold-synchronization`: (Optional) Sets a threshold in seconds to report a stuck node. If the node fails to update it's current head in this threshold, the node is pronounced stuck. Defaults to 300.
- `slack-url`: (Optional) Slack webhook url used to send messages to specific channels
- `slack-token`: (Optional) Token used to upload log file on node crash
- `slack-channel-name`: (Optional) Monitoring cahnnel name.

If you do not wish to recieve slack notifications, do not include the slack options. 

Other options:

- `image-monitor-interval`: (Optional) Sets the interval in seconds  in which the module checks for new images on docker hub. If this flag is not included, the check is not perfomed at all. 
- `resource-monitor-interval`: (Optional) Sets the interval in seconds in which the module performes resource monitoring. Defaults to 5 seconds 
- `rpc-port`: (Optional) Custom port for the resources endpoints. Defaults to 38732.
- `cleanup-volumes`: (Optional) With this option enabled, the module will clean up all the data used in the run (data from both nodes and debugger).
- `tezedge-alert-threshold-cpu`: (Optional) The usage threshold in %. After passing this threshold the high cpu usage is reported as an alert. Disabled by default
- `tezedge-alert-threshold-disk`: (Optional) The threshold in(0-100)%. This measurement calculates the the used space on the filesystem and reports when it has passed the set threshold. Defaults to 95.
- `ocaml-alert-threshold-disk`: (Optional) The threshold in(0-100)%. This measurement calculates the the used space on the filesystem and reports when it has passed the set threshold. 
- `ocaml-alert-threshold-memory`: (Optional) Sets an alert threshold in MB for memory consumption. Defaults to 6144
- `ocaml-alert-threshold-cpu`: (Optional) The usage threshold in %. After passing this threshold the high cpu usage is reported as an alert. Disabled by default.
- `ocaml-alert-threshold-synchronization`: (Optional) Sets a threshold in seconds to report a stuck node. If the node fails to update it's current head in this threshold, the node is pronounced stuck. Defaults to 300s.

### Prepared tezedge data

You can download data for tezedge node that are already synced to a fixed level to make the bootstrapping much faster. In this case, give the **ABSOLUTE** path to the included `_data` directory.



### Shutting down

If you have run the stack with nohup, you will have to find the PID of the deploy_monitoring binary

```
ps aux | grep deploy_monitoring
```

Then you have to send a SIGINT to deploy_monitoring to shut it down properly

```
sudo kill -s SIGINT <PID>
```

Wait until you see *Shutdown complete* in logs. This means the stack shut down gracefully.

## Potentional issues

- When you cancel the execution in the lauching period (not all containers runnig), the containers will not clean up properly. In this case you can run docker-compose directly with the compose file inlcuded in the option `compose-file-path` and close the containers. Run from the `tezedge` root directory.
    ```
        docker compose -f apps/deploy_monitoring/docker-compose.tezedge_and_explorer.yml down
    ```
- If for any reasons there are dangingling containers left running, you can use the docker-compose command above to clean them up.

- This module prunes the unused docker images and containers on every run using `docker system prune -a`. So it will clean images and containers that are not in use at that time.

- The whole codebase is scheduled for a deep refactor to make the deploy_monitoring more user friendly.

