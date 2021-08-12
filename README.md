# volttron-interface
Volttron interface code needed to use NF with the Volttron Platform Driver


## Installation


1. Copy the `normalgw.py` interface into the directory of your platform driver agent, `$VOLTTRON_HOME/services/core/PlatformDriverAgent/platform_driver/interfaces/
1. Edit the config file, `normalgw.config` to refer to the gRPC locations of your NF installation, and have the desired priority level.
1. If not already installed, make sure the platform.driver agent is installed.
1. Load the normalgw configuration using `vctl`: `vctl config store platform.driver devices/campus/building/normalgw normalgw.config --json`
1. Start the platform driver if not already running.


## Usage

When started, the interface loads the points list from NF over gRPC and creates corresponding Volttron registers.  The register names look like: 

```
eac243ad-b49b-313b-889d-b4fc0f6861da/device_id:260001/device_name:Normal Framework/object_name:OCTETSTRING VALUE 1
```

Unlike the Volttron BACnet support, BACnet discovery should be performed using the NF console or APIs.