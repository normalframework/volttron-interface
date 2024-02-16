from volttron.platform.vip.agent.utils import build_agent
a = build_agent()
v = a.vip.rpc.call("platform.driver", "set_point", "campus/building/normalgw", "ANALOG VALUE 0", 47).get()
print(v)
