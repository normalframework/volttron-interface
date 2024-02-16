from volttron.platform.vip.agent.utils import build_agent
a = build_agent()
v = a.vip.rpc.call("platform.driver", "get_point", "campus/building/normalgw", "ANALOG VALUE 0").get()
print(v)
