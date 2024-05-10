# Overview
This repo provides and interface to listen to ports that works with the cse format. 
# port_listener 
## Architecture:
This class is pretty simple. Here is how to create a port and use it. 
```python
port_listener_obj = port_listener(coms = coms, batch_size=batch_size_1, thread_name=port_listener_name, host=port_listener_one_host, port=port_listener_one_port)
``` 
The ports will automatically save there data to the database. If you want it to send data to another class try calling the `create_tap` function. Here is an example. 
```python
    self.__coms.send_request(name_of_class_to_make_tap, ['create_tap', self.send_tap, name_of_your_class])
```
Note: The `self.send_tap` function is the function that will get called on the call back when your class receives data from the port listener. 
