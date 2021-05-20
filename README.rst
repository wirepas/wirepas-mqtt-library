Wirepas MQTT Library
====================

This Python wheel is designed to ease the development of tools on top
of Wirepas Gateway to backend Api (MQTT with protobuf payloads).

Connection through MQTT is wrapped and messages in protobuf format are
generated and parsed for you.

Its main focus is to control a full network without to handle
individually the connection to every gateways.

Most api can be used in a synchronous or asynchronous way to feat your design.

Automatically generated documentation is hosted `here <https://wirepas.github.io/wirepas-mqtt-library/>`__.


Installation
------------

Install from PyPi
~~~~~~~~~~~~~~~~~

This package is available from
`PyPi <https://pypi.org/project/wirepas-mqtt-library/>`__.

.. code:: shell

        pip install wirepas-mqtt-library

From the source
~~~~~~~~~~~~~~~

This wheel can be install from source directly if you need modifications.

.. code:: shell

        pip install -e .

Basic example
-------------

Steps to write a script to send a message
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Import the module
^^^^^^^^^^^^^^^^^

Once the wheel is installed, you can import the module easily

.. code:: python

    from wirepas_mqtt_library import WirepasNetworkInterface

Create a Wirepas Network Interface object
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    wni = WirepasNetworkInterface(host,
                                  port,
                                  username,
                                  password)

This object will allow you to interact with the network. For example,
you can retrieve the list of gateways

.. code:: python

    wni.get_gateways()

Or print the list of sinks for a given network

.. code:: python

    line = ""
    for gw, sink, config in wni.get_sinks():
        line += "[%s:%s] " % (gw, sink)

    print(line)

Send a message with broadcast address from all sinks of a given network
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    for gw, sink, config in wni.get_sinks():
        try:
            res = wni.send_message(gw, sink, 0xFFFFFFFF, 1, 1, "This is a test message".encode())
            if res != wmm.GatewayResultCode.GW_RES_OK:
                print("Cannot send data to %s:%s res=%s" % (gw, sink, res))
        except TimeoutError:
            print("Cannot send data to %s:%s", gw, sink)

Architecture
-------------

Threading model
~~~~~~~~~~~~~~~~

When creating a WirepasNetworkInterface object, few threads will be involved.

* **Network (MQTT) thread**: all the internal MQTT operations will happen on this dedicated thread but no code from your application will be executed on it

* | **Worker thread(s)**: these threads will be used to call all your callbacks.
  | It can be either asynchronous reception of gateway responses or the data you have registered to.
  | If long operation are expected in your callback (like IO operation), you can specify the number of threads to use when creating
  | your WirepasNetworkInterface object to avoid a bottleneck. In fact, multiple threads will allow to handle
  | a new callback if another one is executing long operations.
  | By default there is a single thread.

* **Your calling thread**: Any call made in synchronous mode (cb=None) will lock your calling thread until it a response is receive or a timeout has elapsed

Synchronous vs Asynchronous
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most of the WirepasNetworkInterface methods can be called synchronously or asynchronously.
When synchronous, answer from the gateway is awaited before returning.
from the gateway and a TimeoutError Exception will be generated if the
gateway doesn't answer within the default 2s timeout.

.. code:: python

    # Send a message as broadcast from sink sink_id attached to gateway gw_id on endpoint 1
    # in a synchronous way
    res = wni.send_message(gw, sink, 0xFFFFFFFF, 1, 1, "This is a test message".encode())
    if res != wmm.GatewayResultCode.GW_RES_OK:
        print("Sending data synchronously failed: res=%s" % res)

But if you specify a callback, it will be called when the answer is
received or never if the gateway doesn't answer.

.. code:: python

    def on_message_sent_cb(res, param):
        if res != wmm.GatewayResultCode.GW_RES_OK:
            print("Sending data asynchronously failed: res=%s. Caller param is %s" % (res, param))

    param = 1234
    # Send a message as broadcast from sink sink_id attached to gateway gw_id on endpoint 1
    # in an asynchronous way
    wni.send_message(gw_id, sink_id, 0xFFFFFFFF, 1, 1, "This is a test message".encode(), cb=on_message_sent_cb, param=param)


License
-------

Licensed under the Apache License, Version 2.0.

