Examples
========================

Sending and receiving data
---------------------------------------------------

Objectives
^^^^^^^^^^
With this example you will learn how to receive and send data from your network.

Code
^^^^
.. literalinclude:: ../examples/example_data.py

Setting sink config
---------------------------------------------------

Objectives
^^^^^^^^^^
With this example you will learn how to configure a sink remotely from your backend.
It is useful especially when the sink was not customized when gateway was produced.

.. note:: The script has hardcoded values that should be customized before
    being used.

Code
^^^^
.. literalinclude:: ../examples/example_configure_sink.py

Discovering nodes
---------------------------------------------------

Objectives
^^^^^^^^^^
With this example you will learn how to discover the list of your nodes from your network.

In this version, it is a passive discovery, so only nodes producing data will be discovered.
The script will run for a specified period and print the list of nodes found
or saved to a file.

Code
^^^^
.. literalinclude:: ../examples/example_discover_nodes.py

Making an otap
---------------------------------------------------

Objectives
^^^^^^^^^^
With this example you will learn how to do an otap in your network (if all nodes
are running at list 5.1 version of the Wirepas stack).

The code can be adapted depending on your use case to avoid the command
parameter.

Code
^^^^
.. literalinclude:: ../examples/example_otap.py
