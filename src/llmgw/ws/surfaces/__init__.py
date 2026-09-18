"""One module per relayed product: what its frames mean, and nothing else.

A `WsSurface` is a classifier plus a handful of constants (`base.py` says
which). It knows its provider's dialect and knows nothing about sockets,
buffers, permits, clocks or records -- `relay.py` owns those and contains not
one provider name, which is the property that makes adding G2's Inworld STT
a file here and a line in `llmgw/ws/__init__.py`.

The registry lives one level up, in `llmgw.ws.WS_REGISTRY`, so importing a
surface directly cannot accidentally register it.
"""
