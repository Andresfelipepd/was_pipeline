#!/usr/bin/env python3
import aws_cdk
from api_consumer.json_randomuser_consume import RandomUserConsumerStack
from api_consumer.json_placeholder_consume import JsonPlaceHolderConsumerStack


app = aws_cdk.App()

# get deploy context
config = app.node.try_get_context('config')

if not config:
    raise RuntimeError('Context var missing')

# get deploy props
props = app.node.try_get_context(config)

if not props:
    raise RuntimeError('Configuration not found')

# inyect props and create stack
JsonPlaceHolderConsumerStack(app, "JsonPlaceholderStack", **props)
RandomUserConsumerStack(app, "RandomUserStack", **props)
app.synth()
