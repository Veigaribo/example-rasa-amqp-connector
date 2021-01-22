# Example Rasa AMQP Connector

Example of using AMQP to receive the user messages in Rasa, instead of the standard HTTP webhooks.

Unfortunately this solution currently requires a modification to Rasa itself, so that the InputChannel
is able to access the Sanic app object.
