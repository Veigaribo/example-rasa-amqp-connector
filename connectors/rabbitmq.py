import asyncio
from typing import Dict, Text, Any, Optional, Callable, Awaitable

import pika
from pika.channel import Channel
from rasa.core.channels.channel import InputChannel, OutputChannel, UserMessage
from sanic import Blueprint, response
from sanic.request import Request
from sanic.response import HTTPResponse


class RabbitMqJsonInput(InputChannel):
    def __init__(self, host, port, vhost, user, password, input_queue_name):
        loop = asyncio.get_running_loop()

        self.setup_future = loop.create_future()
        self.channel = None

        def on_connected(connection):
            connection.channel(on_open_callback=on_channel_open)

        def on_channel_open(new_channel):
            self.channel = new_channel
            self.channel.queue_declare(
                queue=input_queue_name,
                durable=True,
                exclusive=False,
                auto_delete=False,
                callback=on_queue_declared,
            )

        def on_queue_declared(frame):
            self.channel.basic_consume(input_queue_name, self.handle_delivery)
            self.setup_future.set_result(True)

        is_default_parameters = user is password is None

        parameters = (
            pika.ConnectionParameters()
            if is_default_parameters
            else pika.PlainCredentials(user, password)
        )

        self.connection = pika.SelectConnection(
            parameters, on_open_callback=on_connected
        )

    @classmethod
    def name(cls) -> Text:
        return "rabbitmq_json"

    @classmethod
    def from_credentials(cls, credentials: Optional[Dict[Text, Any]]) -> InputChannel:
        if not credentials:
            cls.raise_missing_credentials_exception()

        return cls(
            credentials.get("host"),
            credentials.get("port"),
            credentials.get("vhost"),
            credentials.get("user"),
            credentials.get("password"),
            credentials.get("input_queue_name"),
        )

    def blueprint(
        self, on_new_message: Callable[[UserMessage], Awaitable[Any]]
    ) -> Blueprint:
        empty_blueprint = Blueprint("rabbitmq_json_webhook", __name__)

        @empty_blueprint.route("/", methods=["GET"])
        async def health(_: Request) -> HTTPResponse:
            return response.json({"status": "ok"})

        @empty_blueprint.route("/webhook", methods=["POST"])
        async def receive(_: Request) -> HTTPResponse:
            return response.json({"status": "ok", "msg": "noop"})

        return empty_blueprint

    def handle_delivery(channel: Channel, method, header, body):
        print("channel", channel)
        print("method", method)
        print("header", header)
        print("body", body)
        pass


class RabbitMqJsonOutput(OutputChannel):
    pass
