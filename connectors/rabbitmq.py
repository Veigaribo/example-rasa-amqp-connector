import asyncio
from asyncio.events import AbstractEventLoop
import json
from typing import Dict, Text, Any, Optional, Callable, Awaitable

import pika
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.channel import Channel
from pika.spec import Basic, BasicProperties
from rasa.core.channels.channel import (
    InputChannel,
    UserMessage,
    CollectingOutputChannel,
)
from sanic import Blueprint, response
from sanic.app import Sanic
from sanic.request import Request
from sanic.response import HTTPResponse


class RabbitMqJsonInput(InputChannel):
    def __init__(self, connection_uri, input_queue_name, prefetch_count):
        self.input_queue_name = input_queue_name
        self.connection_uri = connection_uri
        self.prefetch_count = prefetch_count

        self.connection = None
        self.blueprint_setup_future = asyncio.Future()

    @classmethod
    def name(cls) -> Text:
        return "rabbitmq_json"

    @classmethod
    def from_credentials(cls, credentials: Optional[Dict[Text, Any]]) -> InputChannel:
        if not credentials:
            cls.raise_missing_credentials_exception()

        return cls(
            credentials.get("connection_uri"),
            credentials.get("input_queue_name"),
            credentials.get("prefetch_count"),
        )

    def blueprint(
        self, on_new_message: Callable[[UserMessage], Awaitable[Any]], app: Sanic
    ) -> Blueprint:
        empty_blueprint = Blueprint("rabbitmq_json_webhook", __name__)

        print("blueprint!")

        @empty_blueprint.get("/")
        async def health(_: Request) -> HTTPResponse:
            return response.json({"status": "ok"})

        @empty_blueprint.post("/webhook")
        async def receive(_: Request) -> HTTPResponse:
            return response.json({"status": "ok", "msg": "noop"})

        print("setupping")
        app.register_listener(
            lambda _, loop: self._setup_connection(on_new_message, loop),
            "after_server_start",
        )
        print("setupped")

        return empty_blueprint

    def _handle_delivery_with(
        self,
        on_new_message: Callable[[UserMessage], Awaitable[Any]],
        loop: AbstractEventLoop,
    ):
        async def handle_delivery(
            channel: Channel,
            method: Basic.Deliver,
            header: BasicProperties,
            body: bytes,
        ):
            print("received message!!", body)
            print("reply to", header.reply_to)

            content_type = header.content_type
            if not content_type == "application/json" or not header.reply_to:
                channel.basic_ack(method.delivery_tag)
                return

            parsed = json.loads(body.decode("utf-8"))

            sender_id = parsed["sender_id"]
            message = parsed["message"]
            input_channel = parsed.get("input_channel", self.name())
            metadata = parsed.get("metadata", {})

            output_collector = RabbitMqJsonOutput()

            user_message = UserMessage(
                message,
                output_collector,
                sender_id,
                input_channel=input_channel,
                metadata=metadata,
            )

            print("will run")
            # run
            await on_new_message(user_message)

            print("outputs", output_collector.messages)
            print("---")

            output_messages = output_collector.messages
            response = json.dumps(output_messages).encode("utf-8")
            reply_to = header.reply_to
            correlation_id = header.correlation_id

            channel.basic_publish(
                "",
                reply_to,
                response,
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,
                    correlation_id=correlation_id,
                ),
            )

            channel.basic_ack(method.delivery_tag)

        def run_handle_delivery(
            channel: Channel,
            method: Basic.Deliver,
            header: BasicProperties,
            body: bytes,
        ):
            print("PUSHED")
            loop.create_task(handle_delivery(channel, method, header, body))

        return run_handle_delivery

    def _setup_connection(
        self,
        on_new_message: Callable[[UserMessage], Awaitable[Any]],
        loop: AbstractEventLoop,
    ):
        connection_uri = self.connection_uri
        input_queue_name = self.input_queue_name

        channel = None

        def on_connected(connection):
            print("connected!")
            connection.channel(on_open_callback=on_channel_open)

        def on_channel_open(new_channel: Channel):
            global channel
            print("channel open!!")
            channel = new_channel

            channel.basic_qos(prefetch_count=self.prefetch_count, callback=on_qos_ok)

        def on_qos_ok(response):
            global channel
            print("prefetch set!!")

            channel.queue_declare(
                queue=input_queue_name,
                durable=True,
                exclusive=False,
                auto_delete=False,
                callback=on_queue_declared,
            )

        def on_queue_declared(frame):
            global channel
            print("queue declared *-*", channel)

            channel.basic_consume(
                self.input_queue_name, self._handle_delivery_with(on_new_message, loop)
            )

        parameters = pika.URLParameters(connection_uri)

        print("connecting...")
        connection = AsyncioConnection(
            parameters, on_open_callback=on_connected, custom_ioloop=loop
        )

        self.connection = connection

        return connection


class RabbitMqJsonOutput(CollectingOutputChannel):
    pass
