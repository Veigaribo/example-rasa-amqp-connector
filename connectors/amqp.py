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


class AmqpJsonInput(InputChannel):
    def __init__(self, connection_uri, input_queue_name, prefetch_count):
        self.input_queue_name = input_queue_name
        self.connection_uri = connection_uri
        self.prefetch_count = prefetch_count

        self.connection = None
        self.blueprint_setup_future = asyncio.Future()

    @classmethod
    def name(cls) -> Text:
        return "amqp_json"

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
        empty_blueprint = Blueprint("amqp_json_webhook", __name__)

        @empty_blueprint.get("/")
        async def health(_: Request) -> HTTPResponse:
            return response.json({"status": "ok"})

        @empty_blueprint.post("/webhook")
        async def receive(_: Request) -> HTTPResponse:
            return response.json({"status": "ok", "msg": "noop"})

        app.register_listener(
            lambda _, loop: self._setup_connection(on_new_message, loop),
            "after_server_start",
        )

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
            content_type = header.content_type
            if not content_type == "application/json" or not header.reply_to:
                channel.basic_ack(method.delivery_tag)
                return

            parsed = json.loads(body.decode("utf-8"))

            sender_id = parsed["sender_id"]
            message = parsed["message"]
            input_channel = parsed.get("input_channel", self.name())
            metadata = parsed.get("metadata", {})

            output_collector = AmqpJsonOutput()

            user_message = UserMessage(
                message,
                output_collector,
                sender_id,
                input_channel=input_channel,
                metadata=metadata,
            )

            # run
            await on_new_message(user_message)

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
            connection.channel(on_open_callback=on_channel_open)

        def on_channel_open(new_channel: Channel):
            global channel
            channel = new_channel

            channel.basic_qos(prefetch_count=self.prefetch_count, callback=on_qos_ok)

        def on_qos_ok(response):
            global channel

            channel.queue_declare(
                queue=input_queue_name,
                durable=True,
                exclusive=False,
                auto_delete=False,
                callback=on_queue_declared,
            )

        def on_queue_declared(frame):
            global channel

            channel.basic_consume(
                self.input_queue_name, self._handle_delivery_with(on_new_message, loop)
            )

        parameters = pika.URLParameters(connection_uri)

        connection = AsyncioConnection(
            parameters, on_open_callback=on_connected, custom_ioloop=loop
        )

        self.connection = connection

        return connection


class AmqpJsonOutput(CollectingOutputChannel):
    pass
