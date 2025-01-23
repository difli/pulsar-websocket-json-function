from pulsar import Function, Client, AuthenticationToken
from pulsar.schema import Record, String, Integer, Boolean, JsonSchema
import json


# Define the JSON schema as a class
class NewRegisterMessageSchema(Record):
    event_name = String()
    id = Integer()
    serial_number = String()
    ethernet_mac_address = String()
    wifi_mac_address = String()
    device_uuid = String()
    brand_model_id = Integer()
    market_id = Integer()
    external_id = String()
    consumer_id = String()
    firmware = String()
    for_test = Boolean()
    brand_model_name = String()
    market_code = String()
    country = String()
    brand_model_group_id = Integer()
    brand_model_group_name = String()


class WebSocketToJsonFunction(Function):
    def __init__(self):
        # Variables to be initialized with user configuration
        self.pulsar_client = None
        self.producers = {}
        self.service_url = None
        self.auth_token = None
        self.new_register_topic = None
        self.other_topic = None
        self.dead_letter_topic = None

    def initialize(self, context):
        """Initialize the Pulsar client and producer using context-provided configurations."""
        try:
            # Retrieve user-configurable values from the context
            self.service_url = context.get_user_config_value("service_url")
            self.auth_token = context.get_user_config_value("auth_token")
            self.new_register_topic = context.get_user_config_value("new_register_topic")
            self.other_topic = context.get_user_config_value("other_topic")
            self.dead_letter_topic = context.get_user_config_value("dead_letter_topic")

            # Initialize Pulsar client and producers
            self.pulsar_client = Client(
                self.service_url,
                authentication=AuthenticationToken(self.auth_token)
            )

            # Producer for the main topic
            self.producers["new_register"] = self.pulsar_client.create_producer(
                topic=self.new_register_topic,
                schema=JsonSchema(NewRegisterMessageSchema)
            )

            # Producer for the other topic
            self.producers["other"] = self.pulsar_client.create_producer(
                topic=self.other_topic,
                schema=JsonSchema(NewRegisterMessageSchema)
            )

            # Producer for the dead letter topic
            self.producers["dead_letter"] = self.pulsar_client.create_producer(
                topic=self.dead_letter_topic
            )

            context.get_logger().info(
                f"Pulsar producers initialized for topics: {self.new_register_topic}, {self.other_topic}, {self.dead_letter_topic}")
        except Exception as e:
            context.get_logger().error(f"Failed to initialize Pulsar producers: {e}")
            raise

    def process(self, input, context):
        """Process incoming messages."""
        try:
            # Perform lazy initialization (ensures `initialize` is only called once)
            if self.pulsar_client is None or not self.producers:
                self.initialize(context)

            # Parse the input message as JSON
            input_data = json.loads(input)

            # Log the input for debugging
            context.get_logger().info(f"Received input: {input_data}")

            # Create an object conforming to the schema
            message = NewRegisterMessageSchema(
                event_name=input_data.get("event_name"),
                id=input_data.get("id"),
                serial_number=input_data.get("serial_number"),
                ethernet_mac_address=input_data.get("ethernet_mac_address"),
                wifi_mac_address=input_data.get("wifi_mac_address"),
                device_uuid=input_data.get("device_uuid"),
                brand_model_id=input_data.get("brand_model_id"),
                market_id=input_data.get("market_id"),
                external_id=input_data.get("external_id"),
                consumer_id=input_data.get("consumer_id"),
                firmware=input_data.get("firmware"),
                for_test=input_data.get("for_test"),
                brand_model_name=input_data.get("brand_model_name"),
                market_code=input_data.get("market_code"),
                country=input_data.get("country"),
                brand_model_group_id=input_data.get("brand_model_group_id"),
                brand_model_group_name=input_data.get("brand_model_group_name"),
            )

            # Determine the target topic based on the event_name
            event_name = input_data.get("event_name")
            if event_name == "new-register":
                self.producers["new_register"].send(message)
                context.get_logger().info(f"Published to new_register_topic: {message}")
            elif event_name == "other-event":
                self.producers["other"].send(message)
                context.get_logger().info(f"Published to other_topic: {message}")
            else:
                # Send to dead letter topic for unknown event types
                self.producers["dead_letter"].send(input)
                context.get_logger().warning(f"Published to dead_letter_topic: {input_data}")

        except json.JSONDecodeError as e:
            context.get_logger().error(f"Failed to decode JSON input: {e}")
        except Exception as e:
            context.get_logger().error(f"Error processing message: {e}")

    def close(self):
        """Clean up resources when the function is stopped."""
        try:
            for producer in self.producers.values():
                producer.close()
            if self.pulsar_client:
                self.pulsar_client.close()
        except Exception as e:
            print(f"Error during resource cleanup: {e}")
