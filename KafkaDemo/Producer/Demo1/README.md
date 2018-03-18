## Producer Demo 1

Example that demonstrate three primary methods of sending messages to Kafka:

- **Fire-and-forget:** Send a message to the server and don’t really care if it arrives successfully or not. Some messages will get lost using this method.


- **Synchronous send:** Send a message where the send() method returns a Future object, and we use get() to wait on the future and see if the send() was successful or not.


- **Asynchronous send:** Call the send() method with a callback function, which gets triggered when it receives a response from the Kafka broker.

