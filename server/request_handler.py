from threading import Lock

class RequestHandler:
    def __init__(self, broker, logger):
        self.broker = broker
        self.logger = logger
        self.lock = Lock()
        self.handlers = {
            "publish": self._handle_publish,
            "subscribe": self._handle_subscribe,
            "poll": self._handle_poll,
        }

    def handle(self, request):
        action = request.get("action")
        if not action:
            return {"error": "Missing 'action' in request"}

        handler = self.handlers.get(action)
        if handler:
            return handler(request)
        else:
            self.logger.warning(f"Unknown action '{action}'")
            return {"error": f"Unknown action: {action}"}

    def _handle_publish(self, request):
        topics = request.get("topics", "")
        message = request.get("message")
        if not topics or not message:
            return {"error": "Missing 'topics' or 'message'"}

        topic_list = [t.strip() for t in topics.split(',') if t.strip()]
        with self.lock:
            for topic in topic_list:
                self.broker.publish(topic, message)

        return {"status": "ok"}

    def _handle_subscribe(self, request):
        topics = request.get("topics", "")
        consumer_id = request.get("consumer_id")
        consumer_group = request.get("consumer_group")
        if not consumer_group:
            consumer_group = "Default"
        if not topics or not consumer_id:
            return {"error": "Missing 'topics' or 'consumer_id'"}

        topic_list = [t.strip() for t in topics.split(',') if t.strip()]
        with self.lock:
            for topic in topic_list:
                if not self.broker.has_topic(topic):
                    return {"error": f"Topic '{topic}' not found"}
                self.broker.subscribe(consumer_id, topic, consumer_group)

        return {"status": "ok"}

    def _handle_poll(self, request):
        topic = request.get("topic")
        consumer_id = request.get("consumer_id")
        if not topic or not consumer_id:
            return {"error": "Missing 'topic' or 'consumer_id'"}

        with self.lock:
            if not self.broker.has_topic(topic):
                return {"error": f"Topic '{topic}' not found"}
            msg = self.broker.poll(topic, consumer_id)
            payload = msg.payload if msg else None

        return {"message": payload}
