"""
Minimal Server-Sent Events (SSE) demo with Flask.

Run:
    pip install flask
    python app.py
Then open http://localhost:5000
"""
import json
import queue
import time
from flask import Flask, Response, render_template, request, stream_with_context

app = Flask(__name__)

# A list of per-client message queues. When someone POSTs to /broadcast,
# we push the message onto every queue, and each open stream drains its own.
subscribers: list[queue.Queue] = []


@app.route("/")
def index():
    return render_template("index.html")


# --- Stream 1: ticking clock ------------------------------------------------
# The simplest possible SSE endpoint. Sends the current time once per second.
@app.route("/stream/time")
def stream_time():
    def gen():
        while True:
            # SSE wire format: "data: <payload>\n\n"
            # The blank line terminates the event.
            yield f"data: {time.strftime('%H:%M:%S')}\n\n"
            time.sleep(1)

    return Response(stream_with_context(gen()), mimetype="text/event-stream")


# --- Stream 2: broadcast ----------------------------------------------------
# Each connected client gets its own Queue added to `subscribers`. When the
# /broadcast endpoint is hit, it fans the message out to every queue.
@app.route("/stream/broadcast")
def stream_broadcast():
    q: queue.Queue = queue.Queue()
    subscribers.append(q)

    def gen():
        try:
            while True:
                msg = q.get()  # blocks until a message arrives
                # Named event: the client listens with addEventListener('chat', ...)
                # This is the closest SSE gets to MQTT-style topics.
                payload = json.dumps(msg)
                yield f"event: chat\ndata: {payload}\n\n"
        finally:
            subscribers.remove(q)

    return Response(stream_with_context(gen()), mimetype="text/event-stream")


@app.route("/broadcast", methods=["POST"])
def broadcast():
    text = request.form.get("text", "").strip()
    if not text:
        return ("", 204)
    msg = {"text": text, "at": time.strftime("%H:%M:%S")}
    for q in subscribers:
        q.put(msg)
    return ("", 204)


if __name__ == "__main__":
    # threaded=True is important — without it, one open SSE connection
    # would block the whole server.
    app.run(debug=True, threaded=True)
