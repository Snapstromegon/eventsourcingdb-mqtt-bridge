# EventsourcingDB to MQTT Bridge

This project is a bridge that connects an EventsourcingDB to an MQTT broker. It listens for events from the DB or MQTT and syncs them to the other side.

Only bare event candidates are supported right now, so setting consistency bounds is not possible.

## Usage

### Configuration

Either provide the environment variables or set a `.env` file based on the example file `.env.example`.

### Running

Just run the executable created by this project.

### Reading events via MQTT

Just subscribe to the output topic configured in the `.env` file. The bridge will publish all events it receives from the DB to the subtopic of this topic depending on the subject. The content will be the event itself, serialized as JSON.

### Writing events via MQTT

To write events to the DB, publish them to the input topic configured in the `.env` file. The bridge will read the event from the message payload and write it to the DB. The event must be a valid JSON object as defined by the EventsourcingDB EventCandidate schema. The bridge will also add a `subject` field to the event, which is the subtopic of the input topic.

### Behavior

#### Crashing

This project is intentionally written in a way to crash when any error occurs. Handling them more gracefully might be added in the future.

#### Control

The bridge uses the control topic to "remember" the last event it processed. This is used to avoid processing the same event multiple times.
Since this uses the retain feature of MQTT, if that is lost, the bridge will start processing events from the beginning again.

## Building

Usually this would just be a matter of running `cargo build --release`, but the project uses the unpublished `eventsourcingdb` crate, which is not **yet** available on crates.io. To build the project, you need to clone the `eventsourcingdb-client-rust` repository and set it as a dependency in your `Cargo.toml`.
