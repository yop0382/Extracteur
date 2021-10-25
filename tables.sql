CREATE EXTENSION amqp;

CREATE TABLE commande (
   command_id serial PRIMARY KEY,
   utilisateur TEXT NOT NULL,
   storage_path TEXT NOT NULL,
   event_state BOOLEAN DEFAULT NULL
);

CREATE TABLE events (
   events_id serial PRIMARY KEY,
   command_id int REFERENCES commande (command_id) ON UPDATE CASCADE,
   session_id TEXT NOT NULL,
   status TEXT NOT NULL
);