CREATE TRIGGER message_rabbit_events_insert BEFORE INSERT ON events
    FOR EACH ROW EXECUTE FUNCTION send_rabbitmq_message();