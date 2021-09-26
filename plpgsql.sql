CREATE OR REPLACE FUNCTION send_rabbitmq_message()
  RETURNS TRIGGER 
  LANGUAGE PLPGSQL
  AS
$$
DECLARE 
vmessage TEXT;
BEGIN
	-- Verifie session_id
	IF NEW.session_id IS NULL THEN
		RAISE EXCEPTION 'session_id ne peut pas être NULL';
	END IF;
	-- Verifie command_id
	IF NEW.command_id IS NULL THEN
		RAISE EXCEPTION 'command_id ne peut pas être NULL';
	END IF;
	
	SELECT row_to_json(row) as message INTO vmessage
	FROM (SELECT commande.utilisateur, commande.storage_path, NEW.session_id, NEW.events_id FROM commande 
	WHERE commande.command_id = NEW.command_id) row;
	
	PERFORM amqp.publish(1, 'amq.direct', 'events', vmessage);
	
	RETURN NEW;
END;
$$
