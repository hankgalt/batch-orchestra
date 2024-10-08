package clients_test

var AgentSchema = `
	DROP TABLE IF EXISTS agent;
	CREATE TABLE agent (
		entity_id   INTEGER PRIMARY KEY,
		entity_name VARCHAR(250) DEFAULT '',
		first_name  VARCHAR(80)  DEFAULT '',
		last_name   VARCHAR(80)  DEFAULT '',
		agent_type  VARCHAR(250) DEFAULT ''
	);
	`
