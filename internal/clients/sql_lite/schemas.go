package sqllite

type Column struct {
	Key   string
	Value any
}

type Row struct {
	Columns []Column
}

type Agent struct {
	EntityId     string `db:"entity_id"`
	EntityName   string `db:"entity_name"`
	Name         string `db:"name"`
	AgentType    string `db:"agent_type"`
	PositionType string `db:"position_type"`
	Address      string `db:"address"`
	CreatedAt    string `db:"created_at"`
	UpdatedAt    string `db:"updated_at"`
}

var AgentSchema = `
	DROP TRIGGER IF EXISTS agent_updated_at;
	DROP TABLE IF EXISTS agent;

	CREATE TABLE agent (
	entity_id   TEXT PRIMARY KEY,
	entity_name TEXT NOT NULL DEFAULT '',
	name        TEXT NOT NULL DEFAULT '',
	agent_type  TEXT NOT NULL DEFAULT '',
	position_type  TEXT DEFAULT '',
	address     TEXT NOT NULL DEFAULT '',
	created_at  TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
	updated_at  TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
	);

	CREATE TRIGGER agent_updated_at
	AFTER UPDATE ON agent
	FOR EACH ROW
	WHEN NEW.updated_at = OLD.updated_at
	BEGIN
	UPDATE agent SET updated_at = CURRENT_TIMESTAMP
	WHERE entity_id = OLD.entity_id;
	END;
`

func MapAgentRecord(record map[string]any) *Row {
	cols := []Column{}
	if entity_id, ok := record["entity_id"]; ok {
		cols = append(cols, Column{Key: "entity_id", Value: entity_id})
	} else {
		return nil // entity_id is required
	}
	if entity_name, ok := record["entity_name"]; ok {
		cols = append(cols, Column{Key: "entity_name", Value: entity_name})
	}
	if name, ok := record["name"]; ok {
		cols = append(cols, Column{Key: "name", Value: name})
	}
	if address, ok := record["address"]; ok {
		cols = append(cols, Column{Key: "address", Value: address})
	}
	if agent_type, ok := record["agent_type"]; ok {
		cols = append(cols, Column{Key: "agent_type", Value: agent_type})
	}
	if position_type, ok := record["position_type"]; ok {
		cols = append(cols, Column{Key: "position_type", Value: position_type})
	}

	if len(cols) == 0 {
		return nil
	}
	return &Row{
		Columns: cols,
	}
}
