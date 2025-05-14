-- Database initialization script

-- Create events table
CREATE TABLE IF NOT EXISTS events (
  id SERIAL PRIMARY KEY,
  event_id VARCHAR(255) NOT NULL,
  event_type VARCHAR(20) NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  full_event JSONB NOT NULL,
  host_responses JSONB NOT NULL
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);

-- Create a view for easy querying of event counts
CREATE OR REPLACE VIEW event_counts AS
SELECT 
  event_type, 
  COUNT(*) as count, 
  MIN(created_at) as first_event, 
  MAX(created_at) as last_event
FROM events
GROUP BY event_type;

-- Create a function to clean old events if needed
CREATE OR REPLACE FUNCTION cleanup_old_events(days INTEGER)
RETURNS INTEGER AS $$
DECLARE
  deleted_count INTEGER;
BEGIN
  DELETE FROM events
  WHERE created_at < (CURRENT_TIMESTAMP - (days || ' days')::INTERVAL);
  
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
</kodu_content>