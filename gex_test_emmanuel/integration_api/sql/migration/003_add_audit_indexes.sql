-- Add indexes to optimize audit queries and common access patterns
-- Safe to run multiple times if guarded by names uniqueness at engine level.

-- distribution_status: optimize delivered lookups by channel/status/time
CREATE INDEX idx_dist_channel_status_delivered_at
  ON distribution_status (channel, status, delivered_at);

-- distribution_status: optimize pending scan by status over time
CREATE INDEX idx_dist_status_created_at
  ON distribution_status (status, created_at);

-- distribution_status: optimize attempts by channel per recent hours
CREATE INDEX idx_dist_channel_created_at
  ON distribution_status (channel, created_at);

-- lead_events: optimize counting/filtering by event across time windows
CREATE INDEX idx_events_event_gateway_time
  ON lead_events (event, gateway_time);
