-- Players the waiver-wire post actually featured, written by the social publisher after a successful post.
-- The API reads the last N days to avoid repeating anyone. One row per (post, player).
CREATE TABLE IF NOT EXISTS `fantasy-survivor-app.nba_data.waiver_wire_featured` (
  player_id     INT64     NOT NULL,
  featured_date DATE      NOT NULL,
  post_id       STRING    NOT NULL,
  recorded_at   TIMESTAMP NOT NULL
);
